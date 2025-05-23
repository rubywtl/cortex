package bucketclient

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	promRules "github.com/prometheus/prometheus/rules"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ruler/rulespb"
	"github.com/cortexproject/cortex/pkg/ruler/rulestore"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/testutil"
)

type testGroup struct {
	user, namespace string
	ruleGroup       rulefmt.RuleGroup
}

func TestListRules(t *testing.T) {
	runForEachRuleStore(t, func(t *testing.T, rs rulestore.RuleStore, _ interface{}) {
		groups := []testGroup{
			{user: "user1", namespace: "hello", ruleGroup: rulefmt.RuleGroup{Name: "first testGroup"}},
			{user: "user1", namespace: "hello", ruleGroup: rulefmt.RuleGroup{Name: "second testGroup"}},
			{user: "user1", namespace: "world", ruleGroup: rulefmt.RuleGroup{Name: "another namespace testGroup"}},
			{user: "user2", namespace: "+-!@#$%. ", ruleGroup: rulefmt.RuleGroup{Name: "different user"}},
		}

		for _, g := range groups {
			desc := rulespb.ToProto(g.user, g.namespace, g.ruleGroup)
			require.NoError(t, rs.SetRuleGroup(context.Background(), g.user, g.namespace, desc))
		}

		{
			users, err := rs.ListAllUsers(context.Background())
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"user1", "user2"}, users)
		}

		{
			allGroupsMap, err := rs.ListAllRuleGroups(context.Background())
			require.NoError(t, err)
			require.Len(t, allGroupsMap, 2)
			require.ElementsMatch(t, []*rulespb.RuleGroupDesc{
				{User: "user1", Namespace: "hello", Name: "first testGroup"},
				{User: "user1", Namespace: "hello", Name: "second testGroup"},
				{User: "user1", Namespace: "world", Name: "another namespace testGroup"},
			}, allGroupsMap["user1"])
			require.ElementsMatch(t, []*rulespb.RuleGroupDesc{
				{User: "user2", Namespace: "+-!@#$%. ", Name: "different user"},
			}, allGroupsMap["user2"])
		}

		{
			user1Groups, err := rs.ListRuleGroupsForUserAndNamespace(context.Background(), "user1", "")
			require.NoError(t, err)
			require.ElementsMatch(t, []*rulespb.RuleGroupDesc{
				{User: "user1", Namespace: "hello", Name: "first testGroup"},
				{User: "user1", Namespace: "hello", Name: "second testGroup"},
				{User: "user1", Namespace: "world", Name: "another namespace testGroup"},
			}, user1Groups)
		}

		{
			helloGroups, err := rs.ListRuleGroupsForUserAndNamespace(context.Background(), "user1", "hello")
			require.NoError(t, err)
			require.ElementsMatch(t, []*rulespb.RuleGroupDesc{
				{User: "user1", Namespace: "hello", Name: "first testGroup"},
				{User: "user1", Namespace: "hello", Name: "second testGroup"},
			}, helloGroups)
		}

		{
			invalidUserGroups, err := rs.ListRuleGroupsForUserAndNamespace(context.Background(), "invalid", "")
			require.NoError(t, err)
			require.Empty(t, invalidUserGroups)
		}

		{
			invalidNamespaceGroups, err := rs.ListRuleGroupsForUserAndNamespace(context.Background(), "user1", "invalid")
			require.NoError(t, err)
			require.Empty(t, invalidNamespaceGroups)
		}

		{
			user2Groups, err := rs.ListRuleGroupsForUserAndNamespace(context.Background(), "user2", "")
			require.NoError(t, err)
			require.ElementsMatch(t, []*rulespb.RuleGroupDesc{
				{User: "user2", Namespace: "+-!@#$%. ", Name: "different user"},
			}, user2Groups)
		}
	})
}

func TestLoadPartialRules(t *testing.T) {
	bucketClient := objstore.NewInMemBucket()
	mockedBucketClient := &testutil.MockBucketFailure{Bucket: bucketClient, GetFailures: map[string]error{}}
	bucketStore := NewBucketRuleStore(mockedBucketClient, nil, log.NewNopLogger())

	groups := []testGroup{
		{user: "user1", namespace: "hello", ruleGroup: rulefmt.RuleGroup{Name: "second testGroup", Interval: model.Duration(2 * time.Minute)}},
		{user: "user2", namespace: "+-!@#$%. ", ruleGroup: rulefmt.RuleGroup{Name: "different user", Interval: model.Duration(5 * time.Minute)}},
		{user: "user3", namespace: "+-!@#$%. ", ruleGroup: rulefmt.RuleGroup{Name: "different user", Interval: model.Duration(5 * time.Minute)}},
	}

	for _, g := range groups {
		desc := rulespb.ToProto(g.user, g.namespace, g.ruleGroup)
		require.NoError(t, bucketStore.SetRuleGroup(context.Background(), g.user, g.namespace, desc))
	}
	allGroups, err := bucketStore.ListAllRuleGroups(context.Background())
	require.NoError(t, err)

	loadedGroups, err := bucketStore.LoadRuleGroups(context.Background(), allGroups)
	require.NoError(t, err)
	require.Equal(t, 3, len(loadedGroups))

	// Fail user1
	mockedBucketClient.GetFailures["rules/user2"] = testutil.ErrKeyAccessDeniedError
	loadedGroups, err = bucketStore.LoadRuleGroups(context.Background(), allGroups)
	require.ErrorContains(t, err, "access denied")
	require.Equal(t, 2, len(loadedGroups))
}

func TestLoadRules(t *testing.T) {
	runForEachRuleStore(t, func(t *testing.T, rs rulestore.RuleStore, _ interface{}) {
		groups := []testGroup{
			{user: "user1", namespace: "hello", ruleGroup: rulefmt.RuleGroup{Name: "first testGroup", Interval: model.Duration(time.Minute), Rules: []rulefmt.Rule{{
				For:    model.Duration(5 * time.Minute),
				Labels: map[string]string{"label1": "value1"},
			}}}},
			{user: "user1", namespace: "hello", ruleGroup: rulefmt.RuleGroup{Name: "second testGroup", Interval: model.Duration(2 * time.Minute)}},
			{user: "user1", namespace: "world", ruleGroup: rulefmt.RuleGroup{Name: "another namespace testGroup", Interval: model.Duration(1 * time.Hour)}},
			{user: "user2", namespace: "+-!@#$%. ", ruleGroup: rulefmt.RuleGroup{Name: "different user", Interval: model.Duration(5 * time.Minute)}},
		}

		for _, g := range groups {
			desc := rulespb.ToProto(g.user, g.namespace, g.ruleGroup)
			require.NoError(t, rs.SetRuleGroup(context.Background(), g.user, g.namespace, desc))
		}

		allGroupsMap, err := rs.ListAllRuleGroups(context.Background())

		// Before load, rules are not loaded
		{
			require.NoError(t, err)
			require.Len(t, allGroupsMap, 2)
			require.ElementsMatch(t, []*rulespb.RuleGroupDesc{
				{User: "user1", Namespace: "hello", Name: "first testGroup"},
				{User: "user1", Namespace: "hello", Name: "second testGroup"},
				{User: "user1", Namespace: "world", Name: "another namespace testGroup"},
			}, allGroupsMap["user1"])
			require.ElementsMatch(t, []*rulespb.RuleGroupDesc{
				{User: "user2", Namespace: "+-!@#$%. ", Name: "different user"},
			}, allGroupsMap["user2"])
		}

		allGroupsMap, err = rs.LoadRuleGroups(context.Background(), allGroupsMap)
		require.NoError(t, err)

		// After load, rules are loaded.
		{
			require.NoError(t, err)
			require.Len(t, allGroupsMap, 2)

			require.ElementsMatch(t, []*rulespb.RuleGroupDesc{
				{User: "user1", Namespace: "hello", Name: "first testGroup", Interval: time.Minute, Rules: []*rulespb.RuleDesc{
					{
						For:    5 * time.Minute,
						Labels: []cortexpb.LabelAdapter{{Name: "label1", Value: "value1"}},
					},
				}},
				{User: "user1", Namespace: "hello", Name: "second testGroup", Interval: 2 * time.Minute},
				{User: "user1", Namespace: "world", Name: "another namespace testGroup", Interval: 1 * time.Hour},
			}, allGroupsMap["user1"])

			require.ElementsMatch(t, []*rulespb.RuleGroupDesc{
				{User: "user2", Namespace: "+-!@#$%. ", Name: "different user", Interval: 5 * time.Minute},
			}, allGroupsMap["user2"])
		}

		// Loading group with mismatched info fails.
		require.NoError(t, rs.SetRuleGroup(context.Background(), "user1", "hello", &rulespb.RuleGroupDesc{User: "user2", Namespace: "world", Name: "first testGroup"}))
		_, err = rs.LoadRuleGroups(context.Background(), allGroupsMap)
		require.EqualError(t, err, "mismatch between requested rule group and loaded rule group, requested: user=\"user1\", namespace=\"hello\", group=\"first testGroup\", loaded: user=\"user2\", namespace=\"world\", group=\"first testGroup\"")

		// Load with missing rule groups fails.
		require.NoError(t, rs.DeleteRuleGroup(context.Background(), "user1", "hello", "first testGroup"))
		_, err = rs.LoadRuleGroups(context.Background(), allGroupsMap)
		require.EqualError(t, err, "get rule group user=\"user2\", namespace=\"world\", name=\"first testGroup\": group does not exist")
	})
}

func TestDelete(t *testing.T) {
	runForEachRuleStore(t, func(t *testing.T, rs rulestore.RuleStore, bucketClient interface{}) {
		groups := []testGroup{
			{user: "user1", namespace: "A", ruleGroup: rulefmt.RuleGroup{Name: "1"}},
			{user: "user1", namespace: "A", ruleGroup: rulefmt.RuleGroup{Name: "2"}},
			{user: "user1", namespace: "B", ruleGroup: rulefmt.RuleGroup{Name: "3"}},
			{user: "user1", namespace: "C", ruleGroup: rulefmt.RuleGroup{Name: "4"}},
			{user: "user2", namespace: "second", ruleGroup: rulefmt.RuleGroup{Name: "group"}},
			{user: "user3", namespace: "third", ruleGroup: rulefmt.RuleGroup{Name: "group"}},
		}

		for _, g := range groups {
			desc := rulespb.ToProto(g.user, g.namespace, g.ruleGroup)
			require.NoError(t, rs.SetRuleGroup(context.Background(), g.user, g.namespace, desc))
		}

		// Verify that nothing was deleted, because we used canceled context.
		{
			canceled, cancelFn := context.WithCancel(context.Background())
			cancelFn()

			require.Error(t, rs.DeleteNamespace(canceled, "user1", ""))

			require.Equal(t, []string{
				"rules/user1/" + getRuleGroupObjectKey("A", "1"),
				"rules/user1/" + getRuleGroupObjectKey("A", "2"),
				"rules/user1/" + getRuleGroupObjectKey("B", "3"),
				"rules/user1/" + getRuleGroupObjectKey("C", "4"),
				"rules/user2/" + getRuleGroupObjectKey("second", "group"),
				"rules/user3/" + getRuleGroupObjectKey("third", "group"),
			}, getSortedObjectKeys(bucketClient))
		}

		// Verify that we can delete individual rule group, or entire namespace.
		{
			require.NoError(t, rs.DeleteRuleGroup(context.Background(), "user2", "second", "group"))
			require.NoError(t, rs.DeleteNamespace(context.Background(), "user1", "A"))

			require.Equal(t, []string{
				"rules/user1/" + getRuleGroupObjectKey("B", "3"),
				"rules/user1/" + getRuleGroupObjectKey("C", "4"),
				"rules/user3/" + getRuleGroupObjectKey("third", "group"),
			}, getSortedObjectKeys(bucketClient))
		}

		// Verify that we can delete all remaining namespaces for user1.
		{
			require.NoError(t, rs.DeleteNamespace(context.Background(), "user1", ""))

			require.Equal(t, []string{
				"rules/user3/" + getRuleGroupObjectKey("third", "group"),
			}, getSortedObjectKeys(bucketClient))
		}

		{
			// Trying to delete empty namespace again will result in error.
			require.Equal(t, rulestore.ErrGroupNamespaceNotFound, rs.DeleteNamespace(context.Background(), "user1", ""))
		}
	})
}

func runForEachRuleStore(t *testing.T, testFn func(t *testing.T, store rulestore.RuleStore, bucketClient interface{})) {
	bucketClient := objstore.NewInMemBucket()
	bucketStore := NewBucketRuleStore(bucketClient, nil, log.NewNopLogger())

	stores := map[string]struct {
		store  rulestore.RuleStore
		client interface{}
	}{
		"bucket": {store: bucketStore, client: bucketClient},
	}

	for name, data := range stores {
		t.Run(name, func(t *testing.T) {
			testFn(t, data.store, data.client)
		})
	}
}

func getSortedObjectKeys(bucketClient interface{}) []string {
	if typed, ok := bucketClient.(*objstore.InMemBucket); ok {
		var keys []string
		for key := range typed.Objects() {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		return keys
	}

	return nil
}

func TestParseRuleGroupObjectKey(t *testing.T) {
	decodedNamespace := "my-namespace"
	encodedNamespace := base64.URLEncoding.EncodeToString([]byte(decodedNamespace))

	decodedGroup := "my-group"
	encodedGroup := base64.URLEncoding.EncodeToString([]byte(decodedGroup))

	tests := map[string]struct {
		key               string
		expectedErr       error
		expectedNamespace string
		expectedGroup     string
	}{
		"empty object key": {
			key:         "",
			expectedErr: errInvalidRuleGroupKey,
		},
		"invalid object key pattern": {
			key:         "way/too/long",
			expectedErr: errInvalidRuleGroupKey,
		},
		"empty namespace": {
			key:         fmt.Sprintf("/%s", encodedGroup),
			expectedErr: errEmptyNamespace,
		},
		"invalid namespace encoding": {
			key:         fmt.Sprintf("invalid/%s", encodedGroup),
			expectedErr: errors.New("illegal base64 data at input byte 4"),
		},
		"empty group": {
			key:         fmt.Sprintf("%s/", encodedNamespace),
			expectedErr: errEmptyGroupName,
		},
		"invalid group encoding": {
			key:         fmt.Sprintf("%s/invalid", encodedNamespace),
			expectedErr: errors.New("illegal base64 data at input byte 4"),
		},
		"valid object key": {
			key:               fmt.Sprintf("%s/%s", encodedNamespace, encodedGroup),
			expectedNamespace: decodedNamespace,
			expectedGroup:     decodedGroup,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			namespace, group, err := parseRuleGroupObjectKey(testData.key)

			if testData.expectedErr != nil {
				assert.EqualError(t, err, testData.expectedErr.Error())
			} else {
				require.NoError(t, err)
				assert.Equal(t, testData.expectedNamespace, namespace)
				assert.Equal(t, testData.expectedGroup, group)
			}
		})
	}
}

func TestParseRuleGroupObjectKeyWithUser(t *testing.T) {
	decodedNamespace := "my-namespace"
	encodedNamespace := base64.URLEncoding.EncodeToString([]byte(decodedNamespace))

	decodedGroup := "my-group"
	encodedGroup := base64.URLEncoding.EncodeToString([]byte(decodedGroup))

	tests := map[string]struct {
		key               string
		expectedErr       error
		expectedUser      string
		expectedNamespace string
		expectedGroup     string
	}{
		"empty object key": {
			key:         "",
			expectedErr: errInvalidRuleGroupKey,
		},
		"invalid object key pattern": {
			key:         "way/too/much/long",
			expectedErr: errInvalidRuleGroupKey,
		},
		"empty user": {
			key:         fmt.Sprintf("/%s/%s", encodedNamespace, encodedGroup),
			expectedErr: errEmptyUser,
		},
		"empty namespace": {
			key:         fmt.Sprintf("user-1//%s", encodedGroup),
			expectedErr: errEmptyNamespace,
		},
		"invalid namespace encoding": {
			key:         fmt.Sprintf("user-1/invalid/%s", encodedGroup),
			expectedErr: errors.New("illegal base64 data at input byte 4"),
		},
		"empty group name": {
			key:         fmt.Sprintf("user-1/%s/", encodedNamespace),
			expectedErr: errEmptyGroupName,
		},
		"invalid group encoding": {
			key:         fmt.Sprintf("user-1/%s/invalid", encodedNamespace),
			expectedErr: errors.New("illegal base64 data at input byte 4"),
		},
		"valid object key": {
			key:               fmt.Sprintf("user-1/%s/%s", encodedNamespace, encodedGroup),
			expectedUser:      "user-1",
			expectedNamespace: decodedNamespace,
			expectedGroup:     decodedGroup,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			user, namespace, group, err := parseRuleGroupObjectKeyWithUser(testData.key)

			if testData.expectedErr != nil {
				assert.EqualError(t, err, testData.expectedErr.Error())
			} else {
				require.NoError(t, err)
				assert.Equal(t, testData.expectedUser, user)
				assert.Equal(t, testData.expectedNamespace, namespace)
				assert.Equal(t, testData.expectedGroup, group)
			}
		})
	}
}

func TestListAllRuleGroupsWithNoNamespaceOrGroup(t *testing.T) {
	obj := mockBucket{
		names: []string{
			"rules/",
			"rules/user1/",
			"rules/user2/bnM=/",         // namespace "ns", ends with '/'
			"rules/user3/bnM=/Z3JvdXAx", // namespace "ns", group "group1"
		},
	}

	s := NewBucketRuleStore(obj, nil, log.NewNopLogger())
	out, err := s.ListAllRuleGroups(context.Background())
	require.NoError(t, err)

	require.Equal(t, 1, len(out))                    // one user
	require.Equal(t, 1, len(out["user3"]))           // one group
	require.Equal(t, "group1", out["user3"][0].Name) // one group
}

type testAlert struct {
	user, namespace, group, rule string
	alerts                       []*promRules.Alert
}

func TestGetAlertRuleState(t *testing.T) {
	alert1 := &promRules.Alert{
		State: 2,
		Labels: []labels.Label{
			{Name: "test1", Value: "value1"},
		},
		Annotations: []labels.Label{
			{Name: "test-annotation", Value: "us-east-1"},
		},
		Value: 1,
	}
	alert2 := &promRules.Alert{
		State: 2,
		Labels: []labels.Label{
			{Name: "alert2", Value: "value1"},
		},
		Annotations: []labels.Label{
			{Name: "test-alert2", Value: "us-east-1"},
		},
		Value: 100,
	}
	testAlerts := []testAlert{
		{user: "user1", namespace: "hello", group: "group1", rule: "ar1", alerts: []*promRules.Alert{alert1}},
		{user: "user1", namespace: "hello", group: "group1", rule: "ar2", alerts: []*promRules.Alert{alert2}},
		{user: "user1", namespace: "hello", group: "group2", rule: "ar3", alerts: []*promRules.Alert{alert1, alert2}},
		{user: "user2", namespace: "test", group: "group1", rule: "ar3", alerts: []*promRules.Alert{}},
	}

	bucketClient := objstore.NewInMemBucket()
	rs := NewBucketAlertsStore(bucketClient, nil, log.NewNopLogger())
	for _, testAlert := range testAlerts {
		require.NoError(t, rs.SetAlertRuleState(context.Background(), testAlert.user, testAlert.namespace, testAlert.group, xxhash.Sum64([]byte(testAlert.rule)), testAlert.alerts))
	}

	tests := map[string]struct {
		user           string
		namespace      string
		group          string
		rule           string
		expectedAlerts []*promRules.Alert
		expectedErr    bool
	}{
		"user1 rule1 alerts": {
			user:           "user1",
			namespace:      "hello",
			group:          "group1",
			rule:           "ar1",
			expectedAlerts: []*promRules.Alert{alert1},
		},
		"user1 rule2 alerts": {
			user:           "user1",
			namespace:      "hello",
			group:          "group1",
			rule:           "ar2",
			expectedAlerts: []*promRules.Alert{alert2},
		},
		"user1 - multiple alerts for a rule": {
			user:           "user1",
			namespace:      "hello",
			group:          "group2",
			rule:           "ar3",
			expectedAlerts: []*promRules.Alert{alert1, alert2},
		},
		"user2 rule - empty alerts": {
			user:           "user2",
			namespace:      "test",
			group:          "group1",
			rule:           "ar3",
			expectedAlerts: []*promRules.Alert{},
		},
		"user3 - no state": {
			user:           "user3",
			namespace:      "invalid",
			group:          "group1",
			rule:           "ar3",
			expectedAlerts: nil,
			expectedErr:    true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			alerts, err := rs.GetAlertRuleState(context.Background(), testData.user, testData.namespace, testData.group, xxhash.Sum64([]byte(testData.rule)))
			if testData.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, testData.expectedAlerts, alerts)
			}
		})
	}
}

type mockBucket struct {
	objstore.Bucket

	names []string
}

func (mb mockBucket) Iter(_ context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	for _, n := range mb.names {
		if err := f(n); err != nil {
			return err
		}
	}
	return nil
}
