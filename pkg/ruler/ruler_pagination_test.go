package ruler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ruler/rulespb"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestRuler_RuleInfos(t *testing.T) {
	mockRules := map[string]rulespb.RuleGroupList{
		"user1": {
			&rulespb.RuleGroupDesc{
				Name:      "group1",
				Namespace: "namespace1",
				User:      "user1",
				Rules: []*rulespb.RuleDesc{
					{
						Alert: "UP_ALERT",
						Expr:  "1", // always fire for this test
					},
				},
				Interval: interval,
			},
			&rulespb.RuleGroupDesc{
				Name:      "group2",
				Namespace: "namespace2",
				User:      "user1",
				Rules: []*rulespb.RuleDesc{
					{
						Alert: "UP_ALERT",
						Expr:  "1", // always fire for this test
					},
				},
				Interval: interval,
			},
			&rulespb.RuleGroupDesc{
				Name:      "group3",
				Namespace: "namespace3",
				User:      "user1",
				Rules: []*rulespb.RuleDesc{
					{
						Alert: "UP_ALERT",
						Expr:  "1", // always fire for this test
					},
				},
				Interval: interval,
			},
		},
	}

	// NEXT, set up ruler config
	store := newMockRuleStore(mockRules, nil)
	rulerCfg := defaultRulerConfig(t)

	// create a ruler but don't start it. instead, we'll evaluate the rule groups manually.
	r := newTestRuler(t, rulerCfg, store, nil)
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	ruleGroup := r.manager.GetRules("user1")

	// NEXT, evaluate the rule group the first time and assert
	ctx := user.InjectOrgID(context.Background(), "user1")
	for _, rg := range ruleGroup {
		rg.Eval(ctx, time.Now().UTC())
	}

	type testCase struct {
		orgID                string
		rulesRequest         *RuleInfosRequest
		expectedRgLength     int
		expectedAlertsLength int
		expectedRg           rulespb.RuleGroupList
		hasMore              bool
		expectNextToken      string
	}

	testCases := map[string]testCase{
		"No max items, user1": {
			orgID:                "user1",
			rulesRequest:         &RuleInfosRequest{MaxAlerts: -1, MaxRuleGroups: -1},
			expectedRgLength:     3,
			expectedAlertsLength: 1,
			expectedRg:           mockRules["user1"],
			hasMore:              false,
			expectNextToken:      "",
		},
		"Has max alerts, user1": {
			orgID:                "user1",
			rulesRequest:         &RuleInfosRequest{MaxAlerts: 0, MaxRuleGroups: -1},
			expectedRgLength:     3,
			expectedAlertsLength: 0,
			expectedRg:           mockRules["user1"],
			hasMore:              true,
			expectNextToken:      "",
		},
		"Has max rule groups, user1": {
			orgID:                "user1",
			rulesRequest:         &RuleInfosRequest{MaxAlerts: 0, MaxRuleGroups: 1},
			expectedRgLength:     1,
			expectedAlertsLength: 0,
			expectedRg:           mockRules["user1"],
			hasMore:              true,
			expectNextToken:      getRuleGroupNextToken(mockRules["user1"][1].Namespace, mockRules["user1"][1].Name),
		},
		"Has max rule groups and start next token, user1": {
			orgID:                "user1",
			rulesRequest:         &RuleInfosRequest{MaxAlerts: 0, MaxRuleGroups: 1, NextToken: getRuleGroupNextToken(mockRules["user1"][1].Namespace, mockRules["user1"][1].Name)},
			expectedRgLength:     1,
			expectedAlertsLength: 0,
			expectedRg:           mockRules["user1"],
			hasMore:              true,
			expectNextToken:      getRuleGroupNextToken(mockRules["user1"][2].Namespace, mockRules["user1"][2].Name),
		},
		"Has max rule groups = 2 and start next token, user1": {
			orgID:                "user1",
			rulesRequest:         &RuleInfosRequest{MaxAlerts: 0, MaxRuleGroups: 2, NextToken: getRuleGroupNextToken(mockRules["user1"][1].Namespace, mockRules["user1"][1].Name)},
			expectedRgLength:     2,
			expectedAlertsLength: 0,
			expectedRg:           mockRules["user1"],
			hasMore:              true,
			expectNextToken:      "",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), tc.orgID)
			rls, err := r.RuleInfos(ctx, tc.rulesRequest)
			require.NoError(t, err)
			require.Len(t, rls.Groups, tc.expectedRgLength)
			for _, rg := range rls.Groups {
				for j, expectRG := range tc.expectedRg {
					if (rg.Group.Namespace == tc.expectedRg[j].Namespace) && (rg.Group.Name == tc.expectedRg[j].Name) {
						compareRuleGroupDescToResponseDesc(t, expectRG, rg)
					}
				}
			}
			require.Equal(t, tc.expectNextToken, rls.NextToken)
			require.Equal(t, tc.expectedAlertsLength, len(rls.Groups[0].ActiveRules[0].AlertInfo.Alerts))
			require.Equal(t, tc.hasMore, rls.Groups[0].ActiveRules[0].AlertInfo.HasMore)
		})
	}

}

func TestRuler_AlertInfos(t *testing.T) {
	mockRules := map[string]rulespb.RuleGroupList{
		"user1": {
			&rulespb.RuleGroupDesc{
				Name:      "group1",
				Namespace: "namespace1",
				User:      "user1",
				Rules: []*rulespb.RuleDesc{
					{
						Alert: "UP_ALERT_1",
						Expr:  "1", // always fire for this test
					},
				},
				Interval: interval,
			},
			&rulespb.RuleGroupDesc{
				Name:      "group2",
				Namespace: "namespace2",
				User:      "user1",
				Rules: []*rulespb.RuleDesc{
					{
						Alert: "UP_ALERT_2",
						Expr:  "1", // always fire for this test
					},
					{
						Alert: "UP_ALERT_3",
						Expr:  "1", // always fire for this test
					},
				},
				Interval: interval,
			},
			&rulespb.RuleGroupDesc{
				Name:      "group3",
				Namespace: "namespace3",
				User:      "user1",
				Rules: []*rulespb.RuleDesc{
					{
						Alert: "UP_ALERT_4",
						Expr:  "1", // always fire for this test
					},
					{
						Alert: "UP_ALERT_5",
						Expr:  "1", // always fire for this test
					},
				},
				Interval: interval,
			},
		},
	}

	// NEXT, set up ruler config
	store := newMockRuleStore(mockRules, nil)
	rulerCfg := defaultRulerConfig(t)

	// create a ruler but don't start it. instead, we'll evaluate the rule groups manually.
	r := newTestRuler(t, rulerCfg, store, nil)
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	ruleGroup := r.manager.GetRules("user1")

	// NEXT, evaluate the rule group the first time and assert
	ctx := user.InjectOrgID(context.Background(), "user1")
	for _, rg := range ruleGroup {
		rg.Eval(ctx, time.Now().UTC())
	}

	type testCase struct {
		orgID               string
		alertsRequest       *AlertInfosRequest
		expectedAlertLength int
		expectedAlert       []string
		expectNextToken     string
	}

	testCases := map[string]testCase{
		"No max items, user1": {
			orgID:               "user1",
			alertsRequest:       &AlertInfosRequest{MaxResults: -1},
			expectedAlertLength: 5,
			expectedAlert:       []string{"UP_ALERT_2", "UP_ALERT_1", "UP_ALERT_3", "UP_ALERT_4", "UP_ALERT_5"},
			expectNextToken:     "",
		},
		"Has max alerts, user1": {
			orgID:               "user1",
			alertsRequest:       &AlertInfosRequest{MaxResults: 0},
			expectedAlertLength: 0,
			expectedAlert:       []string{},
			expectNextToken:     "",
		},
		"Has max alerts = 3, user1": {
			orgID:               "user1",
			alertsRequest:       &AlertInfosRequest{MaxResults: 3},
			expectedAlertLength: 3,
			expectedAlert:       []string{"UP_ALERT_2", "UP_ALERT_1", "UP_ALERT_3"},
			expectNextToken: getAlertNextToken("namespace2", "group2", "UP_ALERT_3", "1",
				[]cortexpb.LabelAdapter{
					{Name: "alertname", Value: "UP_ALERT_3"},
				}),
		},
		"Has max alerts and start next token, user1": {
			orgID: "user1",
			alertsRequest: &AlertInfosRequest{MaxResults: 1, NextToken: getAlertNextToken(
				"namespace2", "group2", "UP_ALERT_3", "1",
				[]cortexpb.LabelAdapter{
					{Name: "alertname", Value: "UP_ALERT_3"},
				})},
			expectedAlertLength: 1,
			expectedAlert:       []string{"UP_ALERT_4"},
			expectNextToken: getAlertNextToken("namespace3", "group3", "UP_ALERT_4", "0",
				[]cortexpb.LabelAdapter{
					{Name: "alertname", Value: "UP_ALERT_4"},
				}),
		},
		"Filter by rule group and has max alerts": {
			orgID:               "user1",
			alertsRequest:       &AlertInfosRequest{MaxResults: 1, RuleGroupNames: []string{"group2"}},
			expectedAlertLength: 1,
			expectedAlert:       []string{"UP_ALERT_2"},
			expectNextToken: getAlertNextToken("namespace2", "group2", "UP_ALERT_2", "0",
				[]cortexpb.LabelAdapter{
					{Name: "alertname", Value: "UP_ALERT_2"},
				}),
		},
		"Filter by matcher": {
			orgID:               "user1",
			alertsRequest:       &AlertInfosRequest{MaxResults: -1, RuleGroupNames: []string{"group2"}, Matches: []string{`{alertname="UP_ALERT_2"}`}},
			expectedAlertLength: 1,
			expectedAlert:       []string{"UP_ALERT_2"},
			expectNextToken:     "",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), tc.orgID)
			alerts, err := r.AlertInfos(ctx, tc.alertsRequest)
			require.NoError(t, err)
			require.Len(t, alerts.Alerts, tc.expectedAlertLength)
			alertnames := make([]string, 0, len(alerts.Alerts))
			for _, alert := range alerts.Alerts {
				alertnames = append(alertnames, cortexpb.FromLabelAdaptersToLabels(alert.Labels).Get("alertname"))
			}
			require.Equal(t, tc.expectedAlert, alertnames)
			require.Equal(t, tc.expectNextToken, alerts.NextToken)
		})
	}

}
