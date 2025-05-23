package ruler

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/cmk"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/ruler/rulespb"
	"github.com/cortexproject/cortex/pkg/ruler/rulestore"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
)

func Test_cmkHooks(t *testing.T) {
	const (
		user1 = "user1"
		user2 = "user2"
		user3 = "user3"
	)

	const (
		ruler1     = "ruler-1"
		ruler1Host = "1.1.1.1"
		ruler1Port = 9999
	)

	user1Group1 := &rulespb.RuleGroupDesc{User: user1, Namespace: "namespace", Name: "first", Interval: time.Minute}
	user1Group2 := &rulespb.RuleGroupDesc{User: user1, Namespace: "namespace", Name: "second", Interval: time.Minute}
	user2Group1 := &rulespb.RuleGroupDesc{User: user2, Namespace: "namespace", Name: "first", Interval: time.Minute}
	user3Group1 := &rulespb.RuleGroupDesc{User: user3, Namespace: "namespace", Name: "first", Interval: time.Minute}

	allRules := map[string]rulespb.RuleGroupList{
		user1: {user1Group1, user1Group2},
		user2: {user2Group1},
		user3: {user3Group1},
	}

	kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	setupRuler := func(id string, host string, port int, forceRing *ring.Ring) *Ruler {
		store := newMockRuleStore(allRules, nil)
		u, _ := url.Parse("")
		cfg := Config{
			EnableSharding:   true,
			ExternalURL:      flagext.URLValue{URL: u},
			PollInterval:     time.Millisecond * 100,
			RingCheckPeriod:  time.Minute,
			ShardingStrategy: util.ShardingStrategyShuffle,
			Ring: RingConfig{
				InstanceID:   id,
				InstanceAddr: host,
				InstancePort: port,
				KVStore: kv.Config{
					Mock: kvStore,
				},
				HeartbeatTimeout:  1 * time.Minute,
				ReplicationFactor: 1,
			},
			FlushCheckPeriod: 0,
		}

		r, _ := buildRuler(t, cfg, nil, store, nil)
		r.limits = &ruleLimits{tenantShard: 1}

		if forceRing != nil {
			r.ring = forceRing
		}
		return r
	}

	closeFunctionsByUser := map[string]func(err error) error{}
	openFunctionsByUser := map[string]func(){}

	cmk.Config.PreCreationHook = func(user string, folder string, _ log.Logger, open func(), close func(err error) error) error {
		closeFunctionsByUser[user] = close
		openFunctionsByUser[user] = open
		return nil
	}

	defer func() {
		// Reset hooks config
		cmk.Config = cmk.FilesystemHooksConfig{}
	}()

	r1 := setupRuler(ruler1, ruler1Host, ruler1Port, nil)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), r1))
	t.Cleanup(r1.StopAsync)

	err := kvStore.CAS(context.Background(), ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		d, _ := in.(*ring.Desc)
		if d == nil {
			d = ring.NewDesc()
		}
		d.AddIngester(ruler1, fmt.Sprintf("%v:%v", ruler1Host, ruler1Port), "", []uint32{0}, ring.ACTIVE, time.Now())
		return d, true, nil
	})

	require.NoError(t, err)

	test.Poll(t, time.Second*5, true, func() interface{} {
		return len(r1.manager.GetRules(user1)) > 0 &&
			len(r1.manager.GetRules(user2)) > 0 &&
			len(r1.manager.GetRules(user3)) > 0
	})

	returned, _, err := r1.listRules(context.Background())
	require.NoError(t, err)
	require.Equal(t, returned, allRules)

	// Make sure that the hooks got called
	require.Equal(t, 3, len(closeFunctionsByUser))

	// Closing user1
	require.NoError(t, closeFunctionsByUser[user1](nil))
	returned, _, err = r1.listRules(context.Background())
	require.NoError(t, err)
	require.Equal(t, returned, map[string]rulespb.RuleGroupList{
		user2: {user2Group1},
		user3: {user3Group1},
	})

	// Reopening User1
	openFunctionsByUser[user1]()
	returned, _, err = r1.listRules(context.Background())
	require.NoError(t, err)
	require.Equal(t, returned, allRules)
}

func buildRulerWithLimits(t *testing.T, rulerConfig Config, querierTestConfig *querier.TestConfig, store rulestore.RuleStore, rulerAddrMap map[string]*Ruler, limits RulesLimits) (*Ruler, *DefaultMultiTenantManager) {
	engine, queryable, pusher, logger, _, reg := testSetup(t, querierTestConfig)

	metrics := NewRuleEvalMetrics(rulerConfig, nil)
	managerFactory := DefaultTenantManagerFactory(rulerConfig, pusher, queryable, engine, limits, metrics, reg)
	manager, err := NewDefaultMultiTenantManager(rulerConfig, limits, managerFactory, metrics, reg, log.NewNopLogger(), nil)
	require.NoError(t, err)

	ruler, err := newRuler(
		rulerConfig,
		manager,
		reg,
		logger,
		store,
		limits,
		newMockClientsPool(rulerConfig, logger, reg, rulerAddrMap),
	)
	require.NoError(t, err)
	return ruler, manager
}

func Test_cmkAllowedTenants(t *testing.T) {
	const (
		ruler1     = "ruler-1"
		ruler1Host = "1.1.1.1"
		ruler1Port = 9999
	)
	const (
		user1 = "user1_cmk"
		user2 = "user2_cmk"
		user3 = "user3_cmk"
		user4 = "user4_cmk"
	)

	user1Group1 := &rulespb.RuleGroupDesc{User: user1, Namespace: "namespace", Name: "first", Interval: time.Minute}
	user1Group2 := &rulespb.RuleGroupDesc{User: user1, Namespace: "namespace", Name: "second", Interval: time.Minute}
	user2Group1 := &rulespb.RuleGroupDesc{User: user2, Namespace: "namespace", Name: "first", Interval: time.Minute}
	user3Group1 := &rulespb.RuleGroupDesc{User: user3, Namespace: "namespace", Name: "first", Interval: time.Minute}
	user4Group1 := &rulespb.RuleGroupDesc{User: user3, Namespace: "namespace", Name: "first", Interval: time.Minute}

	allRules := map[string]rulespb.RuleGroupList{
		user1: {user1Group1, user1Group2},
		user2: {user2Group1},
		user3: {user3Group1},
		user4: {user4Group1},
	}

	for _, tc := range []struct {
		name                      string
		expectedRuleGroupsForUser map[string]rulespb.RuleGroupList
		disabledTenants           []string
		s3KmsKeyId                string
		kmsEncryptionWorkspaceKey string
	}{
		{
			name: "tenant not allowed when cmk config is not propagated",
			expectedRuleGroupsForUser: map[string]rulespb.RuleGroupList{
				user3: {user3Group1},
			},
			disabledTenants: []string{user4},
		},
		{
			name: "tenant allowed when cmk config is propagated",
			expectedRuleGroupsForUser: map[string]rulespb.RuleGroupList{
				user1: {user1Group1, user1Group2},
				user3: {user3Group1},
			},
			disabledTenants:           []string{user4},
			s3KmsKeyId:                "testKeyArn",
			kmsEncryptionWorkspaceKey: "testWorkspaceKey",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			setupRuler := func(id string, host string, port int, forceRing *ring.Ring) *Ruler {
				store := newMockRuleStore(allRules, nil)
				cfg := Config{
					EnableSharding:   true,
					ShardingStrategy: util.ShardingStrategyShuffle,
					Ring: RingConfig{
						InstanceID:   id,
						InstanceAddr: host,
						InstancePort: port,
						KVStore: kv.Config{
							Mock: kvStore,
						},
						ReplicationFactor: 1,
						HeartbeatTimeout:  1 * time.Minute,
					},
					FlushCheckPeriod: 0,
					DisabledTenants:  tc.disabledTenants,
				}
				limits := &ruleLimits{tenantShard: 3, s3SseKmsKeyId: tc.s3KmsKeyId, kmsEncryptionWorkspaceKey: tc.kmsEncryptionWorkspaceKey}
				r, _ := buildRulerWithLimits(t, cfg, nil, store, nil, limits)

				// Simulates scenario where tenant is previously disabled due to CMK propagation delay
				r.allowedTenants.disable(user1, keyPropagationDelay)

				// Simulates scenario where tenant is disabled
				r.allowedTenants.disable(user2, other)

				return r
			}

			r1 := setupRuler(ruler1, ruler1Host, ruler1Port, nil)

			rulerRing := r1.ring

			if rulerRing != nil {
				require.NoError(t, services.StartAndAwaitRunning(context.Background(), rulerRing))
				t.Cleanup(rulerRing.StopAsync)
			}

			err := kvStore.CAS(context.Background(), ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
				d, _ := in.(*ring.Desc)
				if d == nil {
					d = ring.NewDesc()
				}
				d.AddIngester(ruler1, fmt.Sprintf("%v:%v", ruler1Host, ruler1Port), "", []uint32{0}, ring.ACTIVE, time.Now())

				return d, true, nil
			})
			require.NoError(t, err)
			time.Sleep(100 * time.Millisecond)

			// user2 disabled reason is other - not cmk propagation delay
			require.False(t, r1.allowedTenants.IsAllowed(user2))
			// user4 explicitly disabled
			require.False(t, r1.allowedTenants.IsAllowed(user4))

			require.True(t, r1.allowedTenants.IsAllowed(user3))
			if tc.s3KmsKeyId == "" || tc.kmsEncryptionWorkspaceKey == "" {
				// assert tenant is not allowed when configs propagated
				require.False(t, r1.allowedTenants.IsAllowed(user1))
			} else {
				// tenant disabled due to cmk propagation delay should be allowed after configs propagated
				require.True(t, r1.allowedTenants.IsAllowed(user1))
			}

			loadedRules, _, err := r1.listRules(context.Background())
			require.NoError(t, err)
			require.Equal(t, tc.expectedRuleGroupsForUser, loadedRules)
		})
	}
}
