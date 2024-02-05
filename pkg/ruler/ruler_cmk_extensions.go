package ruler

import (
	"path/filepath"
	"sync"

	"github.com/go-kit/log"

	"github.com/prometheus/prometheus/model/rulefmt"

	"github.com/cortexproject/cortex/pkg/cmk"
	"github.com/cortexproject/cortex/pkg/util"
	logutil "github.com/cortexproject/cortex/pkg/util/log"
)

type DisabledReason int

const (
	keyPropagationDelay DisabledReason = iota
	other
)

const errCmkConfigurationNotSet = "cmk configuration not set"

var (
	allowedTenants = &cmkAllowedTenants{disabled: make(map[string]DisabledReason)}
)

type cmkMapper struct {
	*mapper
}

func initAllowedTenants(cfg Config, limits RulesLimits) *cmkAllowedTenants {
	allowedTenants.set(limits, util.NewAllowedTenants(cfg.EnabledTenants, cfg.DisabledTenants))
	return allowedTenants
}

func newMapperCmk(cfg Config, logger log.Logger) *cmkMapper {
	m := &cmkMapper{
		mapper: newMapper(cfg.RulePath, logger),
	}
	m.cleanup()

	return m
}

func (m *cmkMapper) MapRules(user string, ruleConfigs map[string][]rulefmt.RuleGroup) (bool, []string, error) {
	if cmk.Config.PreCreationHook != nil {
		userLogger := logutil.WithUserID(user, m.logger)
		udir := filepath.Join(m.Path, user)
		// We don't need to suspend as the folder will be deleted on next sync
		err := cmk.Config.PreCreationHook(user, udir, userLogger,
			func() { allowedTenants.enable(user) },
			func(err error) error {
				allowedTenants.disable(user, other)
				return nil
			},
		)

		if err != nil {
			if err.Error() == errCmkConfigurationNotSet {
				allowedTenants.disable(user, keyPropagationDelay)
			} else {
				allowedTenants.disable(user, other)
			}
			return true, []string{}, nil
		}
	}
	allowedTenants.enable(user)
	return m.mapper.MapRules(user, ruleConfigs)
}

type cmkAllowedTenants struct {
	m sync.RWMutex
	*util.AllowedTenants
	// If empty, no tenants are disabled. If not empty, tenants in the map are disabled.
	disabled map[string]DisabledReason
	limits   RulesLimits
}

func (a *cmkAllowedTenants) disable(tenantID string, reason DisabledReason) {
	a.m.Lock()
	defer a.m.Unlock()
	a.disabled[tenantID] = reason
}

func (a *cmkAllowedTenants) enable(tenantID string) {
	a.m.Lock()
	defer a.m.Unlock()
	delete(a.disabled, tenantID)
}

func (a *cmkAllowedTenants) set(limits RulesLimits, tenants *util.AllowedTenants) {
	a.m.Lock()
	defer a.m.Unlock()
	allowedTenants.limits = limits
	allowedTenants.AllowedTenants = tenants
}

func (a *cmkAllowedTenants) IsAllowed(tenantID string) bool {
	a.m.RLock()
	defer a.m.RUnlock()
	if reason, ok := a.disabled[tenantID]; ok {
		if reason == keyPropagationDelay {
			// allow tenant if cmk config is propagated
			propagated := a.limits.S3SSEKMSKeyID(tenantID) != "" && a.limits.KMSEncryptionWorkspaceKey(tenantID) != ""
			return propagated && a.AllowedTenants.IsAllowed(tenantID)
		}
		return false
	}
	return a.AllowedTenants.IsAllowed(tenantID)
}
