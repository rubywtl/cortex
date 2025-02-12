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

var (
	allowedTenants = &cmkAllowedTenants{disabled: make(map[string]struct{})}
)

type cmkMapper struct {
	*mapper
}

func newMapperCmk(cfg Config, logger log.Logger) *cmkMapper {
	m := &cmkMapper{
		mapper: newMapper(cfg.RulePath, logger),
	}
	allowedTenants.m.Lock()
	defer allowedTenants.m.Unlock()
	allowedTenants.AllowedTenants = util.NewAllowedTenants(cfg.EnabledTenants, cfg.DisabledTenants)
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
				allowedTenants.disable(user)
				return nil
			},
		)

		if err != nil {
			allowedTenants.disable(user)
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
	disabled map[string]struct{}
}

func (a *cmkAllowedTenants) disable(tenantID string) {
	a.m.Lock()
	defer a.m.Unlock()
	a.disabled[tenantID] = struct{}{}
}

func (a *cmkAllowedTenants) enable(tenantID string) {
	a.m.Lock()
	defer a.m.Unlock()
	delete(a.disabled, tenantID)
}

func (a *cmkAllowedTenants) IsAllowed(tenantID string) bool {
	a.m.RLock()
	defer a.m.RUnlock()
	if _, ok := a.disabled[tenantID]; ok {
		return false
	}

	return a.AllowedTenants.IsAllowed(tenantID)
}
