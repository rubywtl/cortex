package alertmanager

import (
	"github.com/cortexproject/cortex/pkg/cmk"
	logutil "github.com/cortexproject/cortex/pkg/util/log"
)

func (am *MultitenantAlertmanager) preCreationHook(userID string, udir string) error {
	if cmk.Config.PreCreationHook != nil {
		userLogger := logutil.WithUserID(userID, am.logger)
		// We don't need to suspend as the folder will be deleted on next sync
		return cmk.Config.PreCreationHook(userID, udir, userLogger,
			func() {},
			func(err error) error { return nil },
		)
	}

	return nil
}
