package compactor

import (
	"github.com/cortexproject/cortex/pkg/cmk"
	logutil "github.com/cortexproject/cortex/pkg/util/log"
)

func (c *Compactor) preCreationHook(userID string, udir string) error {
	if cmk.Config.PreCreationHook != nil {
		userLogger := logutil.WithUserID(userID, c.logger)
		return cmk.Config.PreCreationHook(userID, udir, userLogger,
			func() {},
			func(err error) error { return nil },
		)
	}

	return nil
}
