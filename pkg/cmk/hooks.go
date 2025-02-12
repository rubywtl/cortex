package cmk

import (
	"github.com/go-kit/log"
)

var (
	Config FilesystemHooksConfig
)

type PreCreationHook func(user string, folder string, logger log.Logger, open func(), close func(err error) error) error

type FilesystemHooksConfig struct {
	PreCreationHook PreCreationHook `yaml:"-"`
}
