package ingester

import (
	"testing"

	"github.com/pkg/errors"

	"github.com/stretchr/testify/require"
)

func Test_preHookError(t *testing.T) {
	err := errors.New("internalError")
	var pErr *errPreHook
	preHookError := &errPreHook{cause: err}
	require.True(t, errors.As(preHookError, &pErr))
	require.Equal(t, pErr.Error(), "internalError")
}
