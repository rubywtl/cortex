package compactor

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-kit/log"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/pkg/cmk"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/users"
	"github.com/cortexproject/cortex/pkg/util/services"
	cortex_testutil "github.com/cortexproject/cortex/pkg/util/test"
)

func TestCompactor_ShouldCallHooks(t *testing.T) {
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockGet(users.UserIndexCompressedFilename, "", nil)
	bucketClient.MockIter("", []string{"user-1"}, nil)
	bucketClient.MockExists(cortex_tsdb.GetLocalDeletionMarkPath("user-1"), false, nil)
	bucketClient.MockExists(cortex_tsdb.GetGlobalDeletionMarkPath("user-1"), false, nil)
	bucketClient.MockIter("user-1/", []string{"01DTVP434PA9VFXSW2JKB3392D", "user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", "01FN6CDF3PNEWWRY5MPGJPE3EX", "user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/meta.json"}, nil)
	bucketClient.MockIter("user-1/markers/", nil, nil)
	bucketClient.MockIter("__markers__", nil, nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", mockBlockMetaJSON("01DTVP434PA9VFXSW2JKB3392D"), nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-1/bucket-index-sync-status.json", "", nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/meta.json", mockBlockMetaJSON("01FN6CDF3PNEWWRY5MPGJPE3EX"), nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/deletion-mark.json", "", nil)
	bucketClient.MockGet("user-1/01FN6CDF3PNEWWRY5MPGJPE3EX/no-compact-mark.json", "", nil)
	bucketClient.MockGet("user-1/bucket-index.json.gz", "", nil)
	bucketClient.MockGet("user-1/bucket-index-sync-status.json", "", nil)
	bucketClient.MockIter("user-1/markers/", nil, nil)
	bucketClient.MockUpload("user-1/bucket-index.json.gz", nil)
	bucketClient.MockUpload("user-1/bucket-index-sync-status.json", nil)
	bucketClient.MockGet("user-1/markers/cleaner-visit-marker.json", "", nil)
	bucketClient.MockUpload("user-1/markers/cleaner-visit-marker.json", nil)
	bucketClient.MockDelete("user-1/markers/cleaner-visit-marker.json", nil)

	c, _, tsdbPlanner, _, _ := prepare(t, prepareConfig(), bucketClient, nil)
	preHookCalled := false

	cmk.Config.PreCreationHook = func(user string, folder string, _ log.Logger, _ func(), _ func(err error) error) error {
		require.Equal(t, "user-1", user)
		require.Equal(t, c.compactDirForUser("user-1"), folder)
		preHookCalled = true
		return nil
	}

	// Make sure the user folder is created and is being used
	// This will be called during compaction
	tsdbPlanner.On("Plan", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		_, err := os.Stat(c.compactDirForUser("user-1"))
		require.NoError(t, err)
	}).Return([]*metadata.Meta{}, nil)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	// Wait until a run has completed.
	cortex_testutil.Poll(t, time.Second, 1.0, func() interface{} {
		return prom_testutil.ToFloat64(c.CompactionRunsCompleted)
	})
	require.True(t, preHookCalled)
	_, err := os.Stat(c.compactDirForUser("user-1"))
	require.True(t, os.IsNotExist(err))

	// Reset hooks config
	cmk.Config = cmk.FilesystemHooksConfig{}
}
