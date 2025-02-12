package parquetconverter

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/cortexproject/cortex/pkg/cmk"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/parquet"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestConverter_ShouldCallHooks(t *testing.T) {
	cfg := prepareConfig()
	user := "user"
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })
	dir := t.TempDir()

	cfg.Ring.InstanceID = "parquet-converter-1"
	cfg.Ring.InstanceAddr = "1.2.3.4"
	cfg.Ring.KVStore.Mock = ringStore
	bucketClient, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	userBucket := bucket.NewPrefixedBucketClient(bucketClient, user)
	limits := &validation.Limits{}
	flagext.DefaultValues(limits)
	limits.ParquetConverterEnabled = true
	preHookCalled := false

	c, logger, _ := prepare(t, cfg, objstore.WithNoopInstr(bucketClient), limits)
	cmk.Config.PreCreationHook = func(user string, folder string, _ log.Logger, _ func(), _ func(err error) error) error {
		require.Equal(t, "user", user)
		require.Equal(t, c.compactDirForUser("user"), folder)
		preHookCalled = true
		return nil
	}

	ctx := context.Background()

	lbls := labels.Labels{labels.Label{
		Name:  "__name__",
		Value: "test",
	}}

	blocks := []ulid.ULID{}
	// Create blocks
	for _, duration := range []time.Duration{24 * time.Hour} {
		rnd := rand.New(rand.NewSource(time.Now().Unix()))
		id, err := e2e.CreateBlock(ctx, rnd, dir, []labels.Labels{lbls}, 2, 0, duration.Milliseconds(), time.Minute.Milliseconds(), 10)
		require.NoError(t, err)
		blocks = append(blocks, id)
	}

	for _, bIds := range blocks {
		blockDir := fmt.Sprintf("%s/%s", dir, bIds.String())
		b, err := tsdb.OpenBlock(nil, blockDir, nil, nil)
		require.NoError(t, err)
		err = block.Upload(ctx, logger, userBucket, b.Dir(), metadata.NoneFunc)
		require.NoError(t, err)
	}

	// Try to start the converter with a bad consul kv-store. The
	err = services.StartAndAwaitRunning(context.Background(), c)
	require.NoError(t, err)
	defer services.StopAndAwaitTerminated(ctx, c) // nolint:errcheck

	blocksConverted := []ulid.ULID{}

	test.Poll(t, 3*time.Minute, 1, func() interface{} {
		blocksConverted = blocksConverted[:0]
		for _, bIds := range blocks {
			m, err := parquet.ReadConverterMark(ctx, bIds, userBucket, logger)
			require.NoError(t, err)
			if m.Version == parquet.CurrentVersion {
				blocksConverted = append(blocksConverted, bIds)
			}
		}
		return len(blocksConverted)
	})

	// Verify metrics after conversion
	require.Equal(t, float64(len(blocksConverted)), testutil.ToFloat64(c.metrics.convertedBlocks.WithLabelValues(user)))
	require.Greater(t, testutil.ToFloat64(c.metrics.convertBlockDuration.WithLabelValues(user)), 0.0)
	require.Equal(t, 1.0, testutil.ToFloat64(c.metrics.ownedUsers))

	// Verify all files are there
	for _, block := range blocksConverted {
		for _, file := range []string{
			fmt.Sprintf("%s/parquet-converter-mark.json", block.String()),
			fmt.Sprintf("parquet-markers/%s-parquet-converter-mark.json", block.String()),
			fmt.Sprintf("%s/0.chunks.parquet", block.String()),
			fmt.Sprintf("%s/0.labels.parquet", block.String()),
		} {
			ok, err := userBucket.Exists(ctx, file)
			require.NoError(t, err)
			require.True(t, ok)
		}
	}

	// Make sure the pre hook was called.
	require.True(t, preHookCalled)

	// Reset hooks config
	cmk.Config = cmk.FilesystemHooksConfig{}
}
