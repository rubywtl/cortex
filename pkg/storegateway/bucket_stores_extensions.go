package storegateway

import (
	"context"
	"sync"

	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc/codes"

	"github.com/cortexproject/cortex/pkg/cmk"
	logutil "github.com/cortexproject/cortex/pkg/util/log"
)

var (
	errPreHook = errors.New("pre hook error")

	suspendedTsdbsMu = sync.RWMutex{}
	suspendedTsdbs   = map[string]struct{}{}
)

func (u *BucketStores) preCreationHook(ctx context.Context, user string, udir string) error {
	if cmk.Config.PreCreationHook != nil {
		userLogger := logutil.WithUserID(user, u.logger)
		err := cmk.Config.PreCreationHook(user, udir, userLogger,
			func() { u.resumeUser(ctx, user) },
			func(err error) error { return u.suspendUser(user, err) },
		)

		if err != nil {
			return errPreHook
		}
	}

	return nil
}

func (u *BucketStores) resumeUser(ctx context.Context, user string) {
	userLogger := logutil.WithUserID(user, u.logger)

	suspendedTsdbsMu.RLock()
	_, ok := suspendedTsdbs[user]
	suspendedTsdbsMu.RUnlock()

	// user already resumed
	if !ok {
		level.Info(userLogger).Log("msg", "skipping resuming user as it is already resumed")
		return
	}

	if s, err := u.getOrCreateStore(user); err == nil {
		if err := s.SyncBlocks(ctx); err != nil {
			userLogger := logutil.WithUserID(user, u.logger)
			level.Warn(userLogger).Log("msg", "error sync blocks after resume", "err", err)
		}
	}
	u.storesErrorsMu.Lock()
	delete(u.storesErrors, user)
	u.storesErrorsMu.Unlock()
	suspendedTsdbsMu.Lock()
	delete(suspendedTsdbs, user)
	suspendedTsdbsMu.Unlock()
}

func (u *BucketStores) suspendUser(user string, err error) error {
	userLogger := logutil.WithUserID(user, u.logger)

	suspendedTsdbsMu.RLock()
	_, ok := suspendedTsdbs[user]
	suspendedTsdbsMu.RUnlock()

	// User already suspended
	if ok {
		level.Info(userLogger).Log("msg", "skipping supend user as it is already suspended")
		// call post hook anyway to make sure we flush fscrypt
		return nil
	}

	// Suspend the user
	u.storesErrorsMu.Lock()
	u.storesErrors[user] = httpgrpc.Errorf(int(codes.PermissionDenied), "store error: %s", err)
	u.storesErrorsMu.Unlock()

	u.storesMu.Lock()
	s := u.stores[user]
	delete(u.stores, user)
	u.storesMu.Unlock()

	if s != nil {
		if err := s.Close(); err != nil {
			return err
		}
	}

	u.metaFetcherMetrics.RemoveUserRegistry(user)
	u.bucketStoreMetrics.RemoveUserRegistry(user)
	suspendedTsdbsMu.Lock()
	suspendedTsdbs[user] = struct{}{}
	suspendedTsdbsMu.Unlock()
	return nil
}
