package ingester

import (
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"

	"github.com/cortexproject/cortex/pkg/cmk"
	logutil "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

type errPreHook struct {
	cause error
}

func (e *errPreHook) Error() string {
	return e.cause.Error()
}

func (i *Ingester) getSuspendedTsdb(userID string) error {
	i.suspendedTsdbsMtx.RLock()
	defer i.suspendedTsdbsMtx.RUnlock()
	return i.suspendedTsdbs[userID]
}

func (i *Ingester) resumeTsdb(userID string) bool {
	if err := i.preCreationHook(userID); err != nil {
		level.Warn(i.logger).Log("msg", "failed to resume tsdb", "user", userID, "err", err)
		return false
	}
	// Create the database and a shipper for a user
	db, err := i.createTSDB(userID)
	if err != nil {
		return false
	}
	i.stoppedMtx.Lock()
	i.TSDBState.dbs[userID] = db
	i.stoppedMtx.Unlock()
	i.metrics.memUsers.Inc()

	i.suspendedTsdbsMtx.Lock()
	if i.suspendedTsdbs == nil {
		i.suspendedTsdbs = map[string]error{}
	}
	delete(i.suspendedTsdbs, userID)
	i.suspendedTsdbsMtx.Unlock()
	return true
}

func (i *Ingester) preCreationHook(userID string) error {
	udir := i.cfg.BlocksStorageConfig.TSDB.BlocksDir(userID)
	if cmk.Config.PreCreationHook != nil {
		userLogger := logutil.WithUserID(userID, i.logger)
		err := cmk.Config.PreCreationHook(userID, udir, userLogger,
			func() { i.resumeTsdb(userID) },
			func(err error) error { return i.suspendTsdb(userID, err) },
		)

		if err != nil {
			if e := i.suspendTsdb(userID, err); e != nil {
				level.Warn(userLogger).Log("msg", "error suspending cmk workspace", "err", e)
			}
			return &errPreHook{cause: err}
		}
	}

	return nil
}

func (i *Ingester) suspendTsdb(userID string, err error) error {
	userLogger := logutil.WithUserID(userID, i.logger)
	// Adding user on the suspended TSDB map
	i.suspendedTsdbsMtx.Lock()
	if i.suspendedTsdbs == nil {
		i.suspendedTsdbs = map[string]error{}
	}
	i.suspendedTsdbs[userID] = err
	i.suspendedTsdbsMtx.Unlock()

	userDB, err := i.getTSDB(userID)

	if err != nil || userDB == nil || userDB.db == nil {
		return nil
	}
	// This disables pushes and force-compactions. Not allowed to close while shipping is in progress.
	if !userDB.casState(active, closing) {
		return errors.New("force compaction")
	}

	// If TSDB is fully closed, we will set state to 'closed', which will prevent this defered closing -> active transition.
	defer userDB.casState(closing, active)

	// Make sure we don't ignore any possible inflight pushes.
	userDB.pushesInFlight.Wait()

	if err := userDB.Close(); err != nil {
		level.Error(userLogger).Log("msg", "failed to close idle TSDB", "err", err)
		return err
	}

	// This will prevent going back to "active" state in deferred statement.
	userDB.casState(closing, closed)

	// Only remove user from TSDBState when everything is cleaned up
	// This will prevent concurrency problems when cortex are trying to open new TSDB - Ie: New request for a given tenant
	// came in - while closing the tsdb for the same tenant.
	// If this happens now, the request will get reject as the push will not be able to acquire the lock as the tsdb will be
	// in closed state
	defer func() {
		i.stoppedMtx.Lock()
		delete(i.TSDBState.dbs, userDB.userID)
		userDB.db = nil
		i.stoppedMtx.Unlock()
	}()

	i.metrics.memUsers.Dec()
	i.TSDBState.tsdbMetrics.removeRegistryForUser(userDB.userID)

	i.deleteUserMetadata(userDB.userID)
	i.metrics.deletePerUserMetrics(userDB.userID)

	validation.DeletePerUserValidationMetrics(i.validateMetrics, userDB.userID, i.logger)

	return nil
}
