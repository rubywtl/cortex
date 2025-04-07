package partialdata

import (
	"errors"
)

type IsCfgEnabledFunc func(userID string) bool

var ErrPartialData = errors.New("Please retry later, the query may not have processed all recently ingested time series") //nolint:staticcheck

func IsPartialDataError(err error) bool {
	return errors.Is(err, ErrPartialData)
}
