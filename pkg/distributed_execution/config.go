package distributed_execution

import "time"

type DistributedExec_Config struct {
	DistributedExecEnabled bool `yaml:"distributed_exec_enabled" doc:"hidden"`

	// scheduler configs
	FragmentTrackerExpiration time.Duration `yaml:"fragment_tracker_expiration" doc:"hidden"`

	// querier execution configs
	RetryWaitTime              time.Duration `yaml:"retry_wait_time" doc:"hidden"`
	MaxRetries                 int           `yaml:"max_retries" doc:"hidden"`
	QueryTrackerExpirationTime time.Duration `yaml:"query_tracker_expiration_time" doc:"hidden"`
}
