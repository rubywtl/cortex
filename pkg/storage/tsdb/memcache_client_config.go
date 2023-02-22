package tsdb

import (
	"flag"
	"strings"
	"time"

	"github.com/thanos-io/thanos/pkg/cacheutil"
	"github.com/thanos-io/thanos/pkg/model"
)

type MemcachedClientConfig struct {
	Addresses              string               `yaml:"addresses"`
	Timeout                time.Duration        `yaml:"timeout"`
	MaxIdleConnections     int                  `yaml:"max_idle_connections"`
	MaxAsyncConcurrency    int                  `yaml:"max_async_concurrency"`
	MaxAsyncBufferSize     int                  `yaml:"max_async_buffer_size"`
	MaxGetMultiConcurrency int                  `yaml:"max_get_multi_concurrency"`
	MaxGetMultiBatchSize   int                  `yaml:"max_get_multi_batch_size"`
	SetAsyncCircuitBreaker CircuitBreakerConfig `yaml:"set_async_circuit_breaker_config"`

	MaxItemSize           int    `yaml:"max_item_size"`
	AutoDiscovery         bool   `yaml:"auto_discovery"`
	TLSEnabled            bool   `yaml:"tls_enabled"`
	TLSCertPath           string `yaml:"tls_cert_path"`
	TLSKeyPath            string `yaml:"tls_key_path"`
	TLSCAPath             string `yaml:"tls_ca_path"`
	TLSServerName         string `yaml:"tls_ca_name"`
	TLSInsecureSkipVerify bool   `yaml:"tls_insecure_skip_verify"`
}

func (cfg *MemcachedClientConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Addresses, prefix+"addresses", "", "Comma separated list of memcached addresses. Supported prefixes are: dns+ (looked up as an A/AAAA query), dnssrv+ (looked up as a SRV query, dnssrvnoa+ (looked up as a SRV query, with no A/AAAA lookup made after that).")
	f.DurationVar(&cfg.Timeout, prefix+"timeout", 100*time.Millisecond, "The socket read/write timeout.")
	f.IntVar(&cfg.MaxIdleConnections, prefix+"max-idle-connections", 16, "The maximum number of idle connections that will be maintained per address.")
	f.IntVar(&cfg.MaxAsyncConcurrency, prefix+"max-async-concurrency", 3, "The maximum number of concurrent asynchronous operations can occur.")
	f.IntVar(&cfg.MaxAsyncBufferSize, prefix+"max-async-buffer-size", 10000, "The maximum number of enqueued asynchronous operations allowed.")
	f.IntVar(&cfg.MaxGetMultiConcurrency, prefix+"max-get-multi-concurrency", 100, "The maximum number of concurrent connections running get operations. If set to 0, concurrency is unlimited.")
	f.IntVar(&cfg.MaxGetMultiBatchSize, prefix+"max-get-multi-batch-size", 0, "The maximum number of keys a single underlying get operation should run. If more keys are specified, internally keys are split into multiple batches and fetched concurrently, honoring the max concurrency. If set to 0, the max batch size is unlimited.")
	f.IntVar(&cfg.MaxItemSize, prefix+"max-item-size", 1024*1024, "The maximum size of an item stored in memcached. Bigger items are not stored. If set to 0, no maximum size is enforced.")
	f.BoolVar(&cfg.AutoDiscovery, prefix+"auto-discovery", false, "Use memcached auto-discovery mechanism provided by some cloud provider like GCP and AWS")
	cfg.SetAsyncCircuitBreaker.RegisterFlagsWithPrefix(f, prefix+"set-async.")
	f.BoolVar(&cfg.TLSEnabled, prefix+"tls-enabled", false, "Enable TLS in the memcached client. This flag needs to be enabled when any other TLS flag is set. If set to false, insecure connection to memcached server will be used.")
	f.StringVar(&cfg.TLSCertPath, prefix+"tls-cert-path", "", "Path to the client certificate file, which will be used for authenticating with the server. Also requires the key path to be configured.")
	f.StringVar(&cfg.TLSKeyPath, prefix+"tls-key-path", "", "Path to the key file for the client certificate. Also requires the client certificate to be configured.")
	f.StringVar(&cfg.TLSCAPath, prefix+"tls-ca-path", "", "Path to the CA certificates file to validate server certificate against. If not set, the host's root CA certificates are used.")
	f.StringVar(&cfg.TLSServerName, prefix+"tls-server-name", "", "Override the expected name on the server certificate.")
	f.BoolVar(&cfg.TLSInsecureSkipVerify, prefix+"tls-insecure-skip-verify", false, "Skip validating server certificate.")
}

func (cfg *MemcachedClientConfig) GetAddresses() []string {
	if cfg.Addresses == "" {
		return []string{}
	}

	return strings.Split(cfg.Addresses, ",")
}

// Validate the config.
func (cfg *MemcachedClientConfig) Validate() error {
	if len(cfg.GetAddresses()) == 0 {
		return errNoIndexCacheAddresses
	}

	return nil
}

func (cfg MemcachedClientConfig) ToMemcachedClientConfig() cacheutil.MemcachedClientConfig {
	return cacheutil.MemcachedClientConfig{
		Addresses:                 cfg.GetAddresses(),
		Timeout:                   cfg.Timeout,
		MaxIdleConnections:        cfg.MaxIdleConnections,
		MaxAsyncConcurrency:       cfg.MaxAsyncConcurrency,
		MaxAsyncBufferSize:        cfg.MaxAsyncBufferSize,
		MaxGetMultiConcurrency:    cfg.MaxGetMultiConcurrency,
		MaxGetMultiBatchSize:      cfg.MaxGetMultiBatchSize,
		MaxItemSize:               model.Bytes(cfg.MaxItemSize),
		DNSProviderUpdateInterval: 30 * time.Second,
		AutoDiscovery:             cfg.AutoDiscovery,
		SetAsyncCircuitBreaker: cacheutil.CircuitBreakerConfig{
			Enabled:             cfg.SetAsyncCircuitBreaker.Enabled,
			HalfOpenMaxRequests: uint32(cfg.SetAsyncCircuitBreaker.HalfOpenMaxRequests),
			OpenDuration:        cfg.SetAsyncCircuitBreaker.OpenDuration,
			MinRequests:         uint32(cfg.SetAsyncCircuitBreaker.MinRequests),
			ConsecutiveFailures: uint32(cfg.SetAsyncCircuitBreaker.ConsecutiveFailures),
			FailurePercent:      cfg.SetAsyncCircuitBreaker.FailurePercent,
		},
		TlsEnabled:            cfg.TLSEnabled,
		TLSCertPath:           cfg.TLSCertPath,
		TLSKeyPath:            cfg.TLSKeyPath,
		TLSCAPath:             cfg.TLSCAPath,
		TLSServerName:         cfg.TLSServerName,
		TLSInsecureSkipVerify: cfg.TLSInsecureSkipVerify,
	}
}
