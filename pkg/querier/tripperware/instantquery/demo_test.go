package instantquery

import (
	"context"
	"fmt"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
	"net/http"
	"testing"
	"time"
)

func TestRequestDistributedExec(t *testing.T) {
	t.Parallel()
	codec := testInstantQueryCodec

	for _, tc := range []struct {
		url         string
		expectedURL string
		expected    *tripperware.PrometheusRequest
		expectedErr error
	}{
		{
			url:         "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29&stats=all&time=1536673680&distributedExec=false",
			expectedURL: "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29&stats=all&time=1536673680",
			expected: &tripperware.PrometheusRequest{
				Path:  "/api/v1/query",
				Time:  1536673680 * 1e3,
				Query: "sum(container_memory_rss) by (namespace)",
				Stats: "all",
				Headers: map[string][]string{
					"Test-Header": {"test"},
				},
				DistributedExec: false,
			},
		},
		{
			url:         "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29&time=1536673680&distributedExec=false",
			expectedURL: "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29&time=1536673680",
			expected: &tripperware.PrometheusRequest{
				Path:  "/api/v1/query",
				Time:  1536673680 * 1e3,
				Query: "sum(container_memory_rss) by (namespace)",
				Stats: "",
				Headers: map[string][]string{
					"Test-Header": {"test"},
				},
				DistributedExec: false,
			},
		},
		{
			url:         "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29&time=1536673680&distributedExec=true",
			expectedURL: "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29&time=1536673680",
			expected: &tripperware.PrometheusRequest{
				Path:  "/api/v1/query",
				Time:  1536673680 * 1e3,
				Query: "sum(container_memory_rss) by (namespace)",
				Stats: "",
				Headers: map[string][]string{
					"Test-Header": {"test"},
				},
				DistributedExec: true,
			},
		},
		{
			url:         "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29&time=1536673680",
			expectedURL: "/api/v1/query?query=sum%28container_memory_rss%29+by+%28namespace%29&time=1536673680",
			expected: &tripperware.PrometheusRequest{
				Path:  "/api/v1/query",
				Time:  1536673680 * 1e3,
				Query: "sum(container_memory_rss) by (namespace)",
				// if empty, it should still be run (treated as true)
				DistributedExec: true,
				Stats:           "",
				Headers: map[string][]string{
					"Test-Header": {"test"},
				},
			},
		},
	} {
		tc := tc
		t.Run(tc.url, func(t *testing.T) {
			t.Parallel()
			r, err := http.NewRequest("POST", tc.url, http.NoBody)
			require.NoError(t, err)
			r.Header.Add("Test-Header", "test")

			ctx := user.InjectOrgID(context.Background(), "1")

			r = r.Clone(ctx)

			if tc.expected.Time == 0 {
				now := time.Now()
				tc.expectedURL = fmt.Sprintf("%s%d", tc.expectedURL, now.Unix())
				tc.expected.Time = now.Unix() * 1e3
			}
			req, err := codec.DecodeRequest(ctx, r, []string{"Test-Header"})
			if err != nil {
				require.EqualValues(t, tc.expectedErr, err)
				return
			}
			require.EqualValues(t, tc.expected, req)

			rdash, err := codec.EncodeRequest(context.Background(), req)
			require.NoError(t, err)
			require.EqualValues(t, tc.expectedURL, rdash.RequestURI)

			if !tc.expected.DistributedExec {
				require.Empty(t, tc.expected.LogicalPlan)
			}
		})
	}
}
