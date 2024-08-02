package ruler

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/weaveworks/common/user"

	util_api "github.com/cortexproject/cortex/pkg/util/api"

	"github.com/cortexproject/cortex/pkg/cortexpb"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ruler/rulespb"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestRuler_ruleinfos(t *testing.T) {
	rules := map[string]rulespb.RuleGroupList{
		"user1": {
			&rulespb.RuleGroupDesc{
				Name:      "group1",
				Namespace: "namespace1",
				User:      "user1",
				Rules: []*rulespb.RuleDesc{
					{
						Record: "UP_RULE",
						Expr:   "up",
					},
					{
						Alert: "UP_ALERT",
						Expr:  "up < 1",
					},
				},
				Interval: interval,
			},
			&rulespb.RuleGroupDesc{
				Name:      "group2",
				Namespace: "namespace2",
				User:      "user1",
				Rules: []*rulespb.RuleDesc{
					{
						Record: "UP_RULE",
						Expr:   "up",
					},
					{
						Alert: "UP_ALERT",
						Expr:  "up < 1",
					},
				},
				Interval: interval,
			},
		},
	}
	store := newMockRuleStore(rules, nil)
	cfg := defaultRulerConfig(t)

	r := newTestRuler(t, cfg, store, nil)
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	a := NewAPI(r, r.store, log.NewNopLogger())

	tc := []struct {
		name          string
		input         string
		output        *RuleInfoDiscovery
		err           string
		errorType     string
		statusCode    int
		status        string
		maxAlerts     *int
		maxRuleGroups *int
	}{
		{
			name:          "with no limit",
			statusCode:    200,
			status:        "success",
			maxAlerts:     intPtr(1),
			maxRuleGroups: nil,
			output: &RuleInfoDiscovery{
				RuleGroups: []*RuleGroup{
					{
						Name: "group2",
						File: "namespace2",
						Rules: []rule{
							&recordingRule{
								Name:   "UP_RULE",
								Query:  "up",
								Health: "unknown",
								Type:   "recording",
							},
							&alertingRuleInfo{
								Name:   "UP_ALERT",
								Query:  "up < 1",
								State:  "inactive",
								Health: "unknown",
								Type:   "alerting",
								AlertInfo: AlertInfo{
									Alerts:  []*Alert{},
									HasMore: false,
								},
							},
						},
						Interval: 10,
					},
					{
						Name: "group1",
						File: "namespace1",
						Rules: []rule{
							&recordingRule{
								Name:   "UP_RULE",
								Query:  "up",
								Health: "unknown",
								Type:   "recording",
							},
							&alertingRuleInfo{
								Name:   "UP_ALERT",
								Query:  "up < 1",
								State:  "inactive",
								Health: "unknown",
								Type:   "alerting",
								AlertInfo: AlertInfo{
									Alerts:  []*Alert{},
									HasMore: false,
								},
							},
						},
						Interval: 10,
					},
				},
				NextToken: "",
			},
			err:       "",
			errorType: "",
		},
		{
			name:          "limit on rule groups",
			statusCode:    200,
			status:        "success",
			maxRuleGroups: intPtr(1),
			output: &RuleInfoDiscovery{
				RuleGroups: []*RuleGroup{
					{
						Name: "group2",
						File: "namespace2",
						Rules: []rule{
							&recordingRule{
								Name:   "UP_RULE",
								Query:  "up",
								Health: "unknown",
								Type:   "recording",
							},
							&alertingRuleInfo{
								Name:   "UP_ALERT",
								Query:  "up < 1",
								State:  "inactive",
								Health: "unknown",
								Type:   "alerting",
								AlertInfo: AlertInfo{
									Alerts:  []*Alert{},
									HasMore: false,
								},
							},
						},
						Interval: 10,
					},
				},
				NextToken: getRuleGroupNextToken("namespace2", "group2"),
			},
			err:       "",
			errorType: "",
		},
		{
			name:          "bad maxAlerts",
			statusCode:    400,
			status:        "error",
			maxAlerts:     intPtr(-1),
			maxRuleGroups: nil,
			output:        nil,
			err:           "maxAlerts need to be a valid number and larger than 0",
			errorType:     "bad_data",
		},
		{
			name:          "bad maxRuleGroups",
			statusCode:    400,
			status:        "error",
			maxAlerts:     nil,
			maxRuleGroups: intPtr(-1),
			output:        nil,
			err:           "maxRuleGroups need to be a valid number and larger than 0",
			errorType:     "bad_data",
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			req := requestFor(t, "GET", "https://localhost:8080/api/prom/api/v1/ruleinfos", nil, "user1")
			urlValues := req.URL.Query()
			if tt.maxAlerts != nil {
				addQueryParams(urlValues, "maxAlerts", strconv.Itoa(*tt.maxAlerts))
			}
			if tt.maxRuleGroups != nil {
				addQueryParams(urlValues, "maxRuleGroups", strconv.Itoa(*tt.maxRuleGroups))
			}
			req.URL.RawQuery = urlValues.Encode()
			w := httptest.NewRecorder()
			a.PrometheusRuleInfos(w, req)

			resp := w.Result()
			body, _ := io.ReadAll(resp.Body)

			// Check status code and status response
			responseJSON := util_api.Response{}
			err := json.Unmarshal(body, &responseJSON)
			require.NoError(t, err)
			require.Equal(t, tt.statusCode, resp.StatusCode)
			require.Equal(t, tt.status, responseJSON.Status)

			// Testing the running rules for user1 in the mock store
			res := util_api.Response{
				Status:    tt.status,
				ErrorType: v1.ErrorType(tt.errorType),
				Error:     tt.err,
			}
			if tt.output != nil {
				res.Data = tt.output
			}
			expectedResponse, _ := json.Marshal(res)

			require.Equal(t, string(expectedResponse), string(body))
		})
	}
}

func TestRuler_alertinfos(t *testing.T) {
	alertsZeroFunc := func(i *AlertInfoDiscovery) {
		if i != nil {
			for _, alert := range i.Alerts {
				alert.ActiveAt = nil
				alert.State = "fake"
			}
		}
	}

	rules := map[string]rulespb.RuleGroupList{
		"user1": {
			&rulespb.RuleGroupDesc{
				Name:      "group1",
				Namespace: "namespace1",
				User:      "user1",
				Rules: []*rulespb.RuleDesc{
					{
						Record: "UP_RULE",
						Expr:   "up",
					},
					{
						Alert: "UP_ALERT_1",
						Expr:  "1",
					},
				},
				Interval: interval,
			},
			&rulespb.RuleGroupDesc{
				Name:      "group2",
				Namespace: "namespace2",
				User:      "user1",
				Rules: []*rulespb.RuleDesc{
					{
						Record: "UP_RULE",
						Expr:   "up",
					},
					{
						Alert: "UP_ALERT_2",
						Expr:  "1",
					},
				},
				Interval: interval,
			},
		},
	}
	store := newMockRuleStore(rules, nil)
	cfg := defaultRulerConfig(t)

	r := newTestRuler(t, cfg, store, nil)
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	ruleGroup := r.manager.GetRules("user1")

	// NEXT, evaluate the rule group the first time and assert
	ctx := user.InjectOrgID(context.Background(), "user1")
	for _, rg := range ruleGroup {
		rg.Eval(ctx, time.Now().UTC())
	}

	a := NewAPI(r, r.store, log.NewNopLogger())

	tc := []struct {
		name       string
		output     *AlertInfoDiscovery
		err        string
		errorType  string
		statusCode int
		status     string
		maxResults *int
		nextToken  string
		files      []string
	}{
		{
			name:       "with no limit",
			statusCode: 200,
			status:     "success",
			output: &AlertInfoDiscovery{
				Alerts: []*Alert{
					{
						Labels: labels.Labels{
							{Name: "alertname", Value: "UP_ALERT_2"},
						},
						State:    "fake",
						ActiveAt: nil,
						Value:    "1e+00",
					},
					{
						Labels: labels.Labels{
							{Name: "alertname", Value: "UP_ALERT_1"},
						},
						State:    "fake",
						ActiveAt: nil,
						Value:    "1e+00",
					},
				},
				NextToken: "",
			},
			err:       "",
			errorType: "",
		},
		{
			name:       "with no limit but file filter",
			statusCode: 200,
			status:     "success",
			files:      []string{"namespace1"},
			output: &AlertInfoDiscovery{
				Alerts: []*Alert{
					{
						Labels: labels.Labels{
							{Name: "alertname", Value: "UP_ALERT_1"},
						},
						State:    "fake",
						ActiveAt: nil,
						Value:    "1e+00",
					},
				},
				NextToken: "",
			},
			err:       "",
			errorType: "",
		},
		{
			name:       "with 1 alert limit",
			statusCode: 200,
			status:     "success",
			maxResults: intPtr(1),
			output: &AlertInfoDiscovery{
				Alerts: []*Alert{
					{
						Labels: labels.Labels{
							{Name: "alertname", Value: "UP_ALERT_2"},
						},
						State:    "fake",
						ActiveAt: nil,
						Value:    "1e+00",
					},
				},
				NextToken: getAlertNextToken(
					"namespace2", "group2", "UP_ALERT_2", "1",
					[]cortexpb.LabelAdapter{
						{Name: "alertname", Value: "UP_ALERT_2"},
					}),
			},
			err:       "",
			errorType: "",
		},
		{
			name:       "with start next token",
			statusCode: 200,
			status:     "success",
			maxResults: intPtr(1),
			nextToken: getAlertNextToken(
				"namespace2", "group2", "UP_ALERT_2", "1",
				[]cortexpb.LabelAdapter{
					{Name: "alertname", Value: "UP_ALERT_2"},
				}),
			output: &AlertInfoDiscovery{
				Alerts: []*Alert{
					{
						Labels: labels.Labels{
							{Name: "alertname", Value: "UP_ALERT_1"},
						},
						State:    "fake",
						ActiveAt: nil,
						Value:    "1e+00",
					},
				},
				NextToken: "",
			},
			err:       "",
			errorType: "",
		},
		{
			name:       "bad maxResults",
			statusCode: 400,
			status:     "error",
			maxResults: intPtr(-1),
			output:     nil,
			err:        "maxAlerts need to be a valid number and larger than 0",
			errorType:  "bad_data",
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			req := requestFor(t, "GET", "https://localhost:8080/api/prom/api/v1/alertinfos", nil, "user1")
			urlValues := req.URL.Query()
			if tt.maxResults != nil {
				addQueryParams(urlValues, "maxResults", strconv.Itoa(*tt.maxResults))
			}
			if tt.nextToken != "" {
				addQueryParams(urlValues, "nextToken", tt.nextToken)
			}
			if len(tt.files) > 0 {
				addQueryParams(urlValues, "file[]", tt.files...)
			}

			req.URL.RawQuery = urlValues.Encode()
			w := httptest.NewRecorder()
			a.PrometheusAlertInfos(w, req)

			resp := w.Result()
			body, _ := io.ReadAll(resp.Body)

			// Check status code and status response
			responseJSON := util_api.Response{}
			err := json.Unmarshal(body, &responseJSON)
			require.NoError(t, err)
			require.Equal(t, tt.statusCode, resp.StatusCode)
			require.Equal(t, tt.status, responseJSON.Status)

			// Testing the running rules for user1 in the mock store
			expectedResponse, _ := json.Marshal(tt.output)

			// Zero out the evaluation timestamp of the alerts
			jsonStr, _ := json.Marshal(responseJSON.Data)
			ai := &AlertInfoDiscovery{}
			err = json.Unmarshal(jsonStr, &ai)
			require.NoError(t, err)
			alertsZeroFunc(ai)
			jsonStr, _ = json.Marshal(ai)

			require.Equal(t, string(expectedResponse), string(jsonStr))
		})
	}
}
