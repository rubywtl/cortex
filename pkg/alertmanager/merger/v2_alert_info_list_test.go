package merger

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/go-openapi/swag"

	v2_models "github.com/prometheus/alertmanager/api/v2/models"
	"github.com/stretchr/testify/require"
)

func TestV2AlertInfoList(t *testing.T) {

	// This test is to check the parsing round-trip is working as expected, the merging logic is
	// tested in TestMergeV2AlertGroups. The test data is based on captures from an actual Alertmanager.

	in := [][]byte{

		[]byte(`{"alerts":[{"annotations":{"annotation1":"a1-"},"endsAt":"2020-01-01T12:00:00.000Z","fingerprint":"1111111111111111","receivers":[{"name":"dummy"}],"startsAt":"2020-01-01T12:00:00.000Z","status":{"inhibitedBy":null,"mutedBy":null,"silencedBy":null,"state":null},"updatedAt":"2020-01-01T12:00:00.000Z","generatorURL":"something","labels":{"label1":"foo"}}],"nextToken":"2"}`),
		[]byte(`{"alerts":[{"annotations":{"annotation1":"a2-"},"endsAt":"2020-01-01T12:00:00.000Z","fingerprint":"2222222222222222","receivers":[{"name":"dummy"}],"startsAt":"2020-01-01T12:00:00.000Z","status":{"inhibitedBy":null,"mutedBy":null,"silencedBy":null,"state":null},"updatedAt":"2020-01-01T12:00:00.000Z","generatorURL":"something","labels":{"label1":"foo"}}],"nextToken":"2"}`),
		[]byte(`{"alerts":[{"annotations":{"annotation1":"a3-"},"endsAt":"2020-01-01T12:00:00.000Z","fingerprint":"3333333333333333","receivers":[{"name":"dummy"}],"startsAt":"2020-01-01T12:00:00.000Z","status":{"inhibitedBy":null,"mutedBy":null,"silencedBy":null,"state":null},"updatedAt":"2020-01-01T12:00:00.000Z","generatorURL":"something","labels":{"label1":"foo"}}],"nextToken":"2"}`),
	}

	expected := []byte(`{"alerts":[` +
		`{"annotations":{"annotation1":"a1-"},"endsAt":"2020-01-01T12:00:00.000Z","fingerprint":"1111111111111111","receivers":[{"name":"dummy"}],"startsAt":"2020-01-01T12:00:00.000Z","status":{"inhibitedBy":null,"mutedBy":null,"silencedBy":null,"state":null},"updatedAt":"2020-01-01T12:00:00.000Z","generatorURL":"something","labels":{"label1":"foo"}},` +
		`{"annotations":{"annotation1":"a2-"},"endsAt":"2020-01-01T12:00:00.000Z","fingerprint":"2222222222222222","receivers":[{"name":"dummy"}],"startsAt":"2020-01-01T12:00:00.000Z","status":{"inhibitedBy":null,"mutedBy":null,"silencedBy":null,"state":null},"updatedAt":"2020-01-01T12:00:00.000Z","generatorURL":"something","labels":{"label1":"foo"}},` +
		`{"annotations":{"annotation1":"a3-"},"endsAt":"2020-01-01T12:00:00.000Z","fingerprint":"3333333333333333","receivers":[{"name":"dummy"}],"startsAt":"2020-01-01T12:00:00.000Z","status":{"inhibitedBy":null,"mutedBy":null,"silencedBy":null,"state":null},"updatedAt":"2020-01-01T12:00:00.000Z","generatorURL":"something","labels":{"label1":"foo"}}` +
		`]}`)

	baseURL := "http://example.com"
	params := url.Values{}
	params.Add("maxResults", "0")
	u, _ := url.ParseRequestURI(baseURL)
	u.RawQuery = params.Encode()

	req := &http.Request{
		Method: "GET",
		URL:    u,
		Body:   http.NoBody,
	}
	out, err := V2AlertInfoList{}.MergeResponses(req, in)
	require.NoError(t, err)
	require.Equal(t, string(expected), string(out))
}

func v2alertinfos(alerts []*v2_models.GettableAlert, nextToken string) *v2_models.GettableAlertInfos {
	return &v2_models.GettableAlertInfos{
		Alerts:    alerts,
		NextToken: nextToken,
	}
}

func TestMergeV2AlertInfoList(t *testing.T) {
	var (
		alert1 = v2alert("1111111111111111", "a1-", "2020-01-01T12:00:00.000Z")
		alert2 = v2alert("2222222222222222", "a2-", "2020-01-01T12:00:00.000Z")
		alert3 = v2alert("3333333333333333", "a3-", "2020-01-01T12:00:00.000Z")
		alert4 = v2alert("4444444444444444", "a4-", "2020-01-01T12:00:00.000Z")
	)
	cases := []struct {
		name    string
		in      []v2_models.GettableAlertInfos
		err     error
		out     *v2_models.GettableAlertInfos
		maxItem int
	}{
		{
			name: "no groups, should return no groups",
			in:   []v2_models.GettableAlertInfos{},
			out: &v2_models.GettableAlertInfos{
				Alerts: []*v2_models.GettableAlert{},
			},
			maxItem: 0,
		},
		{
			name: "one response with one alert, should return one alert, no next token",
			in: []v2_models.GettableAlertInfos{
				*v2alertinfos(v2alerts(alert1), ""),
			},
			out: &v2_models.GettableAlertInfos{
				Alerts: v2alerts(alert1),
			},
			maxItem: 0,
		},
		{
			name: "two response with 4 alerts total should return dedupe 2 alert, should has next token",
			in: []v2_models.GettableAlertInfos{
				*v2alertinfos(v2alerts(alert1, alert2), "12345"),
				*v2alertinfos(v2alerts(alert1, alert2), "12345"),
			},
			out: &v2_models.GettableAlertInfos{
				Alerts:    v2alerts(alert1, alert2),
				NextToken: "12345",
			},
			maxItem: 5,
		},
		{
			name: "two response with 4 alerts total should return dedupe 2 groups, should not has next token",
			in: []v2_models.GettableAlertInfos{
				*v2alertinfos(v2alerts(alert1, alert2), ""),
				*v2alertinfos(v2alerts(alert1, alert2), ""),
			},
			out: &v2_models.GettableAlertInfos{
				Alerts: v2alerts(alert1, alert2),
			},
		},
		{
			name: "two response with 4 alerts, maxItem 3, should return 3 alerts",
			in: []v2_models.GettableAlertInfos{
				*v2alertinfos(v2alerts(alert1, alert2), ""),
				*v2alertinfos(v2alerts(alert3, alert4), ""),
			},
			out: &v2_models.GettableAlertInfos{
				Alerts:    v2alerts(alert1, alert2, alert3),
				NextToken: *alert3.Fingerprint,
			},
			maxItem: 3,
		},
		{
			name: "two response with 4 alerts, maxItem 4, should return 4 alerts, should return next token in client response",
			in: []v2_models.GettableAlertInfos{
				*v2alertinfos(v2alerts(alert1, alert2), "12345"),
				*v2alertinfos(v2alerts(alert3, alert4), "23455"),
			},
			out: &v2_models.GettableAlertInfos{
				Alerts:    v2alerts(alert1, alert2, alert3, alert4),
				NextToken: "23455",
			},
			maxItem: 4,
		},
		{
			name: "three response with 6 groups, maxItem 4, should return 4 groups",
			in: []v2_models.GettableAlertInfos{
				*v2alertinfos(v2alerts(alert1, alert2), "12345"),
				*v2alertinfos(v2alerts(alert3, alert4), "12345"),
				*v2alertinfos(v2alerts(alert1, alert4), "12345"),
			},
			out: &v2_models.GettableAlertInfos{
				Alerts:    v2alerts(alert1, alert2, alert3, alert4),
				NextToken: "12345",
			},
			maxItem: 4,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			baseURL := "http://example.com"
			params := url.Values{}
			params.Add("maxResults", fmt.Sprint(c.maxItem))
			u, _ := url.ParseRequestURI(baseURL)
			u.RawQuery = params.Encode()

			req := &http.Request{
				Method: "GET",
				URL:    u,
				Body:   http.NoBody,
			}
			alertInfosIDMap, alertIDToNextTokenMap := getAlertInfosMap(c.in)
			out, err := mergeV2AlertInfoList(req, alertInfosIDMap, alertIDToNextTokenMap)
			expectJSON, _ := swag.WriteJSON(c.out)
			outJSON, _ := swag.WriteJSON(out)
			require.Equal(t, c.err, err)
			require.Equal(t, string(expectJSON[:]), string(outJSON[:]))
		})
	}
}

func getAlertInfosMap(in []v2_models.GettableAlertInfos) (map[string]*v2_models.GettableAlert, map[string]string) {
	alertInfosIDMap := make(map[string]*v2_models.GettableAlert)
	alertIDToNextTokenMap := make(map[string]string)
	for _, alertInfos := range in {
		for _, alert := range alertInfos.Alerts {
			alertInfosIDMap[*alert.Fingerprint] = alert
			alertIDToNextTokenMap[*alert.Fingerprint] = alertInfos.NextToken
		}
	}
	return alertInfosIDMap, alertIDToNextTokenMap
}
