package merger

import (
	"errors"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/go-openapi/swag"
	v2 "github.com/prometheus/alertmanager/api/v2"
	v2_models "github.com/prometheus/alertmanager/api/v2/models"
)

// V2AlertInfoList implements the Merger interface for GET /v2/alertinfos. It returns
// the union of alert group infos over all the responses.
type V2AlertInfoList struct{}

func (V2AlertInfoList) MergeResponses(req *http.Request, in [][]byte) ([]byte, error) {
	alertInfosIDMap := make(map[string]*v2_models.GettableAlert)

	// Keep track of each response's next token
	alertsIDToNextTokenMap := make(map[string]string)
	for _, body := range in {
		parsed := v2_models.GettableAlertInfos{}
		if err := swag.ReadJSON(body, &parsed); err != nil {
			return nil, err
		}
		for _, alert := range parsed.Alerts {
			if alert.Fingerprint == nil {
				return nil, errors.New("unexpected nil alert fingerprint")
			}
			if alert.UpdatedAt == nil {
				return nil, errors.New("unexpected nil updatedAt")
			}
			key := *alert.Fingerprint
			current, ok := alertInfosIDMap[key]
			if ok && time.Time(*alert.UpdatedAt).After(time.Time(*current.UpdatedAt)) {
				alertInfosIDMap[key] = alert
				alertsIDToNextTokenMap[key] = parsed.NextToken
			} else {
				alertInfosIDMap[key] = alert
				alertsIDToNextTokenMap[key] = parsed.NextToken
			}
		}
	}

	merged, err := mergeV2AlertInfoList(req, alertInfosIDMap, alertsIDToNextTokenMap)
	if err != nil {
		return nil, err
	}

	return swag.WriteJSON(merged)
}

func mergeV2AlertInfoList(req *http.Request, alertInfosIDMap map[string]*v2_models.GettableAlert, alertsIDToNextTokenMap map[string]string) (*v2_models.GettableAlertInfos, error) {
	maxItem, err := strconv.ParseInt(req.URL.Query().Get("maxResults"), 10, 64)
	if err != nil {
		maxItem = -1
	}

	alertInfos := make([]*v2_models.GettableAlert, len(alertInfosIDMap))
	index := 0
	for _, alertInfo := range alertInfosIDMap {
		alertInfos[index] = alertInfo
		index++
	}

	// Mimic Alertmanager which returns alerts ordered by fingerprint (as string).
	sort.Slice(alertInfos, func(i, j int) bool {
		return *alertInfos[i].Fingerprint < *alertInfos[j].Fingerprint
	})

	if maxItem > 0 {
		result, nextToken := v2.AlertInfosTruncate(alertInfos, &maxItem, nil)

		if len(result) > 0 {
			// If nextToken is not in the truncate result, we need to check if it is in the api response
			nextTokenFromAPI, ok := alertsIDToNextTokenMap[*result[len(result)-1].Fingerprint]
			if len(nextToken) == 0 && ok {
				nextToken = nextTokenFromAPI
			}
		}

		return &v2_models.GettableAlertInfos{
			Alerts:    result,
			NextToken: nextToken,
		}, nil
	}

	return &v2_models.GettableAlertInfos{
		Alerts:    alertInfos,
		NextToken: "",
	}, nil
}
