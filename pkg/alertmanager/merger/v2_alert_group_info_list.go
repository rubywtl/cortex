package merger

import (
	"errors"
	"net/http"
	"sort"
	"strconv"

	"github.com/go-openapi/swag"
	v2 "github.com/prometheus/alertmanager/api/v2"
	v2_models "github.com/prometheus/alertmanager/api/v2/models"
)

// V2AlertGroupInfoList implements the Merger interface for GET /v2/alertgroups. It returns
// the union of alert group infos over all the responses.
type V2AlertGroupInfoList struct{}

func (V2AlertGroupInfoList) MergeResponses(req *http.Request, in [][]byte) ([]byte, error) {
	groupInfosIDMap := make(map[string]*v2_models.AlertGroupInfo)

	// Keep track of each response's next token
	groupsIDToNextTokenMap := make(map[string]string)
	for _, body := range in {
		parsed := v2_models.AlertGroupInfoList{}
		if err := swag.ReadJSON(body, &parsed); err != nil {
			return nil, err
		}
		for _, groupInfo := range parsed.AlertGroupInfoList {
			if groupInfo.ID == nil {
				return nil, errors.New("unexpected nil id")
			}
			groupInfosIDMap[*groupInfo.ID] = groupInfo
			groupsIDToNextTokenMap[*groupInfo.ID] = parsed.NextToken
		}
	}

	merged, err := mergeV2AlertGroupInfoList(req, groupInfosIDMap, groupsIDToNextTokenMap)
	if err != nil {
		return nil, err
	}

	return swag.WriteJSON(merged)
}

func mergeV2AlertGroupInfoList(req *http.Request, groupInfosIDMap map[string]*v2_models.AlertGroupInfo, groupsIDToNextTokenMap map[string]string) (*v2_models.AlertGroupInfoList, error) {
	maxItem, err := strconv.ParseInt(req.URL.Query().Get("maxResults"), 10, 64)
	if err != nil {
		maxItem = -1
	}

	groupInfos := make([]*v2_models.AlertGroupInfo, 0, len(groupInfosIDMap))
	for _, groupInfo := range groupInfosIDMap {
		groupInfos = append(groupInfos, &v2_models.AlertGroupInfo{
			ID:       groupInfo.ID,
			Labels:   groupInfo.Labels,
			Receiver: groupInfo.Receiver,
		})
	}

	// Mimic Alertmanager which returns groups ordered by group id.
	sort.Sort(byGroupInfos(groupInfos))

	if maxItem > 0 {
		result, nextToken := v2.AlertGroupInfoListTruncate(groupInfos, &maxItem)

		if len(result) > 0 {
			// If nextToken is not in the truncate result, we need to check if it is in the api response
			nextTokenFromAPI, ok := groupsIDToNextTokenMap[*result[len(result)-1].ID]
			if len(nextToken) == 0 && ok {
				nextToken = nextTokenFromAPI
			}
		}

		return &v2_models.AlertGroupInfoList{
			AlertGroupInfoList: result,
			NextToken:          nextToken,
		}, nil
	}

	return &v2_models.AlertGroupInfoList{
		AlertGroupInfoList: groupInfos,
		NextToken:          "",
	}, nil
}

// byGroupInfos implements the ordering of Alertmanager dispatch.AlertGroupInfo on the OpenAPI type.
type byGroupInfos []*v2_models.AlertGroupInfo

func (ag byGroupInfos) Swap(i, j int) { ag[i], ag[j] = ag[j], ag[i] }
func (ag byGroupInfos) Less(i, j int) bool {
	return *ag[i].ID < *ag[j].ID
}
func (ag byGroupInfos) Len() int { return len(ag) }
