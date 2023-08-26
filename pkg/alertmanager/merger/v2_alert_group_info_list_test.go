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

func TestV2AlertGroupInfoList(t *testing.T) {

	// This test is to check the parsing round-trip is working as expected, the merging logic is
	// tested in TestMergeV2AlertGroups. The test data is based on captures from an actual Alertmanager.

	in := [][]byte{

		[]byte(`{"alertGroupInfoList":[{"id":"abcde","labels":{"annotation1":"a1"},"receiver":{"name":"aaa"}},{"id":"yuiop","labels":{"annotation1":"d1"},"receiver":{"name":"ddd"}}],"nextToken":"12345"}`),
		[]byte(`{"alertGroupInfoList":[{"id":"qwert","labels":{"annotation1":"c1"},"receiver":{"name":"ccc"}},{"id":"yuiop","labels":{"annotation1":"d1"},"receiver":{"name":"ddd"}}],"nextToken":"12345"}`),
		[]byte(`{"alertGroupInfoList":[{"id":"abcde","labels":{"annotation1":"a1"},"receiver":{"name":"aaa"}},{"id":"yuiop","labels":{"annotation1":"d1"},"receiver":{"name":"ddd"}}],"nextToken":"12345"}`),
	}

	expected := []byte(`{"alertGroupInfoList":[` +
		`{"id":"abcde","labels":{"annotation1":"a1"},"receiver":{"name":"aaa"}},` +
		`{"id":"qwert","labels":{"annotation1":"c1"},"receiver":{"name":"ccc"}},` +
		`{"id":"yuiop","labels":{"annotation1":"d1"},"receiver":{"name":"ddd"}}` +
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
	out, err := V2AlertGroupInfoList{}.MergeResponses(req, in)
	require.NoError(t, err)
	require.Equal(t, string(expected), string(out))
}

func v2groupinfos(groups []*v2_models.AlertGroupInfo, nextToken string) *v2_models.AlertGroupInfoList {
	return &v2_models.AlertGroupInfoList{
		AlertGroupInfoList: groups,
		NextToken:          nextToken,
	}
}

func v2groupinfo(receiver string, annotation string, id string) *v2_models.AlertGroupInfo {
	return &v2_models.AlertGroupInfo{
		ID: &id,
		Labels: v2_models.LabelSet{
			"annotation1": annotation,
		},
		Receiver: &v2_models.Receiver{Name: &receiver},
	}
}

func TestMergeV2AlertGroupInfoList(t *testing.T) {
	var (
		group1 = v2groupinfo("aaa", "a1", "abcde")
		group2 = v2groupinfo("bbb", "b1", "ghjkl")
		group3 = v2groupinfo("ccc", "c1", "qwert")
		group4 = v2groupinfo("ddd", "d1", "yuiop")
	)
	cases := []struct {
		name    string
		in      []v2_models.AlertGroupInfoList
		err     error
		out     *v2_models.AlertGroupInfoList
		maxItem int
	}{
		{
			name: "no groups, should return no groups",
			in:   []v2_models.AlertGroupInfoList{},
			out: &v2_models.AlertGroupInfoList{
				AlertGroupInfoList: []*v2_models.AlertGroupInfo{},
			},
			maxItem: 0,
		},
		{
			name: "one response with one group, should return one group, no next token",
			in: []v2_models.AlertGroupInfoList{
				*v2groupinfos([]*v2_models.AlertGroupInfo{group1}, ""),
			},
			out: &v2_models.AlertGroupInfoList{
				AlertGroupInfoList: []*v2_models.AlertGroupInfo{group1},
			},
			maxItem: 0,
		},
		{
			name: "two response with 4 groups total should return dedupe 2 groups, should has next token",
			in: []v2_models.AlertGroupInfoList{
				*v2groupinfos([]*v2_models.AlertGroupInfo{group1, group2}, "12345"),
				*v2groupinfos([]*v2_models.AlertGroupInfo{group1, group2}, "12345"),
			},
			out: &v2_models.AlertGroupInfoList{
				AlertGroupInfoList: []*v2_models.AlertGroupInfo{group1, group2},
				NextToken:          "12345",
			},
			maxItem: 5,
		},
		{
			name: "two response with 4 groups total should return dedupe 2 groups, should not has next token",
			in: []v2_models.AlertGroupInfoList{
				*v2groupinfos([]*v2_models.AlertGroupInfo{group1, group2}, ""),
				*v2groupinfos([]*v2_models.AlertGroupInfo{group1, group2}, ""),
			},
			out: &v2_models.AlertGroupInfoList{
				AlertGroupInfoList: []*v2_models.AlertGroupInfo{group1, group2},
			},
		},
		{
			name: "two response with 4 groups, maxItem 3, should return 3 groups",
			in: []v2_models.AlertGroupInfoList{
				*v2groupinfos([]*v2_models.AlertGroupInfo{group1, group2}, ""),
				*v2groupinfos([]*v2_models.AlertGroupInfo{group3, group4}, ""),
			},
			out: &v2_models.AlertGroupInfoList{
				AlertGroupInfoList: []*v2_models.AlertGroupInfo{group1, group2, group3},
				NextToken:          "qwert",
			},
			maxItem: 3,
		},
		{
			name: "two response with 4 groups, maxItem 3, should return 3 groups, has next token",
			in: []v2_models.AlertGroupInfoList{
				*v2groupinfos([]*v2_models.AlertGroupInfo{group1, group2}, "12345"),
				*v2groupinfos([]*v2_models.AlertGroupInfo{group3, group4}, "12345"),
			},
			out: &v2_models.AlertGroupInfoList{
				AlertGroupInfoList: []*v2_models.AlertGroupInfo{group1, group2, group3},
				NextToken:          "qwert",
			},
			maxItem: 3,
		},
		{
			name: "two response with 4 groups, maxItem 4, should return 4 groups, should return next token in client response",
			in: []v2_models.AlertGroupInfoList{
				*v2groupinfos([]*v2_models.AlertGroupInfo{group1, group2}, "12345"),
				*v2groupinfos([]*v2_models.AlertGroupInfo{group3, group4}, "12345"),
			},
			out: &v2_models.AlertGroupInfoList{
				AlertGroupInfoList: []*v2_models.AlertGroupInfo{group1, group2, group3, group4},
				NextToken:          "12345",
			},
			maxItem: 4,
		},
		{
			name: "three response with 6 groups, maxItem 4, should return 4 groups",
			in: []v2_models.AlertGroupInfoList{
				*v2groupinfos([]*v2_models.AlertGroupInfo{group1, group2}, "12345"),
				*v2groupinfos([]*v2_models.AlertGroupInfo{group3, group4}, "12345"),
				*v2groupinfos([]*v2_models.AlertGroupInfo{group1, group4}, "12345"),
			},
			out: &v2_models.AlertGroupInfoList{
				AlertGroupInfoList: []*v2_models.AlertGroupInfo{group1, group2, group3, group4},
				NextToken:          "12345",
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
			groupInfosIDMap, groupsIDToNextTokenMap := getAlertGroupInfosMap(c.in)
			out, err := mergeV2AlertGroupInfoList(req, groupInfosIDMap, groupsIDToNextTokenMap)
			expectJSON, _ := swag.WriteJSON(c.out)
			outJSON, _ := swag.WriteJSON(out)
			require.Equal(t, c.err, err)
			require.Equal(t, string(expectJSON[:]), string(outJSON[:]))
		})
	}
}

func getAlertGroupInfosMap(in []v2_models.AlertGroupInfoList) (map[string]*v2_models.AlertGroupInfo, map[string]string) {
	groupInfosIDMap := make(map[string]*v2_models.AlertGroupInfo)
	groupsIDToNextTokenMap := make(map[string]string)
	for _, groupInfos := range in {
		for _, groupInfo := range groupInfos.AlertGroupInfoList {
			groupInfosIDMap[*groupInfo.ID] = groupInfo
			groupsIDToNextTokenMap[*groupInfo.ID] = groupInfos.NextToken
		}
	}
	return groupInfosIDMap, groupsIDToNextTokenMap
}
