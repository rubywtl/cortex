package distributedqueryutil

import (
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
	"github.com/weaveworks/common/httpgrpc"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func ParseURL(request *httpgrpc.HTTPRequest) (string, query.Options, logicalplan.PlanOptions, error) {
	// get raw query from URL,
	urlstr := request.GetUrl()
	urlquery, err := url.Parse(urlstr)
	if err != nil {
		return "", query.Options{}, logicalplan.PlanOptions{}, err
	}
	q := urlquery.RawQuery

	// get all params from query
	var querystr, logicalPlan, start, end, steps string
	parsedQ := strings.Split(q, "&")
	for _, field := range parsedQ {
		if strings.Contains(field, "logicalPlan") {
			logicalPlan = strings.Split(field, "=")[1]
		} else if strings.Contains(field, "query") {
			querystr = strings.Split(field, "=")[1]
		} else if strings.Contains(field, "start") {
			start = strings.Split(field, "=")[1]
		} else if strings.Contains(field, "end") {
			end = strings.Split(field, "=")[1]
		} else if strings.Contains(field, "step") {
			steps = strings.Split(field, "=")[1]
		}
	}
	startInt, _ := strconv.ParseInt(start, 10, 64)
	endInt, _ := strconv.ParseInt(end, 10, 64)
	stepsInt, _ := strconv.ParseInt(steps, 10, 64)
	print(querystr) // debug

	qOpts := query.Options{
		Start: time.Unix(startInt, 0),
		End:   time.Unix(endInt, 0),
		Step:  time.Duration(stepsInt),
	}

	planOpts := logicalplan.PlanOptions{
		DisableDuplicateLabelCheck: false,
	}

	return logicalPlan, qOpts, planOpts, nil
}

// Insert new logical plan to url
func InsertNewLogicalPlanToURL(oldURLquery string, serializedLogicalPlan string) (string, error) {
	// look for logicalplan field in the url query (&key=value&key=value)
	params := strings.Split(oldURLquery, "&")

	for i, param := range params {
		if strings.HasPrefix(param, "logicalplan=") {
			params[i] = "logicalplan=" + url.QueryEscape(serializedLogicalPlan)
			break
		}
	}
	newQuery := strings.Join(params, "&")
	return newQuery, nil
}

// Insert new query to url
