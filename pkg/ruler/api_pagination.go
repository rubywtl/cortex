package ruler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"

	util_api "github.com/cortexproject/cortex/pkg/util/api"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/tenant"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

type listRulesInfosPaginationRequest struct {
	MaxAlerts     int32
	MaxRuleGroups int32
	NextToken     string
}

type listAlertsPaginationRequest struct {
	MaxResults int32
	NextToken  string
}

func (a *API) PrometheusRuleInfos(w http.ResponseWriter, req *http.Request) {
	logger := util_log.WithContext(req.Context(), a.logger)
	userID, err := tenant.TenantID(req.Context())
	if err != nil || userID == "" {
		level.Error(logger).Log("msg", "error extracting org id from context", "err", err)
		util_api.RespondError(logger, w, v1.ErrBadData, "no valid org id found", http.StatusBadRequest)
		return
	}

	if err := req.ParseForm(); err != nil {
		level.Error(logger).Log("msg", "error parsing form/query params", "err", err)
		util_api.RespondError(logger, w, v1.ErrBadData, "error parsing form/query params", http.StatusBadRequest)
		return
	}

	typ := strings.ToLower(req.URL.Query().Get("type"))
	if typ != "" && typ != alertingRuleFilter && typ != recordingRuleFilter {
		util_api.RespondError(logger, w, v1.ErrBadData, fmt.Sprintf("unsupported rule type %q", typ), http.StatusBadRequest)
		return
	}

	state := strings.ToLower(req.URL.Query().Get("state"))
	if state != "" && state != firingStateFilter && state != pendingStateFilter && state != inactiveStateFilter {
		util_api.RespondError(logger, w, v1.ErrBadData, fmt.Sprintf("unsupported state value %q", state), http.StatusBadRequest)
		return
	}

	health := strings.ToLower(req.URL.Query().Get("health"))
	if health != "" && health != unknownHealthFilter && health != okHealthFilter && health != errHealthFilter {
		util_api.RespondError(logger, w, v1.ErrBadData, fmt.Sprintf("unsupported health value %q", health), http.StatusBadRequest)
		return
	}

	_, err = parseMatchersParam(req.Form["match[]"])
	if err != nil {
		level.Error(logger).Log("msg", "error parsing match query params", "err", err)
		util_api.RespondError(logger, w, v1.ErrBadData, fmt.Sprintf("error parsing match params %s", err), http.StatusBadRequest)
		return
	}

	paginationRequest, err := parseListRuleInfosPaginationRequest(req, logger)
	if err != nil {
		util_api.RespondError(logger, w, v1.ErrBadData, err.Error(), http.StatusBadRequest)
		return
	}

	ruleInfosRequest := RuleInfosRequest{
		RuleNames:      req.Form["rule_name[]"],
		RuleGroupNames: req.Form["rule_group[]"],
		Files:          req.Form["file[]"],
		Type:           typ,
		State:          state,
		Health:         health,
		MaxAlerts:      paginationRequest.MaxAlerts,
		MaxRuleGroups:  paginationRequest.MaxRuleGroups,
		NextToken:      paginationRequest.NextToken,
		Matches:        req.Form["match[]"],
	}

	w.Header().Set("Content-Type", "application/json")
	rgs, err := a.ruler.GetRuleInfos(req.Context(), ruleInfosRequest)

	if err != nil {
		util_api.RespondError(logger, w, v1.ErrServer, err.Error(), http.StatusInternalServerError)
		return
	}

	mergedGroups := mergeListRuleInfosResponse(rgs, paginationRequest.MaxRuleGroups)

	groups := make([]*RuleGroup, 0, len(rgs))

	for _, g := range mergedGroups.Groups {
		grp := RuleGroup{
			Name:           g.Group.Name,
			File:           g.Group.Namespace,
			Rules:          make([]rule, len(g.ActiveRules)),
			Interval:       g.Group.Interval.Seconds(),
			LastEvaluation: g.GetEvaluationTimestamp(),
			EvaluationTime: g.GetEvaluationDuration().Seconds(),
			Limit:          g.Group.Limit,
		}

		for i, rl := range g.ActiveRules {
			if g.ActiveRules[i].Rule.Alert != "" {
				alerts := make([]*Alert, 0, len(rl.AlertInfo.Alerts))
				for _, a := range rl.AlertInfo.Alerts {
					alert := &Alert{
						Labels:      cortexpb.FromLabelAdaptersToLabels(a.Labels),
						Annotations: cortexpb.FromLabelAdaptersToLabels(a.Annotations),
						State:       a.GetState(),
						ActiveAt:    &a.ActiveAt,
						Value:       strconv.FormatFloat(a.Value, 'e', -1, 64),
					}
					if !a.KeepFiringSince.IsZero() {
						alert.KeepFiringSince = &a.KeepFiringSince
					}
					alerts = append(alerts, alert)
				}
				grp.Rules[i] = alertingRuleInfo{
					State:          rl.GetState(),
					Name:           rl.Rule.GetAlert(),
					Query:          rl.Rule.GetExpr(),
					Duration:       rl.Rule.For.Seconds(),
					Labels:         cortexpb.FromLabelAdaptersToLabels(rl.Rule.Labels),
					Annotations:    cortexpb.FromLabelAdaptersToLabels(rl.Rule.Annotations),
					AlertInfo:      AlertInfo{Alerts: alerts, HasMore: rl.AlertInfo.HasMore},
					Health:         rl.GetHealth(),
					LastError:      rl.GetLastError(),
					LastEvaluation: rl.GetEvaluationTimestamp(),
					EvaluationTime: rl.GetEvaluationDuration().Seconds(),
					Type:           v1.RuleTypeAlerting,
					KeepFiringFor:  rl.Rule.KeepFiringFor.Seconds(),
				}
			} else {
				grp.Rules[i] = recordingRule{
					Name:           rl.Rule.GetRecord(),
					Query:          rl.Rule.GetExpr(),
					Labels:         cortexpb.FromLabelAdaptersToLabels(rl.Rule.Labels),
					Health:         rl.GetHealth(),
					LastError:      rl.GetLastError(),
					LastEvaluation: rl.GetEvaluationTimestamp(),
					EvaluationTime: rl.GetEvaluationDuration().Seconds(),
					Type:           v1.RuleTypeRecording,
				}
			}
		}
		groups = append(groups, &grp)
	}

	b, err := json.Marshal(&util_api.Response{
		Status: "success",
		Data:   &RuleInfoDiscovery{RuleGroups: groups, NextToken: mergedGroups.NextToken},
	})
	if err != nil {
		level.Error(logger).Log("msg", "error marshaling json response", "err", err)
		util_api.RespondError(logger, w, v1.ErrServer, "unable to marshal the requested data", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		level.Error(logger).Log("msg", "error writing response", "bytesWritten", n, "err", err)
	}
}

func (a *API) PrometheusAlertInfos(w http.ResponseWriter, req *http.Request) {
	logger := util_log.WithContext(req.Context(), a.logger)
	userID, err := tenant.TenantID(req.Context())
	if err != nil || userID == "" {
		level.Error(logger).Log("msg", "error extracting org id from context", "err", err)
		util_api.RespondError(logger, w, v1.ErrBadData, "no valid org id found", http.StatusBadRequest)
		return
	}

	if err := req.ParseForm(); err != nil {
		level.Error(logger).Log("msg", "error parsing form/query params", "err", err)
		util_api.RespondError(logger, w, v1.ErrBadData, "error parsing form/query params", http.StatusBadRequest)
		return
	}

	_, err = parseMatchersParam(req.Form["match[]"])
	if err != nil {
		level.Error(logger).Log("msg", "error parsing match query params", "err", err)
		util_api.RespondError(logger, w, v1.ErrBadData, fmt.Sprintf("error parsing match params %s", err), http.StatusBadRequest)
		return
	}

	paginationRequest, err := parseListAlertInfosPaginationRequest(req, logger)
	if err != nil {
		util_api.RespondError(logger, w, v1.ErrBadData, err.Error(), http.StatusBadRequest)
		return
	}

	alertInfosRequest := AlertInfosRequest{
		RuleNames:      req.Form["rule_name[]"],
		RuleGroupNames: req.Form["rule_group[]"],
		Files:          req.Form["file[]"],
		Type:           alertingRuleFilter,
		MaxResults:     paginationRequest.MaxResults,
		NextToken:      paginationRequest.NextToken,
		Matches:        req.Form["match[]"],
	}

	w.Header().Set("Content-Type", "application/json")

	alertInfos, err := a.ruler.GetAlertInfos(req.Context(), alertInfosRequest)

	if err != nil {
		util_api.RespondError(logger, w, v1.ErrServer, err.Error(), http.StatusInternalServerError)
		return
	}

	mergedAlerts := mergeListAlertInfosResponse(alertInfos, paginationRequest.MaxResults)

	alerts := []*Alert{}

	for _, a := range mergedAlerts.Alerts {
		alert := &Alert{
			Labels:      cortexpb.FromLabelAdaptersToLabels(a.Labels),
			Annotations: cortexpb.FromLabelAdaptersToLabels(a.Annotations),
			State:       a.GetState(),
			ActiveAt:    &a.ActiveAt,
			Value:       strconv.FormatFloat(a.Value, 'e', -1, 64),
		}
		if !a.KeepFiringSince.IsZero() {
			alert.KeepFiringSince = &a.KeepFiringSince
		}
		alerts = append(alerts, alert)
	}

	b, err := json.Marshal(&util_api.Response{
		Status: "success",
		Data:   &AlertInfoDiscovery{Alerts: alerts, NextToken: mergedAlerts.NextToken},
	})
	if err != nil {
		level.Error(logger).Log("msg", "error marshaling json response", "err", err)
		util_api.RespondError(logger, w, v1.ErrServer, "unable to marshal the requested data", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		level.Error(logger).Log("msg", "error writing response", "bytesWritten", n, "err", err)
	}
}

func mergeListRuleInfosResponse(ruleGroupInfos []*RuleInfosResponse, maxRuleGroups int32) *RuleInfosResponse {
	var groupInfos []*GroupInfoStateDesc

	// Keep track of each response's next token
	ruleGroupToNextTokenMap := make(map[string]string)

	for _, ruleGroupInfo := range ruleGroupInfos {
		for _, ruleGroup := range ruleGroupInfo.Groups {
			ruleGroupID := getRuleGroupNextToken(ruleGroup.Group.Namespace, ruleGroup.Group.Name)
			groupInfos = append(groupInfos, ruleGroup)
			ruleGroupToNextTokenMap[ruleGroupID] = ruleGroupInfo.NextToken
		}
	}

	// sort before we truncate
	sort.Sort(GroupInfoStateDescs(groupInfos))

	if maxRuleGroups > 0 {
		result, nextToken := TruncateGroupInfos(groupInfos, int(maxRuleGroups))

		if len(result) > 0 {
			// If nextToken is not in the truncate result, we need to check if it is in the api response
			nextTokenFromAPI, ok := ruleGroupToNextTokenMap[getRuleGroupNextToken(result[len(result)-1].Group.Namespace, result[len(result)-1].Group.Name)]
			if len(nextToken) == 0 && ok {
				nextToken = nextTokenFromAPI
			}
		}

		return &RuleInfosResponse{
			Groups:    result,
			NextToken: nextToken,
		}
	}

	return &RuleInfosResponse{
		Groups:    groupInfos,
		NextToken: "",
	}
}

func mergeListAlertInfosResponse(alertInfos []*AlertInfosResponse, maxResults int32) *AlertInfosResponse {
	var alerts []*PaginatedAlertStateDesc

	// Keep track of each response's next token
	alertToNextTokenMap := make(map[string]string)

	for _, alertInfo := range alertInfos {
		for _, alert := range alertInfo.Alerts {
			alertfp := getAlertNextToken(alert.Rule.Namespace, alert.Rule.Group, alert.Rule.Name, alert.Rule.Order, alert.Labels)
			alerts = append(alerts, alert)
			alertToNextTokenMap[alertfp] = alertInfo.NextToken
		}
	}

	// sort before we truncate
	sort.Sort(PaginatedAlertStateDescs(alerts))

	if maxResults > 0 {
		result, nextToken := TruncateAlertInfos(alerts, int(maxResults))

		if len(result) > 0 {
			// If nextToken is not in the truncate result, we need to check if it is in the api response
			lastAlert := result[len(result)-1]
			nextTokenFromAPI, ok := alertToNextTokenMap[getAlertNextToken(lastAlert.Rule.Namespace, lastAlert.Rule.Group, lastAlert.Rule.Name, lastAlert.Rule.Order, lastAlert.Labels)]
			if len(nextToken) == 0 && ok {
				nextToken = nextTokenFromAPI
			}
		}

		return &AlertInfosResponse{
			Alerts:    result,
			NextToken: nextToken,
		}
	}

	return &AlertInfosResponse{
		Alerts:    alerts,
		NextToken: "",
	}
}

// parseListRuleInfosPaginationRequest parses the incoming request to parse out the parameters related to pagination request
func parseListRuleInfosPaginationRequest(req *http.Request, logger log.Logger) (listRulesInfosPaginationRequest, error) {
	var (
		returnMaxAlert      = int32(-1)
		returnMaxRuleGroups = int32(-1)
		returnNextToken     = ""
	)

	if req.URL.Query().Get("maxAlerts") != "" {
		maxAlert, err := strconv.ParseInt(req.URL.Query().Get("maxAlerts"), 10, 32)
		if err != nil || maxAlert < 0 {
			level.Error(logger).Log("msg", "error parsing maxAlerts params", "err", err)
			return listRulesInfosPaginationRequest{
				MaxRuleGroups: -1,
				MaxAlerts:     -1,
				NextToken:     "",
			}, errors.New("maxAlerts need to be a valid number and larger than 0")
		}
		returnMaxAlert = int32(maxAlert)
	}

	if req.URL.Query().Get("maxRuleGroups") != "" {
		maxRuleGroups, err := strconv.ParseInt(req.URL.Query().Get("maxRuleGroups"), 10, 32)
		if err != nil || maxRuleGroups < 0 {
			level.Error(logger).Log("msg", "error parsing maxRuleGroups params", "err", err)
			return listRulesInfosPaginationRequest{
				MaxRuleGroups: -1,
				MaxAlerts:     -1,
				NextToken:     "",
			}, errors.New("maxRuleGroups need to be a valid number and larger than 0")
		}
		returnMaxRuleGroups = int32(maxRuleGroups)
	}

	if req.URL.Query().Get("nextToken") != "" {
		returnNextToken = req.URL.Query().Get("nextToken")
	}

	return listRulesInfosPaginationRequest{
		MaxRuleGroups: returnMaxRuleGroups,
		MaxAlerts:     returnMaxAlert,
		NextToken:     returnNextToken,
	}, nil
}

// parseListAlertInfosPaginationRequest parses the incoming request to parse out the parameters related to pagination request
func parseListAlertInfosPaginationRequest(req *http.Request, logger log.Logger) (listAlertsPaginationRequest, error) {
	var (
		returnMaxResult = int32(-1)
		returnNextToken = ""
	)

	if req.URL.Query().Get("maxResults") != "" {
		maxResult, err := strconv.ParseInt(req.URL.Query().Get("maxResults"), 10, 32)
		if err != nil || maxResult < 0 {
			level.Error(logger).Log("msg", "error parsing maxResults params", "err", err)
			return listAlertsPaginationRequest{
				MaxResults: -1,
				NextToken:  "",
			}, errors.New("maxResults need to be a valid number and larger than 0")
		}
		returnMaxResult = int32(maxResult)
	}

	if req.URL.Query().Get("nextToken") != "" {
		returnNextToken = req.URL.Query().Get("nextToken")
	}

	return listAlertsPaginationRequest{
		MaxResults: returnMaxResult,
		NextToken:  returnNextToken,
	}, nil
}
