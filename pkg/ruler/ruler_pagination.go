package ruler

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util/concurrency"
)

// GetRuleInfos retrieves the running rules from this ruler and all running rulers in the ring if
// sharding is enabled
func (r *Ruler) GetRuleInfos(ctx context.Context, ruleInfosRequest RuleInfosRequest) ([]*RuleInfosResponse, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, fmt.Errorf("no user id found in context")
	}

	if r.cfg.EnableSharding {
		return r.getShardedRuleInfos(ctx, userID, ruleInfosRequest)
	}

	response, err := r.getLocalRuleInfos(userID, ruleInfosRequest)
	return []*RuleInfosResponse{&response}, err
}

func (r *Ruler) getLocalRuleInfos(userID string, ruleInfosRequest RuleInfosRequest) (RuleInfosResponse, error) {

	groupDescs, err := r.getLocalRulesCopy(userID, RulesRequest{
		RuleNames:      ruleInfosRequest.GetRuleNames(),
		RuleGroupNames: ruleInfosRequest.GetRuleGroupNames(),
		Files:          ruleInfosRequest.GetFiles(),
		Type:           ruleInfosRequest.GetType(),
		State:          ruleInfosRequest.GetState(),
		Health:         ruleInfosRequest.GetHealth(),
		Matchers:       ruleInfosRequest.GetMatches(),
	}, false)

	if err != nil {
		return RuleInfosResponse{
			Groups:    nil,
			NextToken: "",
		}, err
	}

	sort.Sort(GroupStateDescs(groupDescs))

	returnGroupDescs := make([]*GroupInfoStateDesc, 0, len(groupDescs))
	for _, group := range groupDescs {

		// Skip the rule group if the next token is set and hasn't arrived the nextToken item yet.
		groupID := getRuleGroupNextToken(group.Group.Namespace, group.Group.Name)
		if len(ruleInfosRequest.NextToken) > 0 && ruleInfosRequest.NextToken >= groupID {
			continue
		}

		groupDesc := &GroupInfoStateDesc{
			Group:               group.Group,
			EvaluationTimestamp: group.EvaluationTimestamp,
			EvaluationDuration:  group.EvaluationDuration,
		}
		for _, rule := range group.ActiveRules {
			alertsDesc := rule.Alerts
			hasMore := false
			if ruleInfosRequest.MaxAlerts >= 0 && len(rule.Alerts) > int(ruleInfosRequest.MaxAlerts) {
				alertsDesc = alertsDesc[:int(ruleInfosRequest.MaxAlerts)]
				hasMore = true
			}
			ruleDesc := &RuleInfoStateDesc{
				Rule:      rule.Rule,
				State:     rule.State,
				Health:    rule.Health,
				LastError: rule.LastError,
				AlertInfo: &AlertInfosStateDesc{
					Alerts:  alertsDesc,
					HasMore: hasMore,
				},
				EvaluationTimestamp: rule.EvaluationTimestamp,
				EvaluationDuration:  rule.EvaluationDuration,
			}
			groupDesc.ActiveRules = append(groupDesc.ActiveRules, ruleDesc)
		}
		if len(groupDesc.ActiveRules) > 0 {
			returnGroupDescs = append(returnGroupDescs, groupDesc)
		}
	}

	returnGroupDescs, nextToken := TruncateGroupInfos(returnGroupDescs, int(ruleInfosRequest.MaxRuleGroups))

	return RuleInfosResponse{
		Groups:    returnGroupDescs,
		NextToken: nextToken,
	}, nil
}

func (r *Ruler) getShardedRuleInfos(ctx context.Context, userID string, ruleInfosRequest RuleInfosRequest) ([]*RuleInfosResponse, error) {

	var (
		mergedMx sync.Mutex
		merged   []*RuleInfosResponse
	)

	ctx, err := user.InjectIntoGRPCRequest(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to inject user ID into grpc request, %v", err)
	}

	jobs, failedZones, _, _, err := r.getRingJobs(ctx, userID)
	if err != nil {
		return nil, err
	}

	if len(failedZones) > 0 {
		// for now RulesInfos api don't support HA
		return nil, ring.ErrTooManyUnhealthyInstances
	}

	err = concurrency.ForEach(ctx, jobs, len(jobs), func(ctx context.Context, job interface{}) error {
		addr := job.(string)

		rulerClient, err := r.clientsPool.GetClientFor(addr)
		if err != nil {
			return errors.Wrapf(err, "unable to get client for ruler %s", addr)
		}

		newGrps, err := rulerClient.RuleInfos(ctx, &ruleInfosRequest)
		if err != nil {
			return errors.Wrapf(err, "unable to retrieve rules from ruler %s", addr)
		}

		mergedMx.Lock()
		merged = append(merged, newGrps)
		mergedMx.Unlock()

		return nil
	})

	return merged, err
}

// RuleInfos implements the rules service for pagination api
func (r *Ruler) RuleInfos(ctx context.Context, in *RuleInfosRequest) (*RuleInfosResponse, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, fmt.Errorf("no user id found in context")
	}

	groupDescs, err := r.getLocalRuleInfos(userID, *in)
	if err != nil {
		return nil, err
	}

	return &groupDescs, nil
}

// GetAlertInfos retrieves the active alerts from this ruler and all running rulers in the ring if
// sharding is enabled
func (r *Ruler) GetAlertInfos(ctx context.Context, alertInfosRequest AlertInfosRequest) ([]*AlertInfosResponse, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, fmt.Errorf("no user id found in context")
	}

	if r.cfg.EnableSharding {
		return r.getShardedAlertInfos(ctx, userID, alertInfosRequest)
	}

	response, err := r.getLocalAlertInfos(userID, alertInfosRequest)
	return []*AlertInfosResponse{&response}, err
}

func (r *Ruler) getLocalAlertInfos(userID string, alertInfosRequest AlertInfosRequest) (AlertInfosResponse, error) {

	groupDescs, err := r.getLocalRulesCopy(userID, RulesRequest{
		RuleNames:      alertInfosRequest.GetRuleNames(),
		RuleGroupNames: alertInfosRequest.GetRuleGroupNames(),
		Files:          alertInfosRequest.GetFiles(),
	}, false)

	if err != nil {
		return AlertInfosResponse{
			Alerts:    nil,
			NextToken: "",
		}, err
	}

	var alerts []*PaginatedAlertStateDesc
	matcherSets, err := parseMatchersParam(alertInfosRequest.Matches)
	if err != nil {
		return AlertInfosResponse{
			Alerts:    nil,
			NextToken: "",
		}, errors.Wrap(err, "error parsing matcher values")
	}

	for _, g := range groupDescs {
		for _, rl := range g.ActiveRules {
			if rl.Rule.Alert != "" {
				for _, a := range rl.Alerts {
					if matchesMatcherSets(matcherSets, cortexpb.FromLabelAdaptersToLabels(a.Labels)) {
						var rulename string
						if len(rl.Rule.Alert) > 0 {
							rulename = rl.Rule.Alert
						} else {
							rulename = rl.Rule.Record
						}
						ruleDesc := &AlertBelongedRuleDesc{
							Namespace: g.Group.Namespace,
							Group:     g.Group.Name,
							Name:      rulename,
							Order:     strconv.FormatInt(rl.Rule.Order, 10),
						}
						// Skip the alert if the next token is set and hasn't arrived the nextToken item yet.
						alertfp := getAlertNextToken(ruleDesc.Namespace,
							ruleDesc.Group, ruleDesc.Name, ruleDesc.Order, a.Labels)
						if len(alertInfosRequest.NextToken) > 0 && alertInfosRequest.NextToken >= alertfp {
							continue
						}
						paginatedAlertState := &PaginatedAlertStateDesc{
							State:           a.State,
							Labels:          a.Labels,
							Annotations:     a.Annotations,
							Value:           a.Value,
							ActiveAt:        a.ActiveAt,
							FiredAt:         a.FiredAt,
							ResolvedAt:      a.ResolvedAt,
							LastSentAt:      a.LastSentAt,
							ValidUntil:      a.ValidUntil,
							KeepFiringSince: a.KeepFiringSince,
							Rule:            ruleDesc,
						}

						alerts = append(alerts, paginatedAlertState)
					}
				}
			}
		}
	}

	sort.Sort(PaginatedAlertStateDescs(alerts))

	returnAlertStateDescs, nextToken := TruncateAlertInfos(alerts, int(alertInfosRequest.MaxResults))

	return AlertInfosResponse{
		Alerts:    returnAlertStateDescs,
		NextToken: nextToken,
	}, nil
}

func (r *Ruler) getShardedAlertInfos(ctx context.Context, userID string, alertInfosRequest AlertInfosRequest) ([]*AlertInfosResponse, error) {

	var (
		mergedMx sync.Mutex
		merged   []*AlertInfosResponse
	)

	ctx, err := user.InjectIntoGRPCRequest(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to inject user ID into grpc request, %v", err)
	}

	jobs, failedZones, _, _, err := r.getRingJobs(ctx, userID)
	if err != nil {
		return nil, err
	}

	if len(failedZones) > 0 {
		// for now AlertInfos api don't support HA
		return nil, ring.ErrTooManyUnhealthyInstances
	}

	err = concurrency.ForEach(ctx, jobs, len(jobs), func(ctx context.Context, job interface{}) error {
		addr := job.(string)

		rulerClient, err := r.clientsPool.GetClientFor(addr)
		if err != nil {
			return errors.Wrapf(err, "unable to get client for ruler %s", addr)
		}

		newAlerts, err := rulerClient.AlertInfos(ctx, &alertInfosRequest)
		if err != nil {
			return errors.Wrapf(err, "unable to retrieve rules from ruler %s", addr)
		}

		mergedMx.Lock()
		merged = append(merged, newAlerts)
		mergedMx.Unlock()

		return nil
	})

	return merged, err
}

// AlertInfos implements the rules service for pagination api
func (r *Ruler) AlertInfos(ctx context.Context, in *AlertInfosRequest) (*AlertInfosResponse, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, fmt.Errorf("no user id found in context")
	}

	alertDescs, err := r.getLocalAlertInfos(userID, *in)
	if err != nil {
		return nil, err
	}

	return &alertDescs, nil
}

type GroupStateDescs []*GroupStateDesc

func (gi GroupStateDescs) Swap(i, j int) { gi[i], gi[j] = gi[j], gi[i] }
func (gi GroupStateDescs) Less(i, j int) bool {
	return getRuleGroupNextToken(gi[i].Group.Namespace, gi[i].Group.Name) < getRuleGroupNextToken(gi[j].Group.Namespace, gi[j].Group.Name)
}
func (gi GroupStateDescs) Len() int { return len(gi) }

type GroupInfoStateDescs []*GroupInfoStateDesc

func (gi GroupInfoStateDescs) Swap(i, j int) { gi[i], gi[j] = gi[j], gi[i] }
func (gi GroupInfoStateDescs) Less(i, j int) bool {
	return getRuleGroupNextToken(gi[i].Group.Namespace, gi[i].Group.Name) < getRuleGroupNextToken(gi[j].Group.Namespace, gi[j].Group.Name)
}
func (gi GroupInfoStateDescs) Len() int { return len(gi) }

func getRuleGroupNextToken(namespace string, group string) string {
	h := sha1.New()
	h.Write([]byte(namespace + ";" + group))
	return fmt.Sprintf("%x", h.Sum(nil))
}

type PaginatedAlertStateDescs []*PaginatedAlertStateDesc

func (ai PaginatedAlertStateDescs) Swap(i, j int) { ai[i], ai[j] = ai[j], ai[i] }
func (ai PaginatedAlertStateDescs) Less(i, j int) bool {
	return getAlertNextToken(ai[i].Rule.Namespace, ai[i].Rule.Group, ai[i].Rule.Name, ai[i].Rule.Order, ai[i].Labels) <
		getAlertNextToken(ai[j].Rule.Namespace, ai[j].Rule.Group, ai[j].Rule.Name, ai[j].Rule.Order, ai[j].Labels)
}
func (ai PaginatedAlertStateDescs) Len() int { return len(ai) }

func getAlertNextToken(namespace string, group string, name string, order string, alertLabels []cortexpb.LabelAdapter) string {
	h := sha1.New()
	fmt.Fprintf(h, "%s;%s;%s;%s;%s", namespace, group, name, order,
		model.Fingerprint(model.LabelsToSignature(cortexpb.FromLabelAdaptersToLabels(alertLabels).Map())).String())
	return fmt.Sprintf("%x", h.Sum(nil))
}

func TruncateGroupInfos(groupInfos []*GroupInfoStateDesc, maxRuleGroups int) ([]*GroupInfoStateDesc, string) {
	resultNumber := 0
	var returnPaginationToken string
	returnGroupDescs := make([]*GroupInfoStateDesc, 0, len(groupInfos))
	for _, groupInfo := range groupInfos {

		// Add the rule group to the return slice if the maxRuleGroups is not hit
		if maxRuleGroups < 0 || resultNumber < maxRuleGroups {
			returnGroupDescs = append(returnGroupDescs, groupInfo)
			resultNumber++
			continue
		}

		// Return the next token if there is more aggregation group
		if maxRuleGroups > 0 && resultNumber == maxRuleGroups {
			returnPaginationToken = getRuleGroupNextToken(returnGroupDescs[maxRuleGroups-1].Group.Namespace, returnGroupDescs[maxRuleGroups-1].Group.Name)
			break
		}
	}
	return returnGroupDescs, returnPaginationToken
}

func TruncateAlertInfos(alertInfos []*PaginatedAlertStateDesc, maxAlerts int) ([]*PaginatedAlertStateDesc, string) {
	resultNumber := 0
	var returnPaginationToken string
	returnAlertDescs := make([]*PaginatedAlertStateDesc, 0, len(alertInfos))
	for _, alertInfo := range alertInfos {

		// Add the rule group to the return slice if the maxRuleGroups is not hit
		if maxAlerts < 0 || resultNumber < maxAlerts {
			returnAlertDescs = append(returnAlertDescs, alertInfo)
			resultNumber++
			continue
		}

		// Return the next token if there is more aggregation group
		if maxAlerts > 0 && resultNumber == maxAlerts {
			lastAlert := returnAlertDescs[maxAlerts-1]
			returnPaginationToken = getAlertNextToken(lastAlert.Rule.Namespace, lastAlert.Rule.Group, lastAlert.Rule.Name, lastAlert.Rule.Order, lastAlert.Labels)
			break
		}
	}
	return returnAlertDescs, returnPaginationToken
}

type PaginatedGroupStates []*GroupStateDesc

func (gi PaginatedGroupStates) Swap(i, j int) { gi[i], gi[j] = gi[j], gi[i] }
func (gi PaginatedGroupStates) Less(i, j int) bool {
	return GetRuleGroupNextToken(gi[i].Group.Namespace, gi[i].Group.Name) < GetRuleGroupNextToken(gi[j].Group.Namespace, gi[j].Group.Name)
}
func (gi PaginatedGroupStates) Len() int { return len(gi) }

func GetRuleGroupNextToken(namespace string, group string) string {
	h := sha1.New()
	h.Write([]byte(namespace + ";" + group))
	return hex.EncodeToString(h.Sum(nil))
}

// generatePage function takes in a sorted list of groups and returns a page of groups and the next token which can be
// used to in subsequent requests. The # of groups per page is at most equal to maxRuleGroups. If the total passed in
// rule group count is greater than maxRuleGroups, then a next token is returned. Otherwise, next token is empty
func generatePage(groups []*GroupStateDesc, maxRuleGroups int) ([]*GroupStateDesc, string) {
	resultNumber := 0
	var returnPaginationToken string
	returnGroupDescs := make([]*GroupStateDesc, 0, len(groups))
	for _, groupInfo := range groups {

		// Add the rule group to the return slice if the maxRuleGroups is not hit
		if maxRuleGroups < 0 || resultNumber < maxRuleGroups {
			returnGroupDescs = append(returnGroupDescs, groupInfo)
			resultNumber++
			continue
		}

		// Return the next token if there are more groups
		if maxRuleGroups > 0 && resultNumber == maxRuleGroups {
			returnPaginationToken = GetRuleGroupNextToken(returnGroupDescs[maxRuleGroups-1].Group.Namespace, returnGroupDescs[maxRuleGroups-1].Group.Name)
			break
		}
	}
	return returnGroupDescs, returnPaginationToken
}
