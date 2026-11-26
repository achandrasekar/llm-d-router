/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package tierheadroom provides a capacity-aware heterogeneous decode tier
// scorer. It prefers endpoints matching a request's decode-tier header while
// enabling work-conserving spillover to another tier that still has headroom
// once the preferred tier reaches its configured per-tier concurrency or KV
// utilization ceiling.
package tierheadroom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrconcurrency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/concurrency"
)

const (
	PluginType = "tier-headroom-scorer"

	defaultSpilloverScore      = 0.50
	defaultSaturatedMatchScore = 0.25
)

type parameters struct {
	HeaderName               string              `json:"headerName"`
	LabelKey                 string              `json:"labelKey"`
	TierMaxRunning           map[string]int      `json:"tierMaxRunning,omitempty"`
	TierMaxKVCacheUtil       map[string]float64  `json:"tierMaxKVCacheUtil,omitempty"`
	SpilloverTargets         map[string][]string `json:"spilloverTargets,omitempty"`
	SpilloverScore           *float64            `json:"spilloverScore,omitempty"`
	SaturatedMatchScore      *float64            `json:"saturatedMatchScore,omitempty"`
	StampResponseHeader      *bool               `json:"stampResponseHeader,omitempty"`
	InFlightLoadProducerName string              `json:"inFlightLoadProducerName,omitempty"`
}

// Factory creates a tier-headroom-scorer plugin instance.
func Factory(name string, rawParameters *json.Decoder, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	if rawParameters == nil {
		return nil, errors.New("tier-headroom-scorer requires parameters")
	}
	params := parameters{}
	if err := rawParameters.Decode(&params); err != nil {
		return nil, fmt.Errorf("decode %s parameters: %w", PluginType, err)
	}
	if strings.TrimSpace(params.HeaderName) == "" {
		return nil, errors.New("tier-headroom-scorer headerName is required")
	}
	if strings.TrimSpace(params.LabelKey) == "" {
		return nil, errors.New("tier-headroom-scorer labelKey is required")
	}
	spillover := defaultSpilloverScore
	if params.SpilloverScore != nil {
		spillover = *params.SpilloverScore
	}
	saturatedMatch := defaultSaturatedMatchScore
	if params.SaturatedMatchScore != nil {
		saturatedMatch = *params.SaturatedMatchScore
	}
	if spillover < 0 || spillover > 1.0 {
		return nil, fmt.Errorf("spilloverScore must be in [0, 1], got %f", spillover)
	}
	if saturatedMatch < 0 || saturatedMatch > 1.0 {
		return nil, fmt.Errorf("saturatedMatchScore must be in [0, 1], got %f", saturatedMatch)
	}
	for tier, lim := range params.TierMaxRunning {
		if lim < 0 {
			return nil, fmt.Errorf("tierMaxRunning[%q] must be non-negative, got %d", tier, lim)
		}
	}
	for tier, lim := range params.TierMaxKVCacheUtil {
		if lim < 0 || lim > 1.0 {
			return nil, fmt.Errorf("tierMaxKVCacheUtil[%q] must be in [0, 1], got %f", tier, lim)
		}
	}
	stamp := true
	if params.StampResponseHeader != nil {
		stamp = *params.StampResponseHeader
	}
	var spilloverTargets map[string]map[string]struct{}
	if params.SpilloverTargets != nil {
		spilloverTargets = make(map[string]map[string]struct{}, len(params.SpilloverTargets))
		for srcTier, targets := range params.SpilloverTargets {
			set := make(map[string]struct{}, len(targets))
			for _, t := range targets {
				if trimmed := strings.TrimSpace(t); trimmed != "" {
					set[trimmed] = struct{}{}
				}
			}
			spilloverTargets[strings.TrimSpace(srcTier)] = set
		}
	}
	if name == "" {
		name = PluginType
	}
	return &Scorer{
		typedName:           fwkplugin.TypedName{Type: PluginType, Name: name},
		headerName:          strings.ToLower(strings.TrimSpace(params.HeaderName)),
		labelKey:            strings.TrimSpace(params.LabelKey),
		tierMaxRunning:      params.TierMaxRunning,
		tierMaxKVCacheUtil:  params.TierMaxKVCacheUtil,
		spilloverTargets:    spilloverTargets,
		spilloverScore:      spillover,
		saturatedMatchScore: saturatedMatch,
		stampResponseHeader: stamp,
		inFlightLoadDataKey: attrconcurrency.InFlightLoadDataKey.WithNonEmptyProducerName(params.InFlightLoadProducerName),
	}, nil
}

// Scorer scores heterogeneous decode tiers with per-tier headroom ceilings and
// work-conserving spillover when a preferred tier reaches its soft ceiling.
type Scorer struct {
	typedName           fwkplugin.TypedName
	headerName          string
	labelKey            string
	tierMaxRunning      map[string]int
	tierMaxKVCacheUtil  map[string]float64
	spilloverTargets    map[string]map[string]struct{}
	spilloverScore      float64
	saturatedMatchScore float64
	stampResponseHeader bool
	inFlightLoadDataKey fwkplugin.DataKey
}

var (
	_ fwksched.Scorer               = (*Scorer)(nil)
	_ fwkrh.ResponseHeaderProcessor = (*Scorer)(nil)
	_ fwkplugin.ConsumerPlugin      = (*Scorer)(nil)
)

func (s *Scorer) TypedName() fwkplugin.TypedName { return s.typedName }

func (s *Scorer) Category() fwksched.ScorerCategory { return fwksched.Affinity }

// Consumes declares in-flight concurrency as optional so the scorer functions
// cleanly both with and without inflight-load-producer configured.
func (s *Scorer) Consumes() fwkplugin.DataDependencies {
	return fwkplugin.DataDependencies{
		Optional: map[fwkplugin.DataKey]any{
			s.inFlightLoadDataKey: attrconcurrency.InFlightLoad{},
		},
	}
}

func (s *Scorer) Score(_ context.Context, request *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) map[fwksched.Endpoint]float64 {
	scores := make(map[fwksched.Endpoint]float64, len(endpoints))
	requested := ""
	if request != nil && request.Headers != nil {
		requested = strings.TrimSpace(request.Headers[s.headerName])
	}
	for _, ep := range endpoints {
		scores[ep] = 0
		if requested == "" || ep == nil || ep.GetMetadata() == nil {
			continue
		}
		epTier := ep.GetMetadata().Labels[s.labelKey]
		saturated := s.isSaturated(ep, epTier)
		if epTier == requested {
			if saturated {
				scores[ep] = s.saturatedMatchScore
			} else {
				scores[ep] = 1.0
			}
		} else if !saturated && s.canSpillover(requested, epTier) {
			scores[ep] = s.spilloverScore
		}
	}
	return scores
}

func (s *Scorer) canSpillover(requested, target string) bool {
	if s.spilloverTargets == nil {
		return true
	}
	allowed, configured := s.spilloverTargets[requested]
	if !configured {
		return true
	}
	_, ok := allowed[target]
	return ok
}

func (s *Scorer) isSaturated(ep fwksched.Endpoint, tier string) bool {
	if maxRun, ok := s.tierMaxRunning[tier]; ok && maxRun > 0 {
		if s.effectiveRunning(ep) >= int64(maxRun) {
			return true
		}
	}
	if maxKV, ok := s.tierMaxKVCacheUtil[tier]; ok && maxKV > 0 {
		if m := ep.GetMetrics(); m != nil && m.Updated() && m.KVCacheUsagePercent >= maxKV {
			return true
		}
	}
	return false
}

func (s *Scorer) effectiveRunning(ep fwksched.Endpoint) int64 {
	var loadCount int64
	if m := ep.GetMetrics(); m != nil && m.Updated() {
		loadCount = int64(m.RunningRequestsSize)
	}
	if raw, ok := ep.Get(s.inFlightLoadDataKey); ok {
		if inflight, ok := raw.(*attrconcurrency.InFlightLoad); ok && inflight != nil && inflight.Requests > loadCount {
			loadCount = inflight.Requests
		}
	}
	return loadCount
}

// ResponseHeader stamps the selected endpoint's actual tier label on the HTTP
// response for routing observability, regardless of whether the request asked
// for a tier.
func (s *Scorer) ResponseHeader(_ context.Context, _ *fwksched.InferenceRequest, response *fwkrh.Response, endpoint *fwkdl.EndpointMetadata) {
	if !s.stampResponseHeader || endpoint == nil || response == nil || response.Headers == nil {
		return
	}
	if value := endpoint.Labels[s.labelKey]; value != "" {
		response.Headers[s.headerName] = value
	}
}
