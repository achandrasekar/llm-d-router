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

// Package decodetierclassifier provides a RequestHeaderProcessor plugin that
// classifies incoming agentic requests into heterogeneous decode tiers (such as
// "fast" for interactive main turns vs. "bulk" for background subagents) and
// stamps the tier header before scheduling.
package decodetierclassifier

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

const (
	PluginType = "decode-tier-classifier"

	defaultTierHeaderName = "x-decode-tier"
	defaultFastTier       = "fast"
	defaultSubagentTier   = "bulk"
)

var (
	defaultSubagentPresenceHeaders = []string{
		"x-dynamo-parent-session-id",
		"x-parent-session-id",
		"x-opencode-parent-session-id",
		"x-claude-code-agent-id",
		"x-codex-parent-thread-id",
	}
	defaultSessionHeaders = []string{
		"x-dynamo-session-id",
		"x-correlation-id",
		"x-claude-code-session-id",
		"x-session-affinity",
		"x-opencode-session-id",
	}
	defaultSubagentMarkers = []string{"::sa:"}
)

// Parameters configures the decode-tier-classifier plugin.
type Parameters struct {
	// TierHeaderName is the request header stamped with the classified decode tier.
	// Defaults to "x-decode-tier".
	TierHeaderName string `json:"tierHeaderName,omitempty"`
	// DefaultTier is assigned to interactive / primary requests. Defaults to "fast".
	DefaultTier string `json:"defaultTier,omitempty"`
	// SubagentTier is assigned to background subagent requests. Defaults to "bulk".
	SubagentTier string `json:"subagentTier,omitempty"`
	// OverrideExisting forces reclassification even if TierHeaderName is already set.
	OverrideExisting bool `json:"overrideExisting,omitempty"`
	// SubagentPresenceHeaders lists headers whose non-empty presence marks a request as a subagent.
	SubagentPresenceHeaders []string `json:"subagentPresenceHeaders,omitempty"`
	// SessionHeaders lists session-identifier headers inspected for SubagentMarkers.
	SessionHeaders []string `json:"sessionHeaders,omitempty"`
	// SubagentMarkers lists substrings inside SessionHeaders values that mark a subagent request.
	SubagentMarkers []string `json:"subagentMarkers,omitempty"`
}

// PluginFactory creates a decode-tier-classifier plugin instance.
func PluginFactory(name string, rawParameters *json.Decoder, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	params := Parameters{}
	if rawParameters != nil {
		if err := rawParameters.Decode(&params); err != nil {
			return nil, fmt.Errorf("decode %s parameters: %w", PluginType, err)
		}
	}
	tierHeader := strings.ToLower(strings.TrimSpace(params.TierHeaderName))
	if tierHeader == "" {
		tierHeader = defaultTierHeaderName
	}
	defaultTier := strings.TrimSpace(params.DefaultTier)
	if defaultTier == "" {
		defaultTier = defaultFastTier
	}
	subagentTier := strings.TrimSpace(params.SubagentTier)
	if subagentTier == "" {
		subagentTier = defaultSubagentTier
	}
	presenceHeaders := normalizeHeaders(params.SubagentPresenceHeaders, defaultSubagentPresenceHeaders)
	sessionHeaders := normalizeHeaders(params.SessionHeaders, defaultSessionHeaders)
	markers := params.SubagentMarkers
	if len(markers) == 0 {
		markers = defaultSubagentMarkers
	}
	if name == "" {
		name = PluginType
	}
	return &Plugin{
		typedName:               fwkplugin.TypedName{Type: PluginType, Name: name},
		tierHeaderName:          tierHeader,
		defaultTier:             defaultTier,
		subagentTier:            subagentTier,
		overrideExisting:        params.OverrideExisting,
		subagentPresenceHeaders: presenceHeaders,
		sessionHeaders:          sessionHeaders,
		subagentMarkers:         markers,
	}, nil
}

func normalizeHeaders(provided, fallback []string) []string {
	src := provided
	if len(src) == 0 {
		src = fallback
	}
	out := make([]string, 0, len(src))
	for _, h := range src {
		h = strings.ToLower(strings.TrimSpace(h))
		if h != "" {
			out = append(out, h)
		}
	}
	return out
}

// Plugin stamps a classified decode tier onto incoming InferenceRequest headers.
type Plugin struct {
	typedName               fwkplugin.TypedName
	tierHeaderName          string
	defaultTier             string
	subagentTier            string
	overrideExisting        bool
	subagentPresenceHeaders []string
	sessionHeaders          []string
	subagentMarkers         []string
}

var _ fwkrh.RequestHeaderProcessor = (*Plugin)(nil)

func (p *Plugin) TypedName() fwkplugin.TypedName { return p.typedName }

// RequestHeader classifies the request and sets the tier header prior to scheduling.
func (p *Plugin) RequestHeader(_ context.Context, request *fwksched.InferenceRequest) error {
	if request == nil {
		return nil
	}
	if request.Headers == nil {
		request.Headers = map[string]string{}
	}
	if !p.overrideExisting && strings.TrimSpace(request.Headers[p.tierHeaderName]) != "" {
		return nil
	}
	if p.isSubagent(request.Headers) {
		request.Headers[p.tierHeaderName] = p.subagentTier
	} else {
		request.Headers[p.tierHeaderName] = p.defaultTier
	}
	return nil
}

func (p *Plugin) isSubagent(headers map[string]string) bool {
	for _, h := range p.subagentPresenceHeaders {
		if strings.TrimSpace(headers[h]) != "" {
			return true
		}
	}
	for _, h := range p.sessionHeaders {
		val := headers[h]
		if val == "" {
			continue
		}
		for _, marker := range p.subagentMarkers {
			if marker != "" && strings.Contains(val, marker) {
				return true
			}
		}
	}
	return false
}
