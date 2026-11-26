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

package tierheadroom

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrconcurrency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/concurrency"
)

func makeEndpoint(tier string, running int, kvUtil float64, inflight int64) fwksched.Endpoint {
	m := &fwkdl.Metrics{
		RunningRequestsSize: running,
		KVCacheUsagePercent: kvUtil,
		UpdateTime:          time.Now(),
	}
	ep := fwksched.NewEndpoint(
		&fwkdl.EndpointMetadata{
			Labels: map[string]string{"llm-d.ai/decode-tier": tier},
		},
		m,
		nil,
	)
	if inflight >= 0 {
		ep.Put(attrconcurrency.InFlightLoadDataKey, &attrconcurrency.InFlightLoad{Requests: inflight})
	}
	return ep
}

func TestTierHeadroomScorer(t *testing.T) {
	cfg := `{
		"headerName": "x-decode-tier",
		"labelKey": "llm-d.ai/decode-tier",
		"tierMaxRunning": {"fast": 24, "bulk": 14},
		"tierMaxKVCacheUtil": {"fast": 0.88, "bulk": 0.85}
	}`
	p, err := Factory("decode-tier-headroom", json.NewDecoder(strings.NewReader(cfg)), nil)
	require.NoError(t, err)
	scorer := p.(*Scorer)

	t.Run("prefers matching tier when both have headroom", func(t *testing.T) {
		epFast := makeEndpoint("fast", 8, 0.30, 8)
		epBulk := makeEndpoint("bulk", 6, 0.25, 6)
		req := &fwksched.InferenceRequest{Headers: map[string]string{"x-decode-tier": "bulk"}}

		scores := scorer.Score(context.Background(), req, []fwksched.Endpoint{epFast, epBulk})
		require.InDelta(t, 1.0, scores[epBulk], 1e-6)
		require.InDelta(t, 0.5, scores[epFast], 1e-6)
	})

	t.Run("spills over to non-matching tier when preferred tier reaches inflight ceiling", func(t *testing.T) {
		epFast := makeEndpoint("fast", 10, 0.40, 10)
		// Scraped running=12, but live inflight=14 hits bulk soft ceiling (14) -> spill to fast!
		epBulk := makeEndpoint("bulk", 12, 0.50, 14)
		req := &fwksched.InferenceRequest{Headers: map[string]string{"x-decode-tier": "bulk"}}

		scores := scorer.Score(context.Background(), req, []fwksched.Endpoint{epFast, epBulk})
		require.InDelta(t, 0.50, scores[epFast], 1e-6)
		require.InDelta(t, 0.25, scores[epBulk], 1e-6)
	})

	t.Run("keeps matching tier preference when both tiers are saturated", func(t *testing.T) {
		epFast := makeEndpoint("fast", 25, 0.90, 25)
		epBulk := makeEndpoint("bulk", 15, 0.90, 15)
		req := &fwksched.InferenceRequest{Headers: map[string]string{"x-decode-tier": "fast"}}

		scores := scorer.Score(context.Background(), req, []fwksched.Endpoint{epFast, epBulk})
		require.InDelta(t, 0.25, scores[epFast], 1e-6)
		require.InDelta(t, 0.0, scores[epBulk], 1e-6)
	})

	t.Run("asymmetric spilloverTargets allows bulk-to-fast but blocks fast-to-bulk downgrade", func(t *testing.T) {
		asymCfg := `{
			"headerName": "x-decode-tier",
			"labelKey": "llm-d.ai/decode-tier",
			"tierMaxRunning": {"fast": 14, "bulk": 10},
			"spilloverTargets": {"bulk": ["fast"], "fast": []}
		}`
		ap, err := Factory("asym", json.NewDecoder(strings.NewReader(asymCfg)), nil)
		require.NoError(t, err)
		asym := ap.(*Scorer)

		// Fast at ceiling (14) while bulk has headroom (6): fast must NOT downgrade to bulk.
		epFastFull := makeEndpoint("fast", 14, 0.50, 14)
		epBulkFree := makeEndpoint("bulk", 6, 0.30, 6)
		fastReq := &fwksched.InferenceRequest{Headers: map[string]string{"x-decode-tier": "fast"}}
		scores := asym.Score(context.Background(), fastReq, []fwksched.Endpoint{epFastFull, epBulkFree})
		require.InDelta(t, 0.25, scores[epFastFull], 1e-6)
		require.InDelta(t, 0.0, scores[epBulkFree], 1e-6)

		// Bulk at ceiling (10) while fast has headroom (8): bulk DOES spill up to fast.
		epFastFree := makeEndpoint("fast", 8, 0.30, 8)
		epBulkFull := makeEndpoint("bulk", 10, 0.45, 10)
		bulkReq := &fwksched.InferenceRequest{Headers: map[string]string{"x-decode-tier": "bulk"}}
		scores = asym.Score(context.Background(), bulkReq, []fwksched.Endpoint{epFastFree, epBulkFull})
		require.InDelta(t, 0.50, scores[epFastFree], 1e-6)
		require.InDelta(t, 0.25, scores[epBulkFull], 1e-6)
	})

	t.Run("returns neutral zero without header and still stamps response header", func(t *testing.T) {
		epFast := makeEndpoint("fast", 4, 0.20, 4)
		req := &fwksched.InferenceRequest{Headers: map[string]string{}}
		scores := scorer.Score(context.Background(), req, []fwksched.Endpoint{epFast})
		require.InDelta(t, 0.0, scores[epFast], 1e-6)

		resp := &fwkrh.Response{Headers: map[string]string{}}
		scorer.ResponseHeader(context.Background(), req, resp, epFast.GetMetadata())
		require.Equal(t, "fast", resp.Headers["x-decode-tier"])
	})
}
