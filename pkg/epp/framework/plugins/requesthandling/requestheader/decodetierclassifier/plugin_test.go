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

package decodetierclassifier

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

func TestDecodeTierClassifier(t *testing.T) {
	p, err := PluginFactory("", nil, nil)
	require.NoError(t, err)
	proc := p.(*Plugin)

	t.Run("main agent turn defaults to fast tier", func(t *testing.T) {
		req := &fwksched.InferenceRequest{
			Headers: map[string]string{
				"x-dynamo-session-id": "trace-abc-123",
			},
		}
		require.NoError(t, proc.RequestHeader(context.Background(), req))
		require.Equal(t, "fast", req.Headers["x-decode-tier"])
	})

	t.Run("aiperf subagent session marker classifies as bulk tier", func(t *testing.T) {
		req := &fwksched.InferenceRequest{
			Headers: map[string]string{
				"x-dynamo-session-id": "trace-abc-123::sa:subagent_001_deadbeef",
			},
		}
		require.NoError(t, proc.RequestHeader(context.Background(), req))
		require.Equal(t, "bulk", req.Headers["x-decode-tier"])
	})

	t.Run("explicit parent or claude-code subagent header classifies as bulk tier", func(t *testing.T) {
		for _, hdr := range []string{
			"x-dynamo-parent-session-id",
			"x-parent-session-id",
			"x-opencode-parent-session-id",
			"x-claude-code-agent-id",
			"x-codex-parent-thread-id",
		} {
			req := &fwksched.InferenceRequest{
				Headers: map[string]string{
					hdr: "parent-42",
				},
			}
			require.NoError(t, proc.RequestHeader(context.Background(), req))
			require.Equal(t, "bulk", req.Headers["x-decode-tier"], "header %s", hdr)
		}
	})

	t.Run("preserves explicit caller x-decode-tier unless overrideExisting is true", func(t *testing.T) {
		req := &fwksched.InferenceRequest{
			Headers: map[string]string{
				"x-dynamo-session-id": "trace-abc-123::sa:sub_001",
				"x-decode-tier":       "fast",
			},
		}
		require.NoError(t, proc.RequestHeader(context.Background(), req))
		require.Equal(t, "fast", req.Headers["x-decode-tier"])

		dec := json.NewDecoder(strings.NewReader(`{"overrideExisting": true, "defaultTier": "interactive", "subagentTier": "batch"}`))
		p2, err := PluginFactory("custom", dec, nil)
		require.NoError(t, err)
		require.NoError(t, p2.(*Plugin).RequestHeader(context.Background(), req))
		require.Equal(t, "batch", req.Headers["x-decode-tier"])
	})
}
