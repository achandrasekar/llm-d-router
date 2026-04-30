package filter

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"
	fwkdl "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/datalayer"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/plugin"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/scheduling"
)

func createTestEndpoint(name string, labels map[string]string) scheduling.Endpoint {
	return scheduling.NewEndpoint(
		&fwkdl.EndpointMetadata{
			NamespacedName: k8stypes.NamespacedName{Namespace: "default", Name: name},
			Address:        "127.0.0.1",
			Labels:         labels,
		},
		&fwkdl.Metrics{},
		nil,
	)
}

func TestLengthBucketing(t *testing.T) {
	buckets := []BucketConfig{
		{MinTokens: 0, MaxTokens: 200, LabelValue: "small"},
		{MinTokens: 200, MaxTokens: 500, LabelValue: "medium"},
		{MinTokens: 500, MaxTokens: 100000, LabelValue: "large"},
	}

	filter := &LengthBucketing{
		typedName: plugin.TypedName{Type: LengthBucketingType, Name: "test"},
		labelName: "vllm-bucket",
		buckets:   buckets,
	}

	endpoints := []scheduling.Endpoint{
		createTestEndpoint("pod-small", map[string]string{"vllm-bucket": "small"}),
		createTestEndpoint("pod-medium", map[string]string{"vllm-bucket": "medium"}),
		createTestEndpoint("pod-large", map[string]string{"vllm-bucket": "large"}),
	}

	tests := []struct {
		name              string
		request           *scheduling.LLMRequest
		expectedEndpoints []scheduling.Endpoint
	}{
		{
			name: "Small request",
			request: &scheduling.LLMRequest{
				Body: &scheduling.LLMRequestBody{
					Completions: &scheduling.CompletionsRequest{
						Prompt: "short prompt", // 12 chars / 4 = 3 tokens
					},
				},
			},
			expectedEndpoints: []scheduling.Endpoint{endpoints[0]},
		},
		{
			name: "Medium request",
			request: &scheduling.LLMRequest{
				Body: &scheduling.LLMRequestBody{
					Completions: &scheduling.CompletionsRequest{
						Prompt: string(make([]byte, 1000)), // 1000 chars / 4 = 250 tokens
					},
				},
			},
			expectedEndpoints: []scheduling.Endpoint{endpoints[1]},
		},
		{
			name: "Large request",
			request: &scheduling.LLMRequest{
				Body: &scheduling.LLMRequestBody{
					Completions: &scheduling.CompletionsRequest{
						Prompt: string(make([]byte, 3000)), // 3000 chars / 4 = 750 tokens
					},
				},
			},
			expectedEndpoints: []scheduling.Endpoint{endpoints[2]},
		},
		{
			name: "No matching bucket (fallback to all)",
			request: &scheduling.LLMRequest{
				Body: &scheduling.LLMRequestBody{
					Completions: &scheduling.CompletionsRequest{
						Prompt: string(make([]byte, 500000)), // 125000 tokens
					},
				},
			},
			expectedEndpoints: endpoints,
		},
		{
			name: "Chat completions small request",
			request: &scheduling.LLMRequest{
				Body: &scheduling.LLMRequestBody{
					ChatCompletions: &scheduling.ChatCompletionsRequest{
						Messages: []scheduling.Message{
							{
								Role:    "user",
								Content: scheduling.Content{Raw: "short prompt"},
							},
						},
					},
				},
			},
			expectedEndpoints: []scheduling.Endpoint{endpoints[0]},
		},
		{
			name: "Chat completions medium request",
			request: &scheduling.LLMRequest{
				Body: &scheduling.LLMRequestBody{
					ChatCompletions: &scheduling.ChatCompletionsRequest{
						Messages: []scheduling.Message{
							{
								Role:    "user",
								Content: scheduling.Content{Raw: strings.Repeat("a", 1000)},
							},
						},
					},
				},
			},
			expectedEndpoints: []scheduling.Endpoint{endpoints[1]},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filter.Filter(context.Background(), nil, tt.request, endpoints)
			assert.Equal(t, tt.expectedEndpoints, result)
		})
	}
}

func TestLengthBucketingFactory(t *testing.T) {
	rawConfig := json.RawMessage(`{
		"labelName": "vllm-bucket",
		"buckets": [
			{"minTokens": 0, "maxTokens": 200, "labelValue": "small"}
		]
	}`)

	p, err := LengthBucketingFactory("test-filter", rawConfig, nil)
	require.NoError(t, err)
	assert.NotNil(t, p)
	assert.Equal(t, LengthBucketingType, p.TypedName().Type)
}
