package filter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/plugin"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/scheduling"
)

const (
	// LengthBucketingType is the type of the LengthBucketing filter
	LengthBucketingType = "length-bucketing"
	// AverageCharactersPerToken is an estimated average characters per token.
	AverageCharactersPerToken = 4
)

type BucketConfig struct {
	MinTokens  int    `json:"minTokens"`
	MaxTokens  int    `json:"maxTokens"`
	LabelValue string `json:"labelValue"`
}

type lengthBucketingParameters struct {
	LabelName string         `json:"labelName"`
	Buckets   []BucketConfig `json:"buckets"`
}

var _ scheduling.Filter = &LengthBucketing{} // validate interface conformance

// LengthBucketingFactory defines the factory function for the LengthBucketing filter.
func LengthBucketingFactory(name string, rawParameters json.RawMessage, _ plugin.Handle) (plugin.Plugin, error) {
	parameters := lengthBucketingParameters{}
	if rawParameters != nil {
		if err := json.Unmarshal(rawParameters, &parameters); err != nil {
			return nil, fmt.Errorf("failed to parse the parameters of the '%s' filter - %w", LengthBucketingType, err)
		}
	}
	if name == "" {
		return nil, fmt.Errorf("invalid configuration for '%s' filter: name cannot be empty", LengthBucketingType)
	}
	if parameters.LabelName == "" {
		return nil, fmt.Errorf("invalid configuration for '%s' filter: 'labelName' must be specified", LengthBucketingType)
	}
	if len(parameters.Buckets) == 0 {
		return nil, fmt.Errorf("invalid configuration for '%s' filter: 'buckets' must be non-empty", LengthBucketingType)
	}
	return &LengthBucketing{
		typedName: plugin.TypedName{Type: LengthBucketingType, Name: name},
		labelName: parameters.LabelName,
		buckets:   parameters.Buckets,
	}, nil
}

// LengthBucketing - filters out endpoints based on the token length of the request
type LengthBucketing struct {
	typedName plugin.TypedName
	labelName string
	buckets   []BucketConfig
}

// TypedName returns the typed name of the plugin
func (f *LengthBucketing) TypedName() plugin.TypedName {
	return f.typedName
}

// Filter filters out endpoints that do not match the length bucket of the request
func (f *LengthBucketing) Filter(ctx context.Context, _ *scheduling.CycleState, request *scheduling.LLMRequest, endpoints []scheduling.Endpoint) []scheduling.Endpoint {
	logger := log.FromContext(ctx)

	if request == nil {
		return endpoints
	}

	tokenLen, err := getUserInputLenInTokens(request)
	if err != nil {
		logger.Error(err, "length bucketing: failed to get token length")
		return endpoints
	}

	targetLabelValue := ""
	for _, bucket := range f.buckets {
		if tokenLen >= bucket.MinTokens && tokenLen < bucket.MaxTokens {
			targetLabelValue = bucket.LabelValue
			break
		}
	}

	if targetLabelValue == "" {
		logger.Info("length bucketing: no matching bucket found for token length", "tokenLen", tokenLen)
		return endpoints
	}

	filteredEndpoints := []scheduling.Endpoint{}
	for _, endpoint := range endpoints {
		val, labelDefined := endpoint.GetMetadata().Labels[f.labelName]
		if labelDefined && val == targetLabelValue {
			filteredEndpoints = append(filteredEndpoints, endpoint)
		}
	}

	logger.Info("length bucketing", "tokenLen", tokenLen, "targetLabelValue", targetLabelValue, "filteredCount", len(filteredEndpoints))

	return filteredEndpoints
}

// getUserInputLenInTokens returns an estimated token count for the user input.
func getUserInputLenInTokens(request *scheduling.LLMRequest) (int, error) {
	if request == nil || request.Body == nil {
		return 0, errors.New("request or request body is nil")
	}
	if request.Body.Completions != nil {
		return len(request.Body.Completions.Prompt) / AverageCharactersPerToken, nil
	}
	if request.Body.ChatCompletions == nil {
		return 0, errors.New("request has neither completions nor chat completions body")
	}
	prompt, err := json.Marshal(request.Body.ChatCompletions.Messages)
	if err != nil {
		return 0, err
	}
	return len(prompt) / AverageCharactersPerToken, nil
}
