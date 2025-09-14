package services

import (
	"context"
	"fmt"
	"telegraws/utils"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/wafv2"
	wafTypes "github.com/aws/aws-sdk-go-v2/service/wafv2/types"
	"go.uber.org/zap"
)

// Helper function to get ALB ARN from WAF
func getALBARNFromWAF(ctx context.Context, wafClient *wafv2.Client, webACLName, webACLId string, scope wafTypes.Scope) (string, error) {
	webACLInput := &wafv2.GetWebACLInput{
		Name:  aws.String(webACLName),
		Scope: scope,
		Id:    aws.String(webACLId),
	}

	webACL, err := wafClient.GetWebACL(ctx, webACLInput)
	if err != nil {
		return "", fmt.Errorf("failed to get WAF details: %w", err)
	}

	// CloudFront scope has no ALB associations
	if scope == wafTypes.ScopeCloudfront {
		return "", nil
	}

	resourcesInput := &wafv2.ListResourcesForWebACLInput{
		WebACLArn:    webACL.WebACL.ARN,
		ResourceType: wafTypes.ResourceTypeApplicationLoadBalancer,
	}

	resourcesOutput, err := wafClient.ListResourcesForWebACL(ctx, resourcesInput)
	if err != nil {
		return "", fmt.Errorf("failed to get resources for WAF: %w", err)
	}

	if len(resourcesOutput.ResourceArns) == 0 {
		return "", fmt.Errorf("no ALB resources associated with WAF")
	}

	if len(resourcesOutput.ResourceArns) > 1 {
		return "", fmt.Errorf("multiple ALB resources found, expected only one")
	}

	return resourcesOutput.ResourceArns[0], nil
}

func WAFMetrics(ctx context.Context, wafClient *wafv2.Client, cwClient *cloudwatch.Client, webACLId, webACLName string, scopeStr string, timeParams map[string]time.Time) (map[string]float64, error) {
	// default → REGIONAL
	var scope wafTypes.Scope
	switch scopeStr {
	case "CLOUDFRONT":
		scope = wafTypes.ScopeCloudfront
	default:
		scope = wafTypes.ScopeRegional
	}

	albARN, err := getALBARNFromWAF(ctx, wafClient, webACLName, webACLId, scope)
	if err != nil && scope == wafTypes.ScopeRegional {
		return nil, fmt.Errorf("failed to get ALB ARN from WAF: %w", err)
	}

	metrics := map[string]float64{}
	period := aws.Int32(3600)
	if timeParams["endTime"].Sub(timeParams["startTime"]) >= 24*time.Hour {
		period = aws.Int32(86400)
	}

	wafMetrics := []struct {
		Name      string
		Statistic string
	}{
		{"AllowedRequests", "Sum"},
		{"BlockedRequests", "Sum"},
	}

	for _, metric := range wafMetrics {
		var dimensions []types.Dimension

		if scope == wafTypes.ScopeCloudfront {
			// CloudFront WAF metric dimension is WebACL
			dimensions = []types.Dimension{
				{Name: aws.String("WebACL"), Value: aws.String(webACLName)},
			}
		} else {
			// Regional WAF (ALB)
			dimensions = []types.Dimension{
				{Name: aws.String("Resource"), Value: aws.String(albARN)},
				{Name: aws.String("ResourceType"), Value: aws.String("ALB")},
			}
		}

		input := &cloudwatch.GetMetricStatisticsInput{
			Namespace:  aws.String("AWS/WAFV2"),
			MetricName: aws.String(metric.Name),
			Dimensions: dimensions,
			StartTime:  aws.Time(timeParams["startTime"]),
			EndTime:    aws.Time(timeParams["endTime"]),
			Period:     period,
			Statistics: []types.Statistic{types.Statistic(metric.Statistic)},
		}

		result, err := cwClient.GetMetricStatistics(ctx, input)
		if err != nil {
			utils.Logger.Error("Failed to get WAF metric",
				zap.Error(err),
				zap.String("metricName", metric.Name),
				zap.String("statistic", metric.Statistic),
				zap.String("webACLId", webACLId),
				zap.String("webACLName", webACLName),
				zap.String("scope", scopeStr),
				zap.Int32("period", *period),
			)
			continue
		}

		if len(result.Datapoints) > 0 {
			metrics[metric.Name] = *result.Datapoints[0].Sum
		} else {
			metrics[metric.Name] = 0.0
		}
	}

	return metrics, nil
}
