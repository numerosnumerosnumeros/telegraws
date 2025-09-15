package services

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func DynamoDBMetrics(
	ctx context.Context,
	cwClient *cloudwatch.Client,
	dynamoClient *dynamodb.Client,
	timeParams map[string]time.Time,
	tableName string,
) (map[string]float64, error) {

	metrics := map[string]float64{}
	period := aws.Int32(3600)
	if timeParams["endTime"].Sub(timeParams["startTime"]) >= 24*time.Hour {
		period = aws.Int32(86400)
	}

	// DescribeTable call
	out, err := dynamoClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe table: %w", err)
	}

	// Billing mode
	onDemand := false
	if out.Table != nil && out.Table.BillingModeSummary != nil {
		onDemand = out.Table.BillingModeSummary.BillingMode == dynamodbTypes.BillingModePayPerRequest
	}
	if onDemand {
		metrics["BillingMode"] = 1
	} else {
		metrics["BillingMode"] = 0
	}

	// Item count (approximate)
	if out.Table != nil && out.Table.ItemCount != nil {
		metrics["ItemCount"] = float64(*out.Table.ItemCount)
	} else {
		metrics["ItemCount"] = 0
	}

	// CloudWatch metrics
	dynamoMetrics := []struct {
		Name      string
		Statistic string
	}{
		{"ReadThrottleEvents", "Sum"},
		{"WriteThrottleEvents", "Sum"},
		{"SystemErrors", "Sum"},
		{"UserErrors", "Sum"},
		{"ConsumedReadCapacityUnits", "Sum"},
		{"ConsumedWriteCapacityUnits", "Sum"},
	}

	if !onDemand {
		dynamoMetrics = append(dynamoMetrics,
			struct {
				Name      string
				Statistic string
			}{"RequestCount", "Sum"},
			struct {
				Name      string
				Statistic string
			}{"SuccessfulRequestLatency", "Average"},
		)
	}

	for _, metric := range dynamoMetrics {
		input := &cloudwatch.GetMetricStatisticsInput{
			Namespace:  aws.String("AWS/DynamoDB"),
			MetricName: aws.String(metric.Name),
			Dimensions: []types.Dimension{
				{
					Name:  aws.String("TableName"),
					Value: aws.String(tableName),
				},
			},
			StartTime:  aws.Time(timeParams["startTime"]),
			EndTime:    aws.Time(timeParams["endTime"]),
			Period:     period,
			Statistics: []types.Statistic{types.Statistic(metric.Statistic)},
		}

		result, err := cwClient.GetMetricStatistics(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("error getting %s: %v", metric.Name, err)
		}

		if len(result.Datapoints) > 0 {
			latest := result.Datapoints[0]
			for _, dp := range result.Datapoints {
				if dp.Timestamp.After(*latest.Timestamp) {
					latest = dp
				}
			}
			switch metric.Statistic {
			case "Average":
				metrics[metric.Name] = *latest.Average
			case "Sum":
				metrics[metric.Name] = *latest.Sum
			}
		} else {
			metrics[metric.Name] = 0.0
		}
	}

	return metrics, nil
}
