package services

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

func S3Metrics(ctx context.Context, cwClient *cloudwatch.Client, bucketName string, timeParams map[string]time.Time) (map[string]float64, error) {
	metrics := map[string]float64{}
	period := aws.Int32(86400) // S3 publishes storage metrics once per day

	// BucketSizeBytes can be broken down by StorageType
	storageTypes := []string{
		"StandardStorage",
		"StandardIAStorage",
		"ReducedRedundancyStorage",
		"GlacierStorage",
		"DeepArchiveStorage",
		"IntelligentTieringFAStorage",
		"IntelligentTieringIAStorage",
		"IntelligentTieringAAStorage",
		"IntelligentTieringAIAStorage",
		"IntelligentTieringDAAStorage",
	}

	var totalSize float64
	for _, storageType := range storageTypes {
		input := &cloudwatch.GetMetricStatisticsInput{
			Namespace:  aws.String("AWS/S3"),
			MetricName: aws.String("BucketSizeBytes"),
			Dimensions: []types.Dimension{
				{Name: aws.String("BucketName"), Value: aws.String(bucketName)},
				{Name: aws.String("StorageType"), Value: aws.String(storageType)},
			},
			StartTime:  aws.Time(timeParams["startTime"].AddDate(0, 0, -1)), // widen by 1 day
			EndTime:    aws.Time(timeParams["endTime"]),
			Period:     period,
			Statistics: []types.Statistic{types.StatisticAverage},
		}

		result, err := cwClient.GetMetricStatistics(ctx, input)
		if err != nil || len(result.Datapoints) == 0 {
			continue
		}

		// pick latest datapoint
		latest := result.Datapoints[0]
		for _, dp := range result.Datapoints {
			if dp.Timestamp.After(*latest.Timestamp) {
				latest = dp
			}
		}

		if latest.Average != nil {
			totalSize += *latest.Average
		}
	}

	// convert to MB
	metrics["BucketSizeMB"] = totalSize / (1024.0 * 1024.0)

	// --- NumberOfObjects ---
	input := &cloudwatch.GetMetricStatisticsInput{
		Namespace:  aws.String("AWS/S3"),
		MetricName: aws.String("NumberOfObjects"),
		Dimensions: []types.Dimension{
			{Name: aws.String("BucketName"), Value: aws.String(bucketName)},
			{Name: aws.String("StorageType"), Value: aws.String("AllStorageTypes")},
		},
		StartTime:  aws.Time(timeParams["startTime"].AddDate(0, 0, -1)), // widen by 1 day
		EndTime:    aws.Time(timeParams["endTime"]),
		Period:     period,
		Statistics: []types.Statistic{types.StatisticAverage},
	}

	result, err := cwClient.GetMetricStatistics(ctx, input)
	if err == nil && len(result.Datapoints) > 0 {
		latest := result.Datapoints[0]
		for _, dp := range result.Datapoints {
			if dp.Timestamp.After(*latest.Timestamp) {
				latest = dp
			}
		}
		if latest.Average != nil {
			metrics["NumberOfObjects"] = *latest.Average
		}
	} else {
		metrics["NumberOfObjects"] = 0.0
	}

	return metrics, nil
}
