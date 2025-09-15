package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"telegraws/config"
	"telegraws/services"
	"telegraws/utils"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/aws-sdk-go-v2/service/wafv2"

	"go.uber.org/zap"
)

func getAccountID(ctx context.Context, cfg aws.Config) (string, error) {
	if acct := os.Getenv("AWS_ACCOUNT_ID"); acct != "" {
		return acct, nil
	}

	// Fallback: call STS
	client := sts.NewFromConfig(cfg)
	output, err := client.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", fmt.Errorf("failed to get account ID: %w", err)
	}
	return *output.Account, nil
}

func logic(ctx context.Context) error {
	appConfig, err := config.LoadEmbeddedConfig()
	if err != nil {
		return fmt.Errorf("failed to load app config: %v", err)
	}

	timeParams, err := appConfig.GetTimeParams()
	if err != nil {
		return fmt.Errorf("failed to calculate time parameters: %v", err)
	}
	if timeParams == nil {
		utils.Logger.Info("Skipping execution: outside of daily report hour and no defaultPeriod configured")
		return nil
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("unable to load SDK config: %v", err)
	}

	logsClient := cloudwatchlogs.NewFromConfig(awsCfg)
	cwClient := cloudwatch.NewFromConfig(awsCfg)
	wafClient := wafv2.NewFromConfig(awsCfg)
	dynamoClient := dynamodb.NewFromConfig(awsCfg)

	// CloudFront requires us-east-1 clients
	cfCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion("us-east-1"))
	if err != nil {
		return fmt.Errorf("unable to load SDK config for us-east-1: %v", err)
	}
	cwCfClient := cloudwatch.NewFromConfig(cfCfg)
	wafCfClient := wafv2.NewFromConfig(cfCfg)

	// Resolve AWS account ID
	accountID, err := getAccountID(ctx, awsCfg)
	if err != nil {
		return fmt.Errorf("failed to resolve AWS account ID: %w", err)
	}

	allMetrics := make(map[string]any)

	timeParamsMap := map[string]time.Time{
		"startTime": timeParams.StartTime,
		"endTime":   timeParams.EndTime,
	}

	if appConfig.Services.EC2.Enabled {
		ec2Metrics, err := services.EC2Metrics(ctx, cwClient, appConfig.Services.EC2.InstanceID, timeParamsMap)
		if err != nil {
			utils.Logger.Error("Failed to get EC2 metrics", zap.Error(err))
		} else {
			allMetrics["ec2"] = ec2Metrics
		}
	}

	if appConfig.Services.S3.Enabled && timeParams.IsDailyReport {
		s3Metrics, err := services.S3Metrics(ctx, cwClient, appConfig.Services.S3.BucketName, timeParamsMap)
		if err != nil {
			utils.Logger.Error("Failed to get S3 metrics", zap.Error(err))
		} else {
			allMetrics["s3"] = s3Metrics
		}
	}

	if appConfig.Services.ALB.Enabled {
		albMetrics, err := services.ALBMetrics(ctx, cwClient, appConfig.Services.ALB.ALBName, timeParamsMap)
		if err != nil {
			utils.Logger.Error("Failed to get ALB metrics", zap.Error(err))
		} else {
			allMetrics["alb"] = albMetrics
		}
	}

	if appConfig.Services.CloudFront.Enabled {
		cloudFrontMetrics, err := services.CloudFrontMetrics(ctx, cwCfClient, appConfig.Services.CloudFront.DistributionID, timeParamsMap)
		if err != nil {
			utils.Logger.Error("Failed to get CloudFront metrics", zap.Error(err))
		} else {
			allMetrics["cloudfront"] = cloudFrontMetrics
		}
	}

	if appConfig.Services.CloudWatchAgent.Enabled {
		cwAgentMetrics, err := services.CWAgentMetrics(ctx, cwClient, appConfig.Services.CloudWatchAgent.InstanceID, timeParamsMap)
		if err != nil {
			utils.Logger.Error("Failed to get CloudWatch Agent metrics", zap.Error(err))
		} else {
			allMetrics["cloudwatchAgent"] = cwAgentMetrics
		}
	}

	if appConfig.Services.CloudWatchLogs.Enabled {
		logMetrics := make(map[string]any)
		for _, logGroupName := range appConfig.Services.CloudWatchLogs.LogGroupNames {
			logCounts, err := services.CWLogs(ctx, logsClient, logGroupName, timeParamsMap)
			if err != nil {
				utils.Logger.Error("Failed to get CloudWatch Logs metrics",
					zap.Error(err),
					zap.String("logGroup", logGroupName),
				)
				continue
			}
			logMetrics[logGroupName] = logCounts
		}
		if len(logMetrics) > 0 {
			allMetrics["cloudwatchLogs"] = logMetrics
		}
	}

	if appConfig.Services.WAF.Enabled {
		scope := appConfig.Services.WAF.Scope
		if scope == "" {
			scope = "REGIONAL"
		}

		var wafClientToUse *wafv2.Client
		var cwClientToUse *cloudwatch.Client

		if scope == "CLOUDFRONT" {
			wafClientToUse = wafCfClient
			cwClientToUse = cwCfClient // 🔑 use us-east-1 CW client
		} else {
			wafClientToUse = wafClient
			cwClientToUse = cwClient
		}

		if wafMetrics, err := services.WAFMetrics(
			ctx,
			wafClientToUse,
			cwClientToUse, // 🔑 now correct per scope
			appConfig.Services.WAF.WebACLID,
			appConfig.Services.WAF.WebACLName,
			scope,
			timeParamsMap,
			accountID,
			appConfig.Services.CloudFront.DistributionID,
		); err != nil {
			utils.Logger.Error("Failed to get WAF metrics", zap.Error(err))
		} else {
			allMetrics["waf"] = wafMetrics
		}
	}

	if appConfig.Services.DynamoDB.Enabled {
		dynamoMetrics := make(map[string]any)
		for _, tableName := range appConfig.Services.DynamoDB.TableNames {
			tableMetrics, err := services.DynamoDBMetrics(ctx, cwClient, dynamoClient, timeParamsMap, tableName)
			if err != nil {
				utils.Logger.Error("Failed to get DynamoDB metrics",
					zap.Error(err),
					zap.String("tableName", tableName),
				)
				continue
			}
			dynamoMetrics[tableName] = tableMetrics
		}
		if len(dynamoMetrics) > 0 {
			allMetrics["dynamodb"] = dynamoMetrics
		}
	}

	if appConfig.Services.RDS.Enabled {
		rdsMetrics, err := services.RDSMetrics(ctx, cwClient, appConfig.Services.RDS.ClusterID, appConfig.Services.RDS.DBInstanceIdentifier, timeParamsMap)
		if err != nil {
			utils.Logger.Error("Failed to get RDS metrics", zap.Error(err))
		} else {
			allMetrics["rds"] = rdsMetrics
		}
	}

	message := utils.BuildMessage(appConfig, timeParams, allMetrics)

	err = utils.SendToTelegram(ctx, message, appConfig.Global.Telegram.BotToken, appConfig.Global.Telegram.ChatID)
	if err != nil {
		utils.Logger.Error("Failed to send Telegram message", zap.Error(err))
		return err
	}

	return nil
}

func main() {
	ctx := context.Background()
	defer utils.Logger.Sync()

	if os.Getenv("AWS_LAMBDA_RUNTIME_API") != "" {
		lambda.Start(func(ctx context.Context) error {
			return logic(ctx)
		})
	} else {
		if err := logic(ctx); err != nil {
			log.Printf("Error executing logic: %v", err)
		}
	}
}
