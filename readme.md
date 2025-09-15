# Telegraws

**AWS Infrastructure Monitoring with Telegram Notifications**

Telegraws is a lightweight, serverless AWS monitoring scheduled Lambda function
that collects metrics from multiple AWS services and sends automated reports to
Telegram.

## Features

- **Serverless**: Deploys as AWS Lambda function with automatic scheduling.
- **Telegram Integration**: Sends formatted monitoring reports to Telegram.
- **IAC**: Automatically creates IAM roles, Lambda functions, and EventBridge
  schedules.
- **Local Development**: Test locally with `--local` flag before deployment.
- **Multi-Service Monitoring**: EC2, S3, ALB, CloudFront, DynamoDB, RDS, WAF,
  CloudWatch Logs, Cloudwatch Agents.
- **Smart Scheduling**: Hourly updates + daily reports.
- **Immutable Deployments**: Clean, reproducible deployments.

## Prerequisites

- **AWS CLI** installed and configured in the same region as your monitoring
  infrastructure.
- **Go 1.24+** installed.
- **Telegram Bot** token and chat ID.
- **AWS Resources** to monitor.

## Usage

```bash
git clone https://github.com/numerosnumerosnumeros/telegraws.git
cd telegraws
go mod tidy
cp config/config-template.json config/config.json
# Edit config/config.json with your settings
./build.sh --lambda # or --local
```

## Considerations

- Running `./build.sh --lambda` automatically detects if the function was
  already deployed and updates the existing function.
- Telegraws creates these resources: Lambda function, IAM role, Eventbridge
  rule. All prefixed by 'telegraws-'.
- lambdaCronExpression: EventBridge cron schedule (AWS format: Minutes Hours Day
  Month DayOfWeek Year). Requires raw expression, eg:
  `"lambdaCronExpression": "0 * * * ? *"`.
- timezone: Go time.LoadLocation compatible timezone.
- defaultPeriod: Hours to look back for regular reports (1 = last hour). Set to
  0 to only receive daily reports.
- dailyReportHour: Hour to send daily summary (respects timezone).
- CloudWatch Logs collection counts INFO/WARN/ERROR so structured logging is
  required.
- RDS monitoring currently supports Aurora engine.
- WAF monitoring collects WAFs metrics attached to ALB.
- CloudWatch Agent monitors disk_used_percent and mem_used_percent.
- Telegram has 4096 character limit per message.

## Metrics

- EC2: CPU Utilization (avg/max), Network I/O, Status Checks. If CloudWatch
  Agent: mem_used_percent, disk_used_percent.

- S3: (Daily Reports Only) Bucket Size, Objects Count.

- ALB: Request Count, Response Time, HTTP Status Codes, Healthy/Unhealthy Hosts,
  ALB Errors.

- CloudFront: Requests, Bytes Uploaded, Bytes Downloaded, Error Rates.

- DynamoDB: Request Count, Items Count, Throttles, Latency, Consumed Capacity,
  Error Counts.

- RDS/Aurora: Instance: CPU, Memory, Connections, Read/Write Latency. Cluster:
  Volume Size, IOPS.

- WAF: Allowed/Blocked Requests.

- CloudWatch Logs: INFO/WARN/ERROR log counts (requires structured logging).

## To-do

- Enhanced Metrics: Add comprehensive metric collection for all services. Get
  metrics dynamically using AWS CLI?
- Dynamic Metrics: User-configurable metrics selection. Separated daily and
  scheduled metrics.
- Enhanced AWS Support: ECS/EKS, Fargate, API Gateway.
- Multi-Resource: Multiple IDs per service type.
- Message Splitting: Handle Telegram 4096 character limit.
- Cross-Platform: Windows support for build script.
- Emoji Support: Optional emoji integration in messages.
- Architecture Options: x86_64 Lambda support.
- RDS Engines: Support for MySQL, PostgreSQL, SQL Server.
- Advanced WAF: Multiple WAF configurations.
