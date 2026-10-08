resource "aws_cloudwatch_log_group" "ingest" {
  for_each = local.functions

  name              = "/aws/lambda/${var.name_prefix}-${each.key}"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_event_rule" "reconcile" {
  name                = "${var.name_prefix}-reconcile"
  description         = "Daily comparison of the bucket against the ingest ledger"
  schedule_expression = var.reconcile_schedule
}

resource "aws_cloudwatch_event_target" "reconcile" {
  rule  = aws_cloudwatch_event_rule.reconcile.name
  arn   = aws_lambda_function.ingest["reconcile"].arn
  input = jsonencode({})
}

resource "aws_lambda_permission" "reconcile_from_events" {
  statement_id  = "AllowExecutionFromCloudWatchSchedule"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest["reconcile"].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.reconcile.arn
}

# Anything in the dead letter queue is already five failed deliveries old, so
# the threshold is zero and the period is short.
resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  alarm_name          = "${var.name_prefix}-dlq-not-empty"
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  evaluation_periods  = 1
  period              = 300
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  statistic           = "Maximum"
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.ingest_dlq.name
  }
}

# Depth says how much is waiting, age says whether anything is moving. A queue
# holding 400 messages that are all 10 seconds old is fine. One message 30
# minutes old is not.
resource "aws_cloudwatch_metric_alarm" "ingest_backlog_age" {
  alarm_name          = "${var.name_prefix}-oldest-message-age"
  comparison_operator = "GreaterThanThreshold"
  threshold           = 1800
  evaluation_periods  = 2
  period              = 300
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateAgeOfOldestMessage"
  statistic           = "Maximum"
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.ingest.name
  }
}

# The next three read metrics the functions publish themselves, in embedded
# metric format. A metric that has never been published does not exist, so a
# fresh deployment would leave these in INSUFFICIENT_DATA rather than OK.
# treat_missing_data = "notBreaching" is what keeps that from paging someone
# on the day of the install.
resource "aws_cloudwatch_metric_alarm" "rows_rejected" {
  alarm_name          = "${var.name_prefix}-rows-rejected"
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  evaluation_periods  = 1
  period              = 900
  namespace           = "ServerlessIngest"
  metric_name         = "rows_rejected"
  statistic           = "Sum"
  treat_missing_data  = "notBreaching"

  alarm_description = "A producer changed shape, or a sensor is sending impossible values."

  dimensions = {
    Handler = "load"
  }
}

resource "aws_cloudwatch_metric_alarm" "unreconciled_objects" {
  alarm_name          = "${var.name_prefix}-unreconciled-objects"
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  evaluation_periods  = 1
  period              = 86400
  namespace           = "ServerlessIngest"
  metric_name         = "objects_unreconciled"
  statistic           = "Maximum"
  treat_missing_data  = "notBreaching"

  alarm_description = "An object reached the bucket and no invocation ever loaded it."

  dimensions = {
    Handler = "reconcile"
    Bucket  = aws_s3_bucket.raw.id
  }
}

# Skipped objects are normal at a low rate, since a redelivery after a partial
# batch failure is exactly what the ledger is for. A sustained rate means the
# visibility timeout is short for the batch size and work is being done twice.
resource "aws_cloudwatch_metric_alarm" "objects_skipped" {
  alarm_name          = "${var.name_prefix}-objects-skipped-often"
  comparison_operator = "GreaterThanThreshold"
  threshold           = 50
  evaluation_periods  = 3
  period              = 3600
  namespace           = "ServerlessIngest"
  metric_name         = "objects_skipped"
  statistic           = "Sum"
  treat_missing_data  = "notBreaching"

  dimensions = {
    Handler = "load"
  }
}
