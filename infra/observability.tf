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

# The two alarms above read AWS/SQS, which SQS publishes itself at a fixed
# five minute resolution. Everything below reads the ServerlessIngest
# namespace, which the functions publish as embedded metric format log lines.
#
# That difference is the detection floor. An EMF metric does not exist until
# the log line has been delivered and parsed, so no alarm below can fire
# faster than log ingestion, whatever period it is given. The README states
# the floor; the periods here are chosen on top of it rather than in ignorance
# of it. The two SQS alarms are not subject to it, which is why the dead
# letter queue, the failure most likely to need a fast answer, is watched
# there and not here.
#
# A metric that has never been published does not exist, so a fresh deployment
# would leave these in INSUFFICIENT_DATA rather than OK.
# treat_missing_data = "notBreaching" is what keeps that from paging someone
# on the day of the install. The one exception is the silence alarm at the
# bottom, where missing data is the alarm.
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

# The one alarm here that fires on an absence rather than a value.
#
# The failure this exists for is a queue policy that stopped accepting
# s3.amazonaws.com. Nothing breaks loudly: objects keep landing in the bucket,
# no notification is ever accepted, no invocation happens, and every alarm
# above stays in OK because the metric they watch is simply never published.
# The reconcile job would find it, up to a day later. This finds it inside
# var.quiet_period_seconds.
#
# treat_missing_data = "breaching" is the whole point and is the opposite of
# every other alarm in this file. The period is a variable because it is a
# property of the producer's schedule and not of this code; see the comment on
# quiet_period_seconds for what the two bounds on it are.
resource "aws_cloudwatch_metric_alarm" "ingest_silent" {
  alarm_name          = "${var.name_prefix}-ingest-silent"
  comparison_operator = "LessThanOrEqualToThreshold"
  threshold           = 0
  evaluation_periods  = 1
  period              = var.quiet_period_seconds
  namespace           = "ServerlessIngest"
  metric_name         = "messages_received"
  statistic           = "Sum"
  treat_missing_data  = "breaching"

  alarm_description = join(" ", [
    "No SQS messages reached the load handler for",
    "${var.quiet_period_seconds}s.",
    "Check the queue policy still accepts s3.amazonaws.com, then the bucket",
    "notification configuration, before looking at the function."
  ])

  dimensions = {
    Handler = "load"
  }
}
