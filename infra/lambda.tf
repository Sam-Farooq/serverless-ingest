locals {
  common_environment = {
    INGEST_DATABASE_URL    = var.database_url
    INGEST_MAIN_QUEUE_URL  = aws_sqs_queue.ingest.id
    INGEST_DLQ_URL         = aws_sqs_queue.ingest_dlq.id
    INGEST_BUCKET          = aws_s3_bucket.raw.id
    INGEST_PREFIX          = var.raw_prefix
    INGEST_METRICS_ENABLED = "true"
  }

  functions = {
    load = {
      handler     = "handlers.load.handler"
      timeout     = var.function_timeout
      memory      = 512
      concurrency = var.reserved_concurrency
    }
    replay = {
      handler     = "handlers.replay.handler"
      timeout     = 120
      memory      = 256
      concurrency = 1
    }
    reconcile = {
      handler = "handlers.reconcile.handler"
      # Listing a large prefix and reading the ledger is slower than loading
      # one object, and it runs once a day.
      timeout     = 300
      memory      = 512
      concurrency = 1
    }
  }
}

resource "aws_lambda_function" "ingest" {
  for_each = local.functions

  function_name = "${var.name_prefix}-${each.key}"
  role          = aws_iam_role.ingest.arn
  handler       = each.value.handler
  runtime       = "python3.11"
  architectures = ["arm64"]

  filename         = var.package_path
  source_code_hash = filebase64sha256(var.package_path)

  timeout     = each.value.timeout
  memory_size = each.value.memory

  # Each warm environment holds one Postgres connection, so this is the
  # connection cap. RDS Proxy would remove the cap and add a component with
  # its own failure modes and its own bill; at this concurrency the cap is
  # cheaper than the proxy.
  reserved_concurrent_executions = each.value.concurrency

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = var.security_group_ids
  }

  environment {
    variables = local.common_environment
  }

  # Terraform creates the function before the log group otherwise, the
  # function writes first, and the retention set below never applies to the
  # group that already exists.
  depends_on = [
    aws_cloudwatch_log_group.ingest,
    aws_iam_role_policy_attachment.vpc_access,
  ]
}

resource "aws_lambda_event_source_mapping" "objects" {
  event_source_arn = aws_sqs_queue.ingest.arn
  function_name    = aws_lambda_function.ingest["load"].arn
  enabled          = true

  batch_size = var.batch_size

  # Wait up to 5 seconds to fill a batch. Objects arrive in bursts after an
  # upstream export, and ten objects in one invocation reuse one connection.
  maximum_batching_window_in_seconds = 5

  # The whole point. Without this the handler's return value is ignored and
  # one bad object sends the other nine round again.
  function_response_types = ["ReportBatchItemFailures"]

  scaling_config {
    # Half the reserved concurrency. The mapping scales by adding pollers, and
    # a mapping allowed to reach the reserved limit starves the retries, which
    # are then throttled and counted as failures.
    maximum_concurrency = max(floor(var.reserved_concurrency / 2), 2)
  }
}

# replay is invoked by a person, reconcile by the schedule. Neither has a
# trigger here, and that is deliberate for replay: a lever with an automatic
# arm is not a lever.
