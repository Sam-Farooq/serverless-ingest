locals {
  # The event source mapping will not accept a visibility timeout shorter than
  # the function timeout, and AWS asks for six times it. The reason is the
  # retry: a message that is being processed must stay invisible for every
  # attempt the invocation might make, or it is handed to a second invocation
  # while the first one is still working and loaded twice.
  visibility_timeout = var.function_timeout * 6
}

resource "aws_sqs_queue" "ingest" {
  name                       = "${var.name_prefix}-objects"
  visibility_timeout_seconds = local.visibility_timeout
  message_retention_seconds  = 345600 # 4 days
  sqs_managed_sse_enabled    = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.ingest_dlq.arn
    maxReceiveCount     = var.max_receive_count
  })
}

resource "aws_sqs_queue" "ingest_dlq" {
  name                    = "${var.name_prefix}-objects-dlq"
  sqs_managed_sse_enabled = true

  # The maximum. A message here is waiting for a person, and a person who is
  # on holiday is a fortnight, not four days.
  message_retention_seconds = 1209600
}

# Without this the console redrive button and the replay handler both fail
# with an access denied that names neither queue.
resource "aws_sqs_queue_redrive_allow_policy" "dlq_back_to_ingest" {
  queue_url = aws_sqs_queue.ingest_dlq.id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.ingest.arn]
  })
}

data "aws_iam_policy_document" "ingest_from_s3" {
  statement {
    sid    = "AllowS3Notifications"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }

    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.ingest.arn]

    # Both conditions, not one. SourceArn alone can be satisfied by a bucket of
    # the same name in somebody else's account, since bucket ARNs carry no
    # account id.
    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_s3_bucket.raw.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_sqs_queue_policy" "ingest_from_s3" {
  queue_url = aws_sqs_queue.ingest.id
  policy    = data.aws_iam_policy_document.ingest_from_s3.json
}
