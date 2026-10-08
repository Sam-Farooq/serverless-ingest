data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ingest" {
  name               = "${var.name_prefix}-lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

# Managed, because writing a correct ENI policy by hand is a day nobody gets
# back. This is the one place a managed policy is the right answer.
resource "aws_iam_role_policy_attachment" "vpc_access" {
  role       = aws_iam_role.ingest.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

data "aws_iam_policy_document" "ingest" {
  statement {
    sid    = "ReadRawObjects"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
    ]

    resources = ["${aws_s3_bucket.raw.arn}/${var.raw_prefix}*"]
  }

  # Separate statement because ListBucket is granted on the bucket and not on
  # the objects, and a single statement covering both would silently widen the
  # object permission to the whole bucket.
  statement {
    sid       = "ListRawPrefixForReconcile"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.raw.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${var.raw_prefix}*"]
    }
  }

  statement {
    sid    = "ConsumeTheIngestQueue"
    effect = "Allow"

    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
    ]

    resources = [aws_sqs_queue.ingest.arn]
  }

  statement {
    sid       = "SendReplayedMessagesBack"
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.ingest.arn]
  }

  statement {
    sid    = "DrainTheDeadLetterQueue"
    effect = "Allow"

    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
    ]

    resources = [aws_sqs_queue.ingest_dlq.arn]
  }
}

resource "aws_iam_role_policy" "ingest" {
  name   = "${var.name_prefix}-access"
  role   = aws_iam_role.ingest.id
  policy = data.aws_iam_policy_document.ingest.json
}

# No cloudwatch:PutMetricData anywhere in this role. The functions publish
# metrics in embedded metric format, which is a log write, so the permission
# the managed logging policy already grants is the only one needed.
