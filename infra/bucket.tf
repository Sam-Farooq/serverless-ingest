resource "aws_s3_bucket" "raw" {
  bucket = "${var.name_prefix}-raw-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket = aws_s3_bucket.raw.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# Versioning is on because the ledger is keyed on the etag. An object
# overwritten in place is a second ingest, and without versioning the bytes
# that produced the first one are gone before anyone asks what they were.
resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "tier-and-expire-raw"
    status = "Enabled"

    filter {
      prefix = var.raw_prefix
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    # The raw object is only needed while a rebuild is plausible. After 365
    # days the rows are in Postgres and the ledger says where they came from.
    expiration {
      days = 365
    }

    noncurrent_version_expiration {
      noncurrent_days = 90
    }

    # Without this an aborted multipart upload is billed forever and appears
    # in no listing.
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# The notification depends on the queue policy, not just the queue. S3
# validates that it can send to the target when the configuration is saved, so
# applying these in the wrong order fails with a permissions error that reads
# like a bug in the policy rather than a race.
resource "aws_s3_bucket_notification" "raw_to_queue" {
  bucket = aws_s3_bucket.raw.id

  queue {
    queue_arn     = aws_sqs_queue.ingest.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = var.raw_prefix
  }

  depends_on = [aws_sqs_queue_policy.ingest_from_s3]
}
