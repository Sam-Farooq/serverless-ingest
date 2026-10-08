output "bucket" {
  value       = aws_s3_bucket.raw.id
  description = "Upload NDJSON under the raw prefix to trigger an ingest."
}

output "queue_url" {
  value = aws_sqs_queue.ingest.id
}

output "dlq_url" {
  value       = aws_sqs_queue.ingest_dlq.id
  description = "Pass to the replay function as INGEST_DLQ_URL, already set in its environment."
}

output "function_names" {
  value = { for name, function in aws_lambda_function.ingest : name => function.function_name }
}

output "visibility_timeout" {
  value       = local.visibility_timeout
  description = "Six times the function timeout, which is what the event source mapping wants."
}
