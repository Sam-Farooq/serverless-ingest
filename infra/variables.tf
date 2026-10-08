variable "region" {
  type    = string
  default = "eu-west-1"
}

variable "name_prefix" {
  type        = string
  default     = "ingest"
  description = "Prefixes every resource name, so two copies can share an account."
}

variable "raw_prefix" {
  type        = string
  default     = "raw/"
  description = "Only objects under this prefix trigger a notification."
}

variable "package_path" {
  type        = string
  default     = "../build/ingest.zip"
  description = "Built by scripts/package.sh. Run it before plan."
}

variable "function_timeout" {
  type    = number
  default = 60

  validation {
    condition     = var.function_timeout >= 10 && var.function_timeout <= 300
    error_message = "A batch of 10 objects needs more than 10 seconds and should never need 300."
  }
}

variable "batch_size" {
  type        = number
  default     = 10
  description = "Objects per invocation. Each one is a separate transaction."

  validation {
    condition     = var.batch_size >= 1 && var.batch_size <= 10
    error_message = "Above 10 a single slow object delays the whole batch past the visibility timeout."
  }
}

variable "max_receive_count" {
  type        = number
  default     = 5
  description = "Deliveries of one message before it goes to the dead letter queue."
}

variable "reserved_concurrency" {
  type        = number
  default     = 20
  description = <<-EOT
    The database connection limit, expressed as a Lambda setting. Each warm
    execution environment holds one Postgres connection, so this number is the
    worst case the instance has to accept from this function alone. Raising it
    without raising max_connections is how the ingest path takes the database
    down for everything else.
  EOT
}

variable "database_url" {
  type        = string
  sensitive   = true
  description = "Postgres DSN. Comes from Secrets Manager, never from a tfvars file in git."
}

variable "subnet_ids" {
  type        = list(string)
  description = "Private subnets with a route to RDS. Lambda needs no public address."
}

variable "security_group_ids" {
  type        = list(string)
  description = "Must be allowed outbound to the RDS port."
}

variable "log_retention_days" {
  type        = number
  default     = 30
  description = "Lambda creates its own log group with no expiry if Terraform does not."
}

variable "reconcile_schedule" {
  type        = string
  default     = "cron(15 2 * * ? *)"
  description = "02:15 UTC daily, after the nightly upstream exports have landed."
}
