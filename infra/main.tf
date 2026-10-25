terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}
provider "aws" { region = var.region }
variable "region" { default = "us-east-1" }
variable "stream_name" { default = "clickstream-dev" }
variable "shard_count" {
  default = 8
  validation {
    condition     = var.shard_count >= 1
    error_message = "Use at least one shard."
  }
}
resource "aws_kinesis_stream" "events" {
  name                = var.stream_name
  shard_count         = var.shard_count
  retention_period    = 48
  encryption_type     = "KMS"
  kms_key_id          = "alias/aws/kinesis"
  shard_level_metrics = ["IncomingRecords", "WriteProvisionedThroughputExceeded", "ReadProvisionedThroughputExceeded", "IteratorAgeMilliseconds"]
  tags                = { Project = "clickstream-lakehouse", Environment = "dev" }
}
data "aws_iam_policy_document" "producer" {
  statement {
    actions   = ["kinesis:PutRecords", "kinesis:DescribeStreamSummary"]
    resources = [aws_kinesis_stream.events.arn]
  }
}
data "aws_iam_policy_document" "consumer" {
  statement {
    actions   = ["kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:ListShards", "kinesis:GetShardIterator", "kinesis:GetRecords"]
    resources = [aws_kinesis_stream.events.arn]
  }
}
resource "aws_iam_policy" "producer" {
  name   = "${var.stream_name}-producer"
  policy = data.aws_iam_policy_document.producer.json
}
resource "aws_iam_policy" "consumer" {
  name   = "${var.stream_name}-consumer"
  policy = data.aws_iam_policy_document.consumer.json
}
output "stream_arn" { value = aws_kinesis_stream.events.arn }
output "producer_policy_arn" { value = aws_iam_policy.producer.arn }
output "consumer_policy_arn" { value = aws_iam_policy.consumer.arn }
