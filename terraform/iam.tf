# IAM user for pipeline to access S3
resource "aws_iam_user" "pipeline_user" {
  name = "${var.project_name}-pipeline-user"

  tags = {
    Name = "Spotify Pipeline User"
  }
}

# Access key for programmatic access
resource "aws_iam_access_key" "pipeline_user_key" {
  user = aws_iam_user.pipeline_user.name
}

# Policy for S3 access
resource "aws_iam_user_policy" "pipeline_s3_policy" {
  name = "${var.project_name}-s3-access"
  user = aws_iam_user.pipeline_user.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Resource = [
          aws_s3_bucket.raw_data.arn,
          "${aws_s3_bucket.raw_data.arn}/*"
        ]
      }
    ]
  })
}