#### Data Quality Lambda
resource "aws_iam_role" "quality_lambda_role" {
    name               = "${var.environment}-quality_lambda_role"
    assume_role_policy = jsonencode(
        {
            "Version" : "2012-10-17",
            "Statement" : [
                {
                    "Effect" : "Allow",
                    "Action" : "sts:AssumeRole",
                    "Principal" : {
                        "Service" : "lambda.amazonaws.com"
                    }
                }
            ]
        }
    )
}

resource "aws_iam_policy" "quality_lambda_policy" {
    name        = "${var.environment}-quality_lambda_policy"
    policy = jsonencode(
        {
            "Version" : "2012-10-17",
            "Statement" : [
                {
                    "Effect" : "Allow",
                    "Action" : [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    "Resource" : "arn:aws:logs:*:*:*"
                },
                {
                    "Effect" : "Allow",
                    "Action" : [
                        "s3:PutObject",
                        "s3:GetObject",
                        "s3:DeleteObject",
                        "s3:ListBucket"
                    ],
                    "Resource" : [
                        "arn:aws:s3:::${var.environment}-raw",
                        "arn:aws:s3:::${var.environment}-raw/*",
                        "arn:aws:s3:::${var.environment}-trusted",
                        "arn:aws:s3:::${var.environment}-trusted/*",
                        "arn:aws:s3:::${var.environment}-resources",
                        "arn:aws:s3:::${var.environment}-resources/*"
                    ]
                }
            ]
        }
    )
}

resource "aws_iam_role_policy_attachment" "quality_lambda_policy_attachment" {
    role       = aws_iam_role.quality_lambda_role.name
    policy_arn = aws_iam_policy.quality_lambda_policy.arn
}

### Glue Data Catalog Lambda
resource "aws_iam_role" "data_catalog_lambda_role" {
    name               = "${var.environment}-data-catalog-role"
    assume_role_policy = jsonencode(
        {
            "Version" : "2012-10-17",
            "Statement" : [
                {
                    "Effect" : "Allow",
                    "Action" : "sts:AssumeRole",
                    "Principal" : {
                        "Service" : "lambda.amazonaws.com"
                    }
                }
            ]
        }
    )
}

resource "aws_iam_policy" "data_catalog_lambda_policy" {
    name        = "${var.environment}-data-catalog-policy"
    policy = jsonencode(
        {
            "Version" : "2012-10-17",
            "Statement" : [
                {
                    "Effect" : "Allow",
                    "Action" : [
                        "iam:PassRole"
                    ],
                    "Resource" : [
                        "arn:aws:iam::*:role/data_crawler_role"
                    ]
                },
                {
                    "Effect" : "Allow",
                    "Action" : [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    "Resource" : "arn:aws:logs:*:*:*"
                },
                {
                    "Effect" : "Allow",
                    "Action" : [
                        "s3:PutObject",
                        "s3:GetObject",
                        "s3:DeleteObject",
                        "s3:ListBucket",
                        "s3:GetBucketLocation"
                    ],
                    "Resource" : [
                        "arn:aws:s3:::${var.environment}-trusted",
                        "arn:aws:s3:::${var.environment}-trusted/*",
                        "arn:aws:s3:::${var.environment}-resources",
                        "arn:aws:s3:::${var.environment}-resources/*",
                        "arn:aws:s3:::${var.environment}-awsresources",
                        "arn:aws:s3:::${var.environment}-awsresources/*"
                    ]
                },
                {
                    "Effect" : "Allow",
                    "Action" : [
                        "glue:CreateCrawler",
                        "glue:StartCrawler",
                        "glue:GetCrawler",
                        "glue:GetCrawlers",
                        "glue:GetDatabases",
                        "glue:GetDatabase",
                        "glue:CreateDatabase",
                        "glue:GetTable",
                        "glue:GetTables",
                        "glue:CreateTable",
                        "glue:UpdateTable",
                        "glue:DeleteTable"
                    ],
                    "Resource" : "*"
                },
                {
                    "Effect" : "Allow",
                    "Action" : [
                        "glue:GetPartition",
                        "glue:GetPartitions",
                        "glue:BatchCreatePartition",
                        "glue:BatchDeletePartition",
                        "glue:BatchUpdatePartition"
                    ],
                    "Resource" : "*"
                },
                {
                    "Effect" : "Allow",
                    "Action" : [
                        "athena:StartQueryExecution",
                        "athena:GetQueryExecution",
                        "athena:GetQueryResults",
                        "athena:StopQueryExecution"
                    ],
                    "Resource" : "*"
                }
            ]
        }
    )
}

resource "aws_iam_role_policy_attachment" "data_crawler_lambda_policy_attachment" {
    role       = aws_iam_role.data_catalog_lambda_role.name
    policy_arn = aws_iam_policy.data_catalog_lambda_policy.arn
}
