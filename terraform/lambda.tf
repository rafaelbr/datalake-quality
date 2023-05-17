variable "account_id" {}

variable "config_bucket" {}
variable "ge_path" {}
variable "config_path" {}
variable "target_bucket" {}
variable "trusted_database" {}

### Data Quality Lambda
resource "aws_lambda_function" "quality_function" {
  function_name = "${var.environment}-quality"
  role          = aws_iam_role.quality_lambda_role.arn
  image_uri = "${var.account_id}.dkr.ecr.${var.region}.amazonaws.com/${var.environment}-dataquality:latest"
  package_type = "Image"
  architectures = ["x86_64"]
  timeout = 300
  memory_size = 256
  environment {
    variables = {
      CONFIG_BUCKET = var.config_bucket,
      CONFIG_PATH = var.config_path,
      GE_PATH = var.ge_path,
      TARGET_BUCKET = var.target_bucket
    }
  }
}

resource "aws_lambda_permission" "quality-lambda-permission" {
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.quality_function.function_name
  principal     = "s3.amazonaws.com"
  depends_on = [aws_lambda_function.quality_function]
}

resource "aws_s3_bucket_notification" "quality-trigger" {
  bucket = "${var.environment}-raw"
  lambda_function {
      lambda_function_arn = aws_lambda_function.quality_function.arn
      events              = ["s3:ObjectCreated:*"]
      filter_prefix       = ""
  }
  depends_on = [aws_lambda_permission.quality-lambda-permission]
}

### Data Catalog Lambda
data "archive_file" "data_catalog" {
  type        = "zip"
  output_path = "data_catalog.zip"
  source {
    filename = "app.py"
    content = file("${path.module}/../lambda/data_catalog.py")
  }
}

resource "aws_lambda_function" "catalog_function" {
  function_name = "${var.environment}-catalog"
  role          = aws_iam_role.data_catalog_lambda_role.arn
  filename = "data_catalog.zip"
  handler = "app.lambda_handler"
  source_code_hash = data.archive_file.data_catalog.output_base64sha256
  runtime = "python3.9"
  timeout = 300
  memory_size = 256

  environment {
    variables = {
      CONFIG_BUCKET = var.config_bucket,
      CONFIG_PATH = var.config_path,
      TRUSTED_DATABASE = var.trusted_database,
      TRUSTED_BUCKET = "${var.environment}-trusted",
      AWS_RESOURCES_BUCKET = "${var.environment}-awsresources"
    }
  }
}

resource "aws_lambda_permission" "catalog-lambda-permission" {
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.catalog_function.function_name
  principal     = "s3.amazonaws.com"
  depends_on = [aws_lambda_function.catalog_function]
}

resource "aws_s3_bucket_notification" "catalog-trigger" {
  bucket = "${var.environment}-trusted"
  lambda_function {
      lambda_function_arn = aws_lambda_function.catalog_function.arn
      events              = ["s3:ObjectCreated:*"]
      filter_prefix       = ""
  }
  depends_on = [aws_lambda_permission.catalog-lambda-permission]
}