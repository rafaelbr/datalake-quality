variable "environment" {
  default = "sb"
}

variable "region" {
  default = "us-east-1"
}

variable "profile" {}

provider "aws" {
  region = var.region
  profile = var.profile

  default_tags {
    tags = {
      project    = "datalake"
      managed-by = "terraform"
    }
  }
}