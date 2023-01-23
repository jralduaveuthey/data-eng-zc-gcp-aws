terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    aws = {
      source  = "hashicorp/aws"
    }
  }
}

# Configure the AWS Provider
provider "aws" {
  region = var.region
   // You can use assume_role, access_key and secret_key as well
}

resource "aws_s3_bucket" "data-lake-bucket" {
  bucket = "${local.data_lake_bucket}-${var.accout_id}"
  tags = {
    client      = var.client
  }
  versioning {
    enabled     = true
  }
  lifecycle_rule { 
    id      = "delete_old_objects"
    enabled = true
    prefix = "logs/"
    tags = {
      autoclean = "true"
      rule = "log"
    }
    expiration {
      days = 30
    }
  }

  force_destroy = true
}

resource "aws_redshift_cluster" "cluster" {
  cluster_identifier = var.REDSHIFT_CLUSTER
  master_username    = "exampleuser"
  master_password    = "Mustbe8characters"
  node_type          = var.REDSHIFT_NODE_TYPE
  cluster_type       = "single-node"
  number_of_nodes = var.REDSHIFT_NUMBER_OF_NODES
  tags = {
    client      = var.client
  }
}
