locals {
  data_lake_bucket = "dtc-de-zc-data-lake"
}

variable "accout_id" {
  description = "Your AWS ACCOUNT ID"
}

variable "region" {
  description = "Region for AWS resources."
  default = "eu-central-1"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

# client
variable "client" {
  description = "Client name"
  type = string
  default = "dtc-de-zc"
}

# REDSHIFT_CLUSTER
variable "REDSHIFT_CLUSTER" {
  description = "cluster_identifier - (Required) The Cluster Identifier. Must be a lower case string."
  type = string
  default = "trips-data-all"
}

# REDSHIFT_NODE_TYPE
variable "REDSHIFT_NODE_TYPE" {
  description = "node_type - (Required) The node type to be provisioned for the cluster."
  type = string
  default = "dc2.large"
}
 
# REDSHIFT_NUMBER_OF_NODES
variable "REDSHIFT_NUMBER_OF_NODES" {
  description = "number_of_nodes - (Required) The number of compute nodes in the cluster."
  type = string
  default = "1"
}
