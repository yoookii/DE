variable "project" {
  description = "project name"
  default = "inlaid-reactor-427616-m8"
}

variable "credentials" {
  description = "project credentials"
  default = "./key/my-creds.json"
}

variable "region" {
  description = "project region"
  default = "us-central1"
}

variable "location" {
  description = "project location"
  default = "US"
}

variable "bq_dataset_name" {
  description = "bigquery dataset name"
  default = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "storage bucket name"
  default = "terraform-demo-427616-bucket"
}

variable "gcs_storage_class" {
  description = "bucket storage class"
  default = "STANDARD"
}