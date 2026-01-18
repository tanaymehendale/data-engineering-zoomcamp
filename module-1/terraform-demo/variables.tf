variable "credentials" {
  description = "My Credentials"
  default     = "my-creds.json" # filepath
}

variable "project" {
  description = "Project"
  default     = "de-zoomcamp-5467"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "region" {
  description = "Project Region"
  default     = "us-central1"
}


variable "bq_dataset_name" {
  description = "My BigQuery Dataset name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "de-zoomcamp-5467-bucket"
}


variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
