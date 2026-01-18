terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  # Configuration options
  project = "de-zoomcamp-5467"
  region  = "us-central1"
}

# resource "resource type" "<local-resource-name>"
resource "google_storage_bucket" "demo-bucket" {
  name          = "de-zoomcamp-5467-bucket"  # Should be globally unique
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}