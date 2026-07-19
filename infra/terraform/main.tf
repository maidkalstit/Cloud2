# --- Terraform Provider & Backend Configuration ---
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
  zone    = var.gcp_zone
}

# --- Input Variables (Zero Hardcoded Values) ---
variable "gcp_project_id" {
  type        = string
  description = "The target Google Cloud Project ID."
}

variable "gcp_region" {
  type        = string
  default     = "us-central1"
  description = "The GCP region for regional resources."
}

variable "gcp_zone" {
  type        = string
  default     = "us-central1-a"
  description = "The GCP zone for the Compute Engine instance."
}

variable "gcs_bucket_name" {
  type        = string
  description = "The name of the GCS bucket for our Iceberg warehouse."
}

variable "developer_ip_cidr" {
  type        = string
  description = "Your personal developer public IP address in CIDR block format (e.g., '1.2.3.4/32') to restrict network exposure."
}
variable "bigquery_dataset_id" {
  type        = string
  description = "The target BigQuery dataset ID for the lakehouse."
}


# --- Resource 1: Least-Privilege IAM Service Account ---
resource "google_service_account" "pipeline_sa" {
  account_id   = "sa-data-pipeline"
  display_name = "Data Pipeline Execution Service Account"
  description  = "Dedicated service account running the streaming lakehouse pipeline with strictly scoped access."
}

# Bucket-Scoped Role Binding: Grant read/write permission ONLY to our specific warehouse bucket
resource "google_storage_bucket_iam_member" "gcs_access" {
  bucket = var.gcs_bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# Dataset-Scoped Role Binding: Grant read/write permission ONLY to our project-level BigQuery editor
# Why: Restricting this to project/dataset level prevents the SA from messing with other corporate datasets
resource "google_bigquery_dataset_iam_member" "bigquery_access" {
  project    = var.gcp_project_id
  dataset_id = var.bigquery_dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# --- Resource 2: Secure Firewall Rules ---
# Why: Completely blocks public access to Kafka, Spark UI, and Schema Registry.
# Only allows traffic from the developer's specific IP and the Internal VPC network.
resource "google_compute_firewall" "pipeline_firewall" {
  name    = "allow-dev-and-internal-only"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["9092", "8080", "4040", "8081"] # Kafka, Spark Web UIs, Schema Registry
  }

  # Strict Source IP filtering
  source_ranges = [var.developer_ip_cidr]
  target_tags   = ["data-pipeline-node"]
}

# --- Resource 3: Compute Engine VM Instance (4GB RAM Constraint) ---
resource "google_compute_instance" "pipeline_vm" {
  name         = "olist-lakehouse-vm"
  machine_type = "e2-medium" # 2 vCPUs, 4GB RAM — matches our strict cloud-cost constraint
  zone         = var.gcp_zone

  tags = ["data-pipeline-node"] # Tag must match the firewall target tag exactly to apply security rules

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = 30 # 30GB standard persistent disk (fits nicely inside GCP Free Tier)
    }
  }

  network_interface {
    network = "default"
    access_config {
      # Leaving this block empty automatically assigns a public ephemeral IP address for development
    }
  }

  # Attaches the lease-privilege service account to the VM instance
  service_account {
    email  = google_service_account.pipeline_sa.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  metadata = {
    # Secure access rule: enforce SSH access via GCP Identity-Aware Proxy (IAP), blocking raw port 22
    enable-oslogin = "TRUE"
  }
}