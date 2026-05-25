# ==============================================================================
# Service Accounts and Policies - Root Module
# ==============================================================================
# This root module manages GCP service accounts and their IAM policies
# for BYOC (Bring Your Own Cloud) deployments.
# ==============================================================================

terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# ==============================================================================
# Service Accounts Module
# ==============================================================================

module "service_accounts" {
  source = "./modules/service_accounts"

  # Required variables
  project_id                       = var.project_id
  region                           = var.region
  dataplane_id                     = var.dataplane_id
  dataplane_crn                    = var.dataplane_crn
  terraform_source_bucket          = var.terraform_source_bucket
  infra_manager_artifacts_bucket   = var.infra_manager_artifacts_bucket

  # Optional variables
  byoc_engine_label = var.byoc_engine_label
}