# ==============================================================================
# Service Accounts and Policies - Provider Configuration
# ==============================================================================

provider "google" {
  project = var.project_id
  region  = var.region
}