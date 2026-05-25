# ==============================================================================
# IAM Configuration for VPC Module
# ==============================================================================
# This file defines IAM permissions for VPC-related resources.

# TERRAFORM SERVICE ACCOUNT PERMISSIONS (for deployment):
#    The service account running Terraform needs these permissions:
#    - roles/compute.networkAdmin - Create/manage VPC, subnets, routes
#    - roles/compute.securityAdmin - Manage firewall rules
#    - roles/vpcaccess.admin - Create/manage VPC Access Connectors
#    - roles/iam.serviceAccountUser - If creating service accounts

# ==============================================================================
# Private Google Access Service Account Permissions
# ==============================================================================
# If you enable Private Google Access on subnets, instances in those subnets
# can access Google APIs using internal IP addresses. The service accounts
# used by those instances need appropriate API permissions.
#
# Example: Grant GCS access to instances in private subnets
# resource "google_storage_bucket_iam_member" "private_instance_gcs_access" {
#   bucket = "your-bucket-name"
#   role   = "roles/storage.objectViewer"
#   member = "serviceAccount:your-instance-service-account@project.iam.gserviceaccount.com"
# }

