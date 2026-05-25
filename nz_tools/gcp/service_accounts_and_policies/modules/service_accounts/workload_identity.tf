# ==============================================================================
# Workload Identity Configuration for GCP Service Accounts Module
# ==============================================================================

resource "google_service_account_iam_member" "ibm_byoc_cluster_operator_workload_identity" {
  service_account_id = google_service_account.ibm_byoc_cluster_operator_sa.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[cluster-operator/cluster-operator-controller-manager]"
}