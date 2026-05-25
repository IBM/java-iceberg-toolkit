# ==============================================================================
# Firewall Rules for VPC (FIXED for Private GKE)
# ==============================================================================

# ==============================================================================
# Allow Internal Communication (within subnet)
# ==============================================================================

resource "google_compute_firewall" "allow_internal" {
  count   = var.create_default_firewall_rules ? 1 : 0
  name    = "${var.naming_prefix}-vpc-allow-internal"
  project = var.project_id
  network = google_compute_network.ibm_byoc_vpc.name

  allow {
    protocol = "tcp"
  }

  allow {
    protocol = "udp"
  }

  source_ranges = [
    "10.0.0.0/24",     # nodes
    "10.160.128.0/17", # pods
    "10.88.0.0/20"     # services
  ]


  priority    = 1000
  description = "Allow internal traffic within VPC"
}

# ==============================================================================
# Nodes → GKE Control Plane (CRITICAL)
# ==============================================================================

resource "google_compute_firewall" "allow_nodes_to_master" {
  count   = var.create_default_firewall_rules ? 1 : 0
  name    = "${var.naming_prefix}-vpc-allow-nodes-to-master"
  project = var.project_id
  network = google_compute_network.ibm_byoc_vpc.name

  allow {
    protocol = "tcp"
    ports    = ["443"]
  }

  source_ranges      = ["10.0.0.0/24"]    # Node subnet
  destination_ranges = ["172.16.0.64/28"] # Master CIDR

  priority    = 1000
  description = "Allow nodes to communicate with GKE control plane"
}

# ==============================================================================
# GKE Control Plane → Nodes
# ==============================================================================

resource "google_compute_firewall" "allow_master_to_nodes" {
  count   = var.create_default_firewall_rules ? 1 : 0
  name    = "${var.naming_prefix}-vpc-allow-master-to-nodes"
  project = var.project_id
  network = google_compute_network.ibm_byoc_vpc.name

  allow {
    protocol = "tcp"
    ports    = ["443", "10250"]
  }

  source_ranges      = ["172.16.0.64/28"] # Master CIDR
  destination_ranges = ["10.0.0.0/24"]    # Node subnet

  priority    = 1000
  description = "Allow GKE master to communicate with nodes"
}

# ==============================================================================
# VPC Connector → GKE Control Plane (CRITICAL for Cloud Functions)
# ==============================================================================

resource "google_compute_firewall" "allow_vpc_connector_to_master" {
  count   = var.create_default_firewall_rules ? 1 : 0
  name    = "${var.naming_prefix}-vpc-allow-vpc-connector-to-master"
  project = var.project_id
  network = google_compute_network.ibm_byoc_vpc.name

  allow {
    protocol = "tcp"
    ports    = ["443", "10250"]
  }

  source_ranges      = ["10.8.0.0/28"]    # VPC Connector CIDR
  destination_ranges = ["172.16.0.64/28"] # Master CIDR

  priority    = 1000
  description = "Allow VPC Connector (Cloud Functions) to communicate with GKE control plane"
}

# ==============================================================================
# GKE Control Plane → VPC Connector (for webhooks/callbacks)
# ==============================================================================

resource "google_compute_firewall" "allow_master_to_vpc_connector" {
  count   = var.create_default_firewall_rules ? 1 : 0
  name    = "${var.naming_prefix}-vpc-allow-master-to-vpc-connector"
  project = var.project_id
  network = google_compute_network.ibm_byoc_vpc.name

  allow {
    protocol = "tcp"
    ports    = ["443", "8443", "9443"]
  }

  source_ranges      = ["172.16.0.64/28"] # Master CIDR
  destination_ranges = ["10.8.0.0/28"]    # VPC Connector CIDR

  priority    = 1000
  description = "Allow GKE master to communicate with VPC Connector for webhooks"
}

# ==============================================================================
# Allow NFS (if needed)
# ==============================================================================

resource "google_compute_firewall" "allow_nfs" {
  count   = var.create_default_firewall_rules ? 1 : 0
  name    = "${var.naming_prefix}-vpc-allow-nfs"
  project = var.project_id
  network = google_compute_network.ibm_byoc_vpc.name

  allow {
    protocol = "tcp"
    ports    = ["2049"]
  }

  source_ranges = ["10.0.0.0/24"]

  priority    = 1000
  description = "Allow NFS traffic"
}

# ==============================================================================
# Allow Egress HTTPS (REQUIRED for GKE, APIs, image pulls)
# ==============================================================================

resource "google_compute_firewall" "allow_egress_https" {
  count     = var.create_default_firewall_rules ? 1 : 0
  name      = "${var.naming_prefix}-vpc-allow-egress-https"
  project   = var.project_id
  network   = google_compute_network.ibm_byoc_vpc.name
  direction = "EGRESS"

  allow {
    protocol = "tcp"
    ports    = ["443"]
  }

  destination_ranges = ["0.0.0.0/0"]

  priority    = 1000
  description = "Allow outbound HTTPS traffic"
}

# ==============================================================================
# Deny All Ingress (Safe - GKE auto rules override this)
# ==============================================================================

resource "google_compute_firewall" "deny_all_ingress" {
  count   = var.create_default_firewall_rules ? 1 : 0
  name    = "${var.naming_prefix}-vpc-deny-all-ingress"
  project = var.project_id
  network = google_compute_network.ibm_byoc_vpc.name

  deny {
    protocol = "tcp"
  }

  deny {
    protocol = "udp"
  }

  source_ranges = ["0.0.0.0/0"]

  priority    = 65534
  description = "Deny all ingress traffic by default"
}