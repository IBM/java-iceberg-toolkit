# ==============================================================================
# VPC Module Variables - Based on test-vpc configuration
# ==============================================================================

# ==============================================================================
# REQUIRED VARIABLES
# ==============================================================================

variable "project_id" {
  description = "GCP project ID where VPC will be created"
  type        = string
}

variable "region" {
  description = "Primary GCP region for regional resources (router, NAT, VPC connector)"
  type        = string
}

variable "naming_prefix" {
  description = "Standardized naming prefix for resources (e.g., ibm-byoc-dp-12345)"
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$", var.naming_prefix))
    error_message = "naming_prefix must be lowercase alphanumeric with hyphens, start and end with alphanumeric, max 63 chars"
  }
}

variable "dataplane_id" {
  description = "Unique identifier for the dataplane, used for standardized module interfaces"
  type        = string
}

variable "dataplane_crn" {
  description = "Cloud Resource Name (CRN) for the dataplane, used for standardized module interfaces"
  type        = string
  default     = ""
}

variable "byoc_engine_label" {
  description = "BYOC engine label for standardized module interfaces"
  type        = string
  default     = "netezza"
}

variable "labels" {
  description = "Labels to apply to resources"
  type        = map(string)
  default     = {}
}

# ==============================================================================
# VPC CONFIGURATION
# ==============================================================================

variable "vpc_description" {
  description = "Description of the VPC network"
  type        = string
  default     = "VPC network for BYOC deployment"
}

variable "routing_mode" {
  description = "Network routing mode (REGIONAL or GLOBAL)"
  type        = string
  default     = "REGIONAL"
  validation {
    condition     = contains(["REGIONAL", "GLOBAL"], var.routing_mode)
    error_message = "Routing mode must be either REGIONAL or GLOBAL"
  }
}

variable "mtu" {
  description = "Maximum Transmission Unit in bytes (1460-8896)"
  type        = number
  default     = 8896
  validation {
    condition     = var.mtu >= 1460 && var.mtu <= 8896
    error_message = "MTU must be between 1460 and 8896"
  }
}

variable "delete_default_routes_on_create" {
  description = "Delete default routes on VPC creation"
  type        = bool
  default     = false
}

# ==============================================================================
# SUBNET CONFIGURATION
# ==============================================================================

variable "subnets" {
  description = "Map of subnets to create. Names are auto-generated from naming_prefix and subnet map keys."
  type = map(object({
    region                   = string
    ip_cidr_range            = string
    private_ip_google_access = bool
    description              = string
    enable_flow_logs         = bool
    flow_logs_config = object({
      aggregation_interval = string
      flow_sampling        = number
      metadata             = string
    })
    secondary_ip_ranges = list(object({
      range_name    = string
      ip_cidr_range = string
    }))
  }))
  default = {}
}

# ==============================================================================
# CLOUD ROUTER & NAT CONFIGURATION
# ==============================================================================

variable "create_nat_gateway" {
  description = "Create Cloud NAT gateway for outbound internet access"
  type        = bool
  default     = true
}

variable "router_asn" {
  description = "BGP ASN for Cloud Router"
  type        = number
  default     = 64514
}

variable "nat_log_config" {
  description = "Cloud NAT logging configuration"
  type = object({
    enable = bool
    filter = string
  })
  default = {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

# ==============================================================================
# FIREWALL RULES
# ==============================================================================

variable "create_default_firewall_rules" {
  description = "Create default firewall rules (allow internal, allow IAP SSH)"
  type        = bool
  default     = true
}

# ==============================================================================
# VPC CONNECTOR CONFIGURATION
# ==============================================================================

variable "create_vpc_connector" {
  description = "Create VPC Access Connector for Cloud Functions"
  type        = bool
  default     = true
}

variable "vpc_connector_name" {
  description = "Name of the VPC Access Connector. If null, a name derived from naming_prefix is used."
  type        = string
  default     = null
}

variable "vpc_connector_ip_cidr_range" {
  description = "CIDR range for VPC connector (must be /28 and not overlap with existing ranges)"
  type        = string
  default     = "10.8.0.0/28"
}

variable "vpc_connector_min_instances" {
  description = "Minimum number of VPC connector instances"
  type        = number
  default     = 2
}

variable "vpc_connector_max_instances" {
  description = "Maximum number of VPC connector instances"
  type        = number
  default     = 3
}

variable "vpc_connector_machine_type" {
  description = "Machine type for VPC connector instances"
  type        = string
  default     = "e2-micro"
  validation {
    condition     = contains(["e2-micro", "e2-standard-4", "f1-micro"], var.vpc_connector_machine_type)
    error_message = "Machine type must be one of: e2-micro, e2-standard-4, f1-micro"
  }
}