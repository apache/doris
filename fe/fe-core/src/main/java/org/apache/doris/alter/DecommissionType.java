package org.apache.doris.alter;

public enum DecommissionType {
    SystemDecommission, // after finished system decommission, the backend will be removed from Palo.
    ClusterDecommission // after finished cluster decommission, the backend will be removed from cluster.
}
