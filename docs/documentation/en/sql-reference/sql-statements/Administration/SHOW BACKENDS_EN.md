# SHOW BACKENDS
## Description
This statement is used to view BE nodes in the cluster
Grammar:
SHOW BACKENDS;

Explain:
1. LastStartTime indicates the last BE start-up time.
2. LastHeartbeat represents the latest heartbeat.
3. Alive indicates whether the node survives.
4. System Decommissioned is true to indicate that the node is safely offline.
5. Cluster Decommissioned is true to indicate that the node is rushing downline in the current cluster.
6. TabletNum represents the number of fragments on the node.
7. Data Used Capacity represents the space occupied by the actual user data.
8. Avail Capacity represents the available space on the disk.
9. Total Capacity represents total disk space. Total Capacity = AvailCapacity + DataUsedCapacity + other non-user data files take up space.
10. UsedPct represents the percentage of disk usage.
11. ErrMsg is used to display error messages when a heartbeat fails.

## keyword
SHOW, BACKENDS
