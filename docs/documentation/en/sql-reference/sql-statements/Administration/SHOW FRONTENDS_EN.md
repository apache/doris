# SHOW FRONTENDS
# Description
This statement is used to view FE nodes
Grammar:
SHOW FRONTENDS;

Explain:
1. name denotes the name of the FE node in bdbje.
2. Join is true to indicate that the node has joined the cluster. But it doesn't mean that it's still in the cluster (it may be out of touch)
3. Alive indicates whether the node survives.
4. Replayed Journal Id represents the maximum metadata log ID that the node has currently replayed.
5. LastHeartbeat is the latest heartbeat.
6. IsHelper indicates whether the node is a helper node in bdbje.
7. ErrMsg is used to display error messages when the heartbeat fails.

## keyword
SHOW, FRONTENDS
