# CANCEL DECOMMISSION
## Description

This statement is used to undo a node's offline operation. (Administrator only!)
Grammar:
CANCEL DECOMMISSION BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];

## example

1. Cancel the offline operation of two nodes:
CANCEL DECOMMISSION BACKEND "host1:port", "host2:port";

## keyword
CANCEL,DECOMMISSION,BACKEND
