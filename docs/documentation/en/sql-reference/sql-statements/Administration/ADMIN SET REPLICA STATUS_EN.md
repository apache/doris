# ADMIN SET REPLICA STATUS
## description

    This commend is used to set the status of the specified replica.
    This command is currently only used to manually set the status of some replicas to BAD, allowing the system to automatically repair these replicas.

    Syntax:

        ADMIN SET REPLICA STATUS
        PROPERTIES ("key" = "value", ...);

        The following attributes are currently supported:
        "tablet_id": required. Specify a Tablet Id.
        "backend_id": required. Specify a Backend Id.
        "status": required. Specify the status. Only "bad" is currently supported.

        If the specified replica does not exist or the status is already bad, it will be ignored.

    Notice:

        Replica set to Bad status can no longer be restored, please proceed with caution.

## example

    1. Set the replica status of tablet 10003 on BE 10001 to bad.

        ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "10003", "backend_id" = "10001", "status" = "bad");

## keyword

    ADMIN,SET,REPLICA,STATUS

