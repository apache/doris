# Graceful shutdown and rolling restart on 4.1

## Operator procedure

Before a rolling restart, run:

```sql
SET GLOBAL enable_graceful_shutdown = true;
```

Restart one node at a time. Use `bin/stop_fe.sh --grace` and
`bin/stop_be.sh --grace`, or send SIGTERM. Wait for the restarted node to become
available before stopping the next node. The FE container prestop hook uses the
graceful stop command. Allow the process enough termination time to finish the
configured waits and final service cleanup.

After the restart, restore normal operation:

```sql
SET GLOBAL enable_graceful_shutdown = false;
```

Clients must reconnect or retry through another available FE when their connection
closes. Accepted SQL remains on its original FE and BE execution path while that
node drains. This feature does not migrate an executing query to another FE.

## FE shutdown

The FE immediately becomes unready, then continues serving for
`graceful_shutdown_drain_seconds` (default 15) to allow load balancers to converge.
It subsequently stops admitting MySQL commands and Flight SQL statements and
closes the MySQL listener. Flight result retrieval and session close remain
available during the grace period.

The FE waits up to `grace_shutdown_wait_seconds` (default 300) for accepted
requests, coordinators, cached Flight results and idle MySQL cleanup. Cleanup
runs on a daemon worker with the session's thread context and includes transaction
rollback and temporary tables. A slow cleanup cannot extend the shared wait
budget. Once the wait ends, the FE closes its remaining services.

## BE shutdown

The BE advertises its shutdown state in heartbeats before draining work. Its
waits are controlled by:

| Configuration | Default seconds | Purpose |
| --- | ---: | --- |
| `grace_shutdown_heartbeat_wait_seconds` | 30 | Wait for a shutdown heartbeat response to be prepared |
| `grace_shutdown_heartbeat_propagation_seconds` | 10 | Allow FE scheduling state to propagate |
| `grace_shutdown_wait_seconds` | 120 | Drain queries, load receivers and load contexts |
| `grace_shutdown_post_delay_seconds` | 30 | Allow remaining distributed work to finish |

The propagation delay is an allowance, not an acknowledgement from every FE.
Load accounting includes queued receiver opens, receiver destruction, HTTP upload
ownership and transaction rollback, even if no pipeline fragment is running.

## Recovery during rolling restart

With the global mode enabled:

- The FE heartbeat interval is capped at three seconds without changing the
  configured normal interval.
- A recovering BE needs five consecutive successful heartbeats before scheduling
  resumes. A new process epoch or failed heartbeat restarts confirmation; first
  startup and healthy unchanged processes bypass this wait. Followers replay the
  master's scheduling decision.
- Each FE invalidates its local BE RPC clients and DNS entry before publishing a
  recovered backend. Retired gRPC channels drain accepted RPCs without forcing
  them closed. Newly created or rebuilt BE brpc channels must pass a bounded
  handshake before entering the cache; healthy cache hits avoid the probe.
- Stream Load planning and transaction RPCs retry transport failures and
  `NOT_MASTER`, rereading the heartbeat master address. Five additional attempts
  wait 1, 3, 5, 7 and 9 seconds. Other application errors are returned directly.
- Losing the originating FE does not prematurely cancel BE-coordinated Stream
  Load. Query timeouts and ordinary query failure handling remain in force.

Forced DNS refresh retains the last known address during a resolver outage.
System and JVM DNS TTLs still apply to the underlying resolver.

## Compatibility and remaining work

The heartbeat mode uses optional `TMasterInfo` field 16, matching the newer
implementation. Field 15 is already used for changelog on master and is not
reused. The old 3.1 implementation's field-15 flag is not interpreted as this
field-16 flag: use participating FEs and BEs that support field 16 before relying
on rolling mode. Older heartbeat journal entries default to ready for scheduling.
4.1's TLS/server/client factories are preserved.

Partial-open receiver cleanup and multi-table setup failures before completion
callbacks remain tracked by TODOs at their ownership sites. They can consume the
full drain budget after an abnormal load failure. Hard termination, lost process
state, client retry support and complete cluster rolling-restart validation remain
outside what a successful static check or unit test can establish.
