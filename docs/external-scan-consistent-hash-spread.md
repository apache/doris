# Spread hot external scan splits across consistent hash candidates

Repeated queries for the same remote file split can all choose the same backend
under consistent hashing. Each query starts with zero assigned weight, and the
original tie rule always chooses the same candidate. Increasing the original
candidate count alone does not remove this bias.

## Automatic scheduling by default

`external_scan_consistent_hash_spread_num` accepts integers from 0 through
2147483647. Its default is **0**. Remote external scans automatically spread
across all eligible backends in the query's compute group, regardless of the
number of backends. No manual hash setting is required.

| Value | Behavior |
| --- | --- |
| 0 (default) | Balance across all eligible backends, using the existing hash ring when file cache or consistent hashing is enabled, otherwise the random strategy. |
| 1 | Preserve original scheduling, configured candidate counts and global redistribution. |
| N > 1 | Spread within at most N hash candidates when file cache or consistent hashing is enabled; otherwise preserve original round robin scheduling. |

```sql
-- Restore original behavior.
SET external_scan_consistent_hash_spread_num = 1;
-- Limit hash candidates while retaining spreading.
SET use_consistent_hash_for_external_scan = true;
SET external_scan_consistent_hash_spread_num = 3;
-- Restore automatic scheduling.
UNSET VARIABLE external_scan_consistent_hash_spread_num;
```

Existing persisted global values are retained on upgrade. If an upgraded cluster
still has a global value of 1, an administrator can set the global value to 0
for new sessions; existing sessions can use `UNSET VARIABLE` to restore the
compiled default or set their session value explicitly.

The variable does not enable file caching. Other consumers of the backend policy,
including file load and schema scans, retain their existing behavior.

## Assignment and locality

For a remote split without a preferred local backend, automatic mode considers
all eligible backends. Explicit N uses the first N distinct eligible backends
from the existing consistent hash ring, capped by the compute group size. It selects the
backend with the least weight already assigned by this policy. Ties are broken
uniformly at random. A fresh query can therefore choose a different backend for
the same split, while multiple splits and batches share their policy's weight
history. Each split is assigned for execution exactly once.

The mode preserves local preference and mandatory Host constraints for splits
that cannot be accessed remotely. These constraints can choose a backend outside
the remote split's hash candidates. Different backends on the same Host remain
distinct hash candidates.

Global split redistribution is disabled in this mode: it could otherwise move a
split outside its hash candidates or mandatory locality. Queries with many splits
balance within each split's candidates, rather than across every eligible backend.
Some split distributions can consequently be less balanced than with the original
global redistribution. Setting the variable to 1 retains that redistribution.

## Capacity and cache tradeoffs

Spreading independently planned queries can increase throughput when the original
hot backend is saturated and the other candidate backends have available capacity.
Random choice does not guarantee a particular assignment sequence or throughput
multiplier. Frontend planning, remote storage, network capacity and shared host
resources can still limit throughput. No CPU monitoring or shared query counter is
introduced.

With file caching enabled, automatic mode can store the same data on every
eligible backend and incur additional warmup reads. Use 1 to restore the original
cache placement or explicit N to cap hash candidates. The variable bounds remote hash
candidates; it does not create cache replicas in advance. Locality takes precedence
over this bound. To measure scheduling changes separately from cache hits, keep
file caching disabled and enable consistent hashing explicitly on both sides of
a comparison.
