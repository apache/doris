# Spread hot external scan splits across consistent hash candidates

Repeated queries for the same remote file split can all choose the same backend
under consistent hashing. Each query starts with zero assigned weight, and the
original tie rule always chooses the same candidate. Increasing the original
candidate count alone does not remove this bias.

## Enable the optional scheduling mode

```sql
SET use_consistent_hash_for_external_scan = true;
SET external_scan_consistent_hash_spread_num = 3;
```

`external_scan_consistent_hash_spread_num` accepts integers from 1 through
2147483647. Its default is **1**, which preserves the original scheduler,
including its configured hash candidate count and split redistribution. It does
not force the original scheduler to use just one candidate. `UNSET VARIABLE
external_scan_consistent_hash_spread_num` restores the session default.

Values greater than 1 enable spreading only when an external scan uses consistent
hashing. This occurs when either `enable_file_cache` or
`use_consistent_hash_for_external_scan` is enabled. The new variable alone does
not switch a round robin scan to consistent hashing, and it does not enable file
caching. Other consumers of the backend policy, including file load and schema
scans, retain their existing behavior.

## Assignment and locality

For a remote split without a preferred local backend, the scheduler takes the
first N distinct eligible backends from the existing consistent hash ring, capped
by the number of eligible backends in the query's compute group. It selects the
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
global redistribution. The default mode retains that redistribution.

## Capacity and cache tradeoffs

Spreading independently planned queries can increase throughput when the original
hot backend is saturated and the other candidate backends have available capacity.
Random choice does not guarantee a particular assignment sequence or throughput
multiplier. Frontend planning, remote storage, network capacity and shared host
resources can still limit throughput. No CPU monitoring or shared query counter is
introduced.

With file caching enabled, the same data can occupy cache space on multiple
backends and incur additional warmup reads. The variable bounds remote hash
candidates; it does not create cache replicas in advance. Locality takes precedence
over this bound. To measure scheduling changes separately from cache hits, keep
file caching disabled and enable consistent hashing explicitly on both sides of
a comparison.
