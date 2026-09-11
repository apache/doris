# Aggregate-state parameter compatibility

Parameterized aggregate states distinguish initialization from retained data:

- A fresh state has no configuration. It is an identity during merge.
- A non-null input establishes configuration even when it retains no effective
  data. Merging two initialized states requires compatible parameter values.
- `reset()` discards both data and configuration. The resulting state is an
  identity, including after serialization and deserialization.

These rules apply in both merge orders and to `_merge` and `_union`. Examples of
initialized states include a percentile reservoir with only NaN samples, an exact
percentile V2 with only NaN samples, a zero-weight approximate percentile, a sequence
or funnel with all-false events, a zero-limit collection, an empty-string
`group_concat`, an empty bitmap intersection, and a zero-half-decay moving average.
TopN retains its N/capacity configuration even when a zero capacity causes its
serialized payload to contain no elements. After configuration checks, empty
TopN payloads do not change compatible counters; this also prevents zero-capacity
empty maps from applying an invalid full-map count adjustment.

Negative collection limits retain the existing unlimited-collection behavior;
they are configurations and must match when states are merged. A zero EMA
half-decay still produces zero, but it is a configuration rather than an identity.
Invalid parameters remain subject to each function's existing validation.

Existing initialization flags are reused where available. Reservoir reserves
quantile `-1` for fresh/reset states, outside the valid interval `[0, 1]`. Limited
collection states reserve one below the minimum Int32 value, outside their
parameter domain and within the serialized varint range. EMA tracks initialization
independently in memory and reserves a serialized NaN half-decay for fresh/reset
states; configured NaN half-decays cannot be serialized.
The existing serialized field layouts are retained.

Historical states from the trial AggState implementation are outside this change's
compatibility scope. In particular, old reset states may contain stale configuration
values that cannot always be distinguished from initialized states. The rules above
are guaranteed for states produced by the updated implementation.
