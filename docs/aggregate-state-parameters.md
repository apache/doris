# Aggregate-state parameter compatibility

Merging two aggregate states that contribute data requires compatible parameters.
States whose payload can be skipped do not participate in parameter checks. When
the destination has no contributing data, it adopts the contributing source's
parameters. These rules apply in both merge orders, including `_merge` and `_union`.

Non-contributing states include:

- TopN states with no retained counters, including serialized zero-capacity states.
- Limited `collect_list` and `collect_set` states with no retained elements.
- Percentile reservoirs, exact percentile V2 states and approximate percentiles
  with no retained samples, including all-NaN samples and zero-weight inputs.
- Percentile arrays with no quantile levels and therefore no retained samples.
- Sequence functions and Window Funnel V2 states with no matched events.

Empty percentile arrays may still retain their output shape when all merged
states have no samples. Their parameters never constrain a contributing state.
An empty TopN map is handled before full-map count adjustment, avoiding invalid
counter changes when capacity is zero.

An empty final result does not always imply a skippable state:

- `group_concat('')` retains an input string and can contribute a separator.
- `intersect_count` retains filter keys even when their bitmaps are empty; those
  keys affect the intersection.
- Window Funnel V1 stores all-false rows, which can interrupt a fixed-mode chain.
- EMA with zero half-decay retains its accumulated value and reference time.

These contributing states still require matching parameters. Negative collection
limits retain unlimited-collection behavior and must match for nonempty states.
Invalid parameters remain subject to each function's existing validation.

Fresh states and states cleared by `reset()` are merge identities. Serialized
field layouts are retained. Window Funnel V2 uses the original boolean sorted
field; no initialization tag is needed for eventless states. Historical states
from the trial AggState implementation remain outside the compatibility scope.
