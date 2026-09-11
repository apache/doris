# Exponential moving average: zero half-decay semantics

`exponential_moving_average(half_decay, value, timeunit)` accepts a constant numeric
half-decay. A half-decay of `0` is a special case: the aggregate returns `0` for
non-null input rows, rather than evaluating the exponential-decay formula.

For example:

```sql
SELECT exponential_moving_average(0, 7, 1);
-- 0
```

A non-null input establishes the half-decay configuration, including when it is
`0`. When combining states with `exponential_moving_average_merge` or
`exponential_moving_average_union`, every initialized state participates in
parameter compatibility checks, regardless of its final numeric result.

Consequently:

- Combining initialized states with half-decays `0` and `1` raises an
  incompatible-half-decay error in either input order, including after serialization.
- Combining initialized states with different nonzero half-decays, such as `1`
  and `2`, also raises an incompatible-half-decay error.
- Combining non-null states whose half-decays are all `0` yields `0` when the
  aggregate is finalized.

Fresh states and states cleared by `reset()` have no configuration. They are
identities during merge and adopt the other state's configuration. SQL NULL
handling is unchanged.

A NaN half-decay is unsupported. Serializing an aggregate state or finalizing its
result raises `exponential_moving_average half decay must not be NaN`. This check
applies to the half-decay parameter, not to the input value or the computed result.

The serialized state retains its three-double layout. Since configured NaN
half-decays are rejected before serialization, a NaN in the serialized half-decay
field is reserved for a fresh/reset state. It is decoded as an uninitialized
state, separately from an initialized state whose half-decay is zero.
