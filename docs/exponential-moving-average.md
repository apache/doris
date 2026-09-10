# Exponential moving average: zero half-decay semantics

`exponential_moving_average(half_decay, value, timeunit)` accepts a constant numeric
half-decay. A half-decay of `0` is a special case: the aggregate returns `0` for
non-null input rows, rather than evaluating the exponential-decay formula.

For example:

```sql
SELECT exponential_moving_average(0, 7, 1);
-- 0
```

A zero half-decay also marks an empty aggregate state. When combining serialized
states with `exponential_moving_average_merge` or
`exponential_moving_average_union`, states whose half-decay is `0` are ignored.
This applies even when the state was created from non-null input rows.

Consequently:

- Combining states with half-decays `0` and `1` uses the contributing state with
  half-decay `1`, without a parameter-mismatch error, in either input order.
- Combining contributing states with different nonzero half-decays, such as `1`
  and `2`, raises an incompatible-half-decay error.
- Combining non-null states whose half-decays are all `0` yields `0` when the
  aggregate is finalized.

This documents the existing zero-half-decay behavior. Zero is not a distinct
contributing configuration for aggregate-state compatibility checks. These rules
concern zero half-decay; they do not define NaN as an empty state or change SQL
NULL handling.
