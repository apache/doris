# Function Combinator

The function combiner is used to automatically create some functions based on the nested function and do some additional logic processing.

Currently we can generate the combinator function we want by implementing a new `FunctionBuilder`. For example `AggStateFunctionBuilder` can generate combiner functions created by the `_state`/`_merge` combiner, so that we can use functions such as `sum_state` and `sum_merge` in SQL statements.

When we input `sum_state`, if `BuiltinFunctionBuilder` does not find a function named `sum_state`, then it will try to use `AggStateFunctionBuilder` to construct `StateCombinator`, so we get a `StateCombinator` object with nested function sum, and use it to generate the `sum_state` function.

## Finalize one aggregate state

`<aggregate>_finalize(state)` is a scalar combinator that returns the result of each
serialized aggregate state independently. It accepts exactly one `AGG_STATE` of
the matching aggregate (including its aliases), and uses the same result type and
state semantics as `<aggregate>_merge`. An outer SQL NULL state returns NULL;
an empty serialized state follows the nested aggregate's existing behavior.

For example, after pre-aggregating each key, the finest grouping can read its AVG
without another aggregate operator:

```sql
SET enable_agg_state = true;
SELECT k, avg_finalize(s)
FROM (SELECT k, avg_combine(v) AS s FROM t GROUP BY k) partial;
```

`avg_finalize` returns one result per input row. In contrast, `avg_merge` merges
all input states in each group before computing one result. The same scalar
combinator applies to other aggregates supporting `AGG_STATE`, such as `sum`,
`count`, `min`, `max`, and `array_agg`; it does not change their serialized formats.
