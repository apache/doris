# Function Combinator

The function combiner is used to automatically create some functions based on the nested function and do some additional logic processing.

Currently we can generate the combinator function we want by implementing a new `FunctionBuilder`. For example `AggStateFunctionBuilder` can generate combiner functions created by the `_state`/`_merge` combiner, so that we can use functions such as `sum_state` and `sum_merge` in SQL statements.

When we input `sum_state`, if `BuiltinFunctionBuilder` does not find a function named `sum_state`, then it will try to use `AggStateFunctionBuilder` to construct `StateCombinator`, so we get a `StateCombinator` object with nested function sum, and use it to generate the `sum_state` function.
