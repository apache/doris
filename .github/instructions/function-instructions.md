---
applyTo:
  - "be/src/vec/functions/**/*.h"
  - "be/src/vec/functions/**/*.hpp"
  - "be/src/vec/functions/**/*.cc"
  - "be/src/vec/functions/**/*.cpp"
  - "fe/fe-core/src/main/java/org/apache/doris/nereids/trees/expressions/functions/**/*.java"
  - "fe/fe-core/src/main/java/org/apache/doris/catalog/BuiltinScalarFunctions.java"
---

# Apache Doris Function Instructions

## Scope

- BE vectorized function implementation and registration: `be/src/vec/functions/`
- FE function signatures/type inference/nullability/visitors: `fe/fe-core/src/main/java/org/apache/doris/nereids/trees/expressions/functions/`
- FE constant folding: `.../functions/executable/`

Goal: provide unified guidance for implementing/registering SQL functions, nullability contracts, constant-column handling, and testing, so reviewers can check for consistency and quality.

## FE-side Specification (Signatures, Nullability, Constant Folding)

- Place signature classes under: `org.apache.doris.nereids.trees.expressions.functions.scalar.*` (or `table`/`agg` subpackages).
- Common nullability/folding traits (combine as needed):
  - `AlwaysNullable`: return type is always `Nullable`, even for not-null inputs (e.g., parse failure/out-of-range → NULL).
  - `AlwaysNotNullable`: return type is never `Nullable` (e.g., normalize NULL to a non-null constant).
  - `PropagateNullable`: if any input is `Nullable`, output is `Nullable`; otherwise not (default preference).
  - `PropagateNullLiteral`: during constant folding, if any input is a `NullLiteral`, output folds to a `NullLiteral` (e.g., `Acos(NULL)` → `NULL`).
  - `PropagateNullableOnDateOrTimeLikeV2Args`: if any argument is DateV2/DateTimeV2/Time-like, behave like `PropagateNullable`; otherwise behave like `AlwaysNullable`.
- Nullability tip: Prefer these traits; if custom semantics are needed, don’t implement the three standard traits and override `nullable()` directly (see `ComputeNullable`).
- Register the class and aliases in `BuiltinScalarFunctions` to ensure parsing and dispatching.
- Constant folding (optional): For deterministic functions, consider adding constant folding under `functions/executable/`; for NULL literal pass-through folding, use `PropagateNullLiteral`.
- Note: Legacy `doris_builtin_functions.py` is deprecated; do not modify.
- Note: Some expressions should not blindly use generic traits (e.g., `InPredicate` should not use `PropagateNullable`) to avoid incorrect folding or nullability inference.

## BE-side Specification (Implementation, Performance, Registration)

- Typical skeleton (inherit from `IFunction`):
  - `static constexpr auto name` / `get_name()`
  - `get_number_of_arguments()` or support variadic
  - `get_return_type_impl(const DataTypes&)` (if `use_default_implementation_for_nulls()==true`, return the bare type; the framework wraps Nullable. Only manually `make_nullable(...)` if semantically “always nullable” or you handle NULLs yourself.)
  - `Status execute_impl(FunctionContext*, Block&, const ColumnNumbers&, uint32_t, size_t)`
- Registration steps:
  1) Add the implementation under `be/src/vec/functions/`.
  2) Register it in `register_function_xxx(SimpleFunctionFactory&)` (or create a new one).
  3) In `simple_function_factory.h`, call `register_function_xxx(instance);` inside `SimpleFunctionFactory::instance()` initialization.

### New Paradigm for Constant Column (ColumnConst) Handling

- Avoid blindly expanding constants via `convert_to_full_column_if_const`.
- Use:
  - `unpack_if_const(col)` → `(ColumnPtr, bool is_const)` to get the real column and constancy.
  - `default_preprocess_parameter_columns(...)` to expand “parameter columns” (non-data columns) when needed.
  - If some arguments must be constants, override `get_arguments_that_are_always_constant()` for automatic validation.
  - Usually keep `use_default_implementation_for_constants() == true`; implement only `vector_vector`/`vector_scalar`; the framework treats `const_const` equivalently.
  - For non-deterministic functions (e.g., `random`), set `use_default_implementation_for_constants()` to `false` to disable folding.

Example (two arguments):

```cpp
const auto& [lcol, lconst] = unpack_if_const(block.get_by_position(arguments[0]).column);
const auto& [rcol, rconst] = unpack_if_const(block.get_by_position(arguments[1]).column);
// Choose vector_vector or vector_scalar based on lconst/rconst
```

Example (multi/variadic arguments):

```cpp
Columns cols(arguments.size());
std::unique_ptr<bool[]> col_const = std::make_unique<bool[]>(arguments.size());
for (int i = 0; i < arguments.size(); ++i) {
    std::tie(cols[i], col_const[i]) = unpack_if_const(block.get_by_position(arguments[i]).column);
}
// Use col_const[i] to decide vector_scalar vs vector_vector and avoid unnecessary expansion
```

Example (with parameter-column optimization):

```cpp
bool col_const[3];
ColumnPtr argument_columns[3];
for (int i = 0; i < 3; ++i) {
    col_const[i] = is_column_const(*block.get_by_position(arguments[i]).column);
}
argument_columns[0] = col_const[0]
        ? static_cast<const ColumnConst&>(*block.get_by_position(arguments[0]).column).convert_to_full_column()
        : block.get_by_position(arguments[0]).column;

default_preprocess_parameter_columns(argument_columns, col_const, {1, 2}, block, arguments);
if (col_const[1] && col_const[2]) {
    execute_impl_scalar_args(...);
} else {
    execute_impl(...);
}
```

#### Using index_check_const

- Purpose: When accessing inputs that may be constant columns, normalize the row index to 0 for those constant inputs to avoid branching/expansion.
- Definition: `be/src/vec/columns/column_const.h`
- Usage forms:
  - Runtime-boolean form:

    ```cpp
    // is_const is a runtime boolean (e.g., from unpack_if_const or col_const[i])
    size_t idx = index_check_const(i, is_const);
    auto ref = col->get_data_at(idx);
    ```

  - Compile-time boolean form (template):

    ```cpp
    // str_const/len_const must be compile-time constant template params
    const auto idx = index_check_const<str_const>(i);
    StringRef v = str_col->get_data_at(idx);
    ```

- Note: The template works for integral indices; source comments note possible performance impact on some GCC targets. For extremely hot paths, consider branching once before a tight loop.

### Nullability & Error Handling
  
- Nullability follows the return type and framework decisions; commonly use `PropagateNullable`; when needed, use `DataTypeNullable`/`ColumnNullable`.
- Common helpers: `make_nullable`, `remove_nullable`, `is_column_nullable`, `is_column_const`.
- Error handling uses the `Status` system and macros: `RETURN_IF_ERROR`, `DORIS_TRY`, `WARN_IF_ERROR` (see `be/src/common/status.h`).

- BE default NULL handling (important): `use_default_implementation_for_nulls()` defaults to `true`:
  - If any input is a constant NULL, return a constant NULL.
  - If any inputs are `Nullable`, execute on nested columns; the framework wraps the result with the union null-map as `Nullable`.
  - In this mode, `get_return_type_impl` must return the bare type; the framework decides whether to wrap Nullable.
- When to set `use_default_implementation_for_nulls()` to `false`: when custom NULL semantics are required or you must read/write the null map explicitly (e.g., `CASE`/`IN`/`IS NULL`/some `JSON`/`CAST`).
- `need_replace_null_data_to_default()`: under default NULL handling, sanitize nested data on NULL rows to type defaults after execution (avoid issues like arithmetic overflow).

### Performance Notes (Hot Path)

- Write directly to output column memory; avoid temporary copies; use `reserve/resize` appropriately.
- Avoid virtual calls and repeated dispatch in loops; use templates to eliminate type checks.
- Abstract/reuse existing implementations; avoid duplication and branching complexity.

## Testing & Quality Assurance

- Regression tests: test types/volume not less than templates (e.g., `test_template_{X}_arg(s).groovy`).
- Input coverage: datatype boundaries, null/not-null combinations, exceptions/out-of-range, encoding/case, etc.
- FE constant folding: cases with `NullLiteral` must match `PropagateNullLiteral` semantics (e.g., `Acos(NULL)` → `NULL`).
- BE unit tests (UT): use `check_function_all_arg_comb` to cover const combinations and column layout variants; cover normal/exception/boundary inputs.
- Scripts: `run-regression-test.sh`, `run-be-ut.sh` (see details inside scripts).

## Reviewer Checklist

- Nullability/folding contract correct: `AlwaysNullable`/`AlwaysNotNullable`/`PropagateNullable`/`PropagateNullLiteral` (and specializations like `PropagateNullableOnDateOrTimeLikeV2Args`) align with BE return type and FE folding semantics.
- BE NULL handling strategy correct: `use_default_implementation_for_nulls()` set appropriately; under default handling `get_return_type_impl` returns the bare type (framework wraps Nullable); set `need_replace_null_data_to_default()` when needed.
- Constant-column handling appropriate: use `unpack_if_const`/`index_check_const`/`default_preprocess_parameter_columns` to avoid unnecessary expansion.
- Registration complete:
  - BE registered in `SimpleFunctionFactory`.
  - FE mapped alias → class in `BuiltinScalarFunctions`.
- Performance reasonable: no redundant copies/allocations; no virtual calls in hot loops; capacity pre-reserved appropriately.
- Error handling: consistent `Status`; cleanup on error paths with context.
- Constant folding: add FE folding for deterministic functions; disable folding for non-deterministic ones.
- Tests complete: cover boundaries/nulls/type conversions/large volumes/const combinations.

## Code Snippets (Examples)

- BE registration:

```cpp
void register_function_compress(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionCompress>();
    factory.register_function<FunctionUncompress>();
}
// Call register_function_compress(instance) inside SimpleFunctionFactory::instance().
```

- FE registration:

```java
// Append in BuiltinScalarFunctions:
scalar(Compress.class, "compress");
scalar(Uncompress.class, "uncompress");
```

- BE execution implementation highlights:

```cpp
Status execute_impl(FunctionContext* ctx, Block& block, const ColumnNumbers& args,
                    uint32_t result, size_t rows) const override {
    const auto& in = assert_cast<const ColumnString&>(*block.get_by_position(args[0]).column);
    auto out_col = ColumnString::create();
    auto& out_data = out_col->get_chars();
    auto& out_offs = out_col->get_offsets();
    out_offs.resize(rows);
    // Pre-estimate capacity / avoid copies / consider Slice or faststring
    block.replace_by_position(result, std::move(out_col));
    return Status::OK();
}
```

## Do & Don’t

- Do not modify generated/third-party code, e.g., `be/src/gen_cpp/**`.
- Keep FE/BE function names and aliases consistent; mind case and compatibility aliases.
- Do not enable constant folding/propagation for non-deterministic functions.
- For variadic functions, clearly define boundary behavior and return types for zero/one-arg cases.
- Aggregation functions: follow the existing implementation and registration flow (state type definition, aggregation implementation, factory registration, FE signatures and nullability contracts).
