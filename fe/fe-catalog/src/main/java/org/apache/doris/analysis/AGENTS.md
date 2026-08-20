# Legacy Expr persistence — Review Guide

Legacy `Expr` objects are persisted by metadata consumers such as Routine Load images and
`ALTER ROUTINE LOAD` journals. Doris Gson serializes only fields annotated with
`@SerializedName`, so an unclassified field can be silently lost during FE recovery.

## Expr changes

- [ ] Any field that changes `ExprToSqlVisitor` output has a stable `@SerializedName` key.
- [ ] Existing serialized keys are not renamed, removed, or reused for a different meaning.
- [ ] Analysis caches and execution-only fields remain unpersisted only when they can be rebuilt
      after the restored expression is converted to SQL and analyzed again.
- [ ] Every unpersisted instance field is listed with that rationale in
      `ExprGsonSerializationTest.NON_DURABLE_EXPR_FIELDS`; do not add fields to the list merely to
      make the test pass.
- [ ] New concrete `Expr` subtypes are registered in both Gson factories and have a non-default
      sample in `ExprGsonSerializationTest`.
- [ ] Samples set every SQL-relevant option to a non-default value so semantic loss is observable.

## Required tests

- [ ] `ExprGsonSerializationTest` preserves the concrete subtype and `ExprToSqlVisitor` output
      with and without table names across Gson round-trip.
- [ ] Metadata consumers that introduce a new Expr carrier add an image and journal replay test.
- [ ] Routine Load expression changes cover column mappings, preceding filters, where filters, and
      delete conditions as applicable.
