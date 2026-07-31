# BE Core Module — Review Guide

## Allocator-Aware Containers

Types in `be/src/core/custom_allocator.h` exist to route owned memory through Doris allocation and tracking paths.

### Checkpoints

- [ ] For BE-owned buffers or containers whose memory should be tracked, prefer `DorisVector`, `DorisMap`, `DorisUniqueBufferPtr`, and related allocator-aware wrappers instead of the corresponding standard library ownership types

## COW Column Semantics

Vectorized columns (`IColumn`) use intrusive-reference-counted copy-on-write.

### Standard Use Pattern

1. For newly created columns locally: Prefer to directly retain the original `MutableColumnPtr` if possible. Use `assume_mutable()` if necessary.
2. For shared or unknown ownership `ColumnPtr`: Use `IColumn::mutate(...)` and write back the owner.
3. When modifying the entire `Block` or `Column`: Prefer using `mutate_columns_scoped()` or `mutate_column_scoped(pos)`.
4. When applying an algorithm to a `Block` using `MutableBlock`: Understand the potential for detachment, then use `ScopedMutableBlock` or `VectorizedUtils::build_scoped_mutable_mem_reuse_block(...)`.
5. Row-level loops: Do not mutate the block or columns row by row. Obtain the mutable owners all at once.
6. Modifying columns of complex types: First mutate the parent owner. Do not force clone to satisfy mutable access. Only detach when it is definitely necessary to modify.

Avoid anti-patterns:

1. Do not use `IColumn::mutate` to modify and write back a column in a `Block`; instead, use `mutate_column_scoped`.
2. Do not call `Block::get_columns()` directly on a `Block` before mutation.
3. Do not use `assert_mutable()` if ownership is not clearly defined; instead, use `mutate` and understand the possibility of detach.
4. Do not `mutate()` row by row.
5. Do not call `ScopedMutableColumns::release()` unless it is a special scenario requiring temporary transfer of ownership.

### Checkpoints

- [ ] Exclusive ownership guaranteed before `mutate()` on hot paths? Shared ownership triggers deep copy
- [ ] `assert_mutable()` used only when exclusive ownership is already guaranteed?
- [ ] If you need to modify the data within a `Block`, have you correctly used `ScopedMutableBlock`?
- [ ] `convert_to_full_column_if_const()` materializes only `ColumnConst`; ordinary columns may return shared storage?
- [ ] If a `Block` is still going to be used later, and we temporarily need to use its column externally，should we prefer to copy rather than `std::move` its `ColumnPtr`? If moving is necessary, are they all put back before all exits?

## Type System and Serialization

### Upgrade/Downgrade Compatibility

- [ ] Serialized block/datatype layout changes gated with `be_exec_version` where required?
- [ ] Old and new serialization branches updated together (byte-size, serialize, deserialize)?

### Decimal and Type Metadata

- [ ] Decimal result types check precision growth limits, no accidental incompatible widening?
- [ ] `DecimalV2` `original_precision`/`original_scale` set intentionally, not left as `UINT32_MAX` sentinel?
- [ ] `TScalarType` optional fields filled for DECIMAL, CHAR/VARCHAR, DATETIMEV2?
- [ ] Flattened `TTypeDesc.types` traversal correct for nested complex types?

## Block Merge Nullable Trap

`MutableBlock::merge_impl()` assumes nullable promotion shape without dynamic checking in release builds.

- [ ] New block merge logic preserves nullable-promotion preconditions?
