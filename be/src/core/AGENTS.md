# BE Core Module — Review Guide

## Allocator-Aware Containers

Types in `be/src/core/custom_allocator.h` exist to route owned memory through Doris allocation and tracking paths.

### Checkpoints

- [ ] For BE-owned buffers or containers whose memory should be tracked, prefer `DorisVector`, `DorisMap`, `DorisUniqueBufferPtr`, and related allocator-aware wrappers instead of the corresponding standard library ownership types

## COW Column Semantics

Vectorized columns (`IColumn`) use intrusive-reference-counted copy-on-write.

### Checkpoints

- [ ] Exclusive ownership guaranteed before `mutate()` on hot paths? Shared ownership triggers deep copy
- [ ] `assume_mutable_ref()` used only when exclusive ownership is already guaranteed?
- [ ] After `Block::mutate_columns()`, columns put back with `set_columns()`?
- [ ] `convert_to_full_column_if_const()` materializes only `ColumnConst`; ordinary columns may return shared storage?

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
