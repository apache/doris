# BE Common Module — Review Guide

## Error Handling: Status / Exception / Result<T>

| Type | Use | Propagation |
|------|-----|-------------|
| `Status` | Default error return | `RETURN_IF_ERROR` |
| `Exception` | Vectorized hot paths | `RETURN_IF_CATCH_EXCEPTION` |
| `Result<T>` | Value-or-error return | `DORIS_TRY` |

### Checkpoints

- [ ] Every `Status` return checked? Never `static_cast<void>(...)` to discard
- [ ] Every `THROW_IF_ERROR` has a `RETURN_IF_CATCH_EXCEPTION` or `RETURN_IF_ERROR_OR_CATCH_EXCEPTION` above it?
- [ ] `THROW_IF_ERROR` kept out of `Defer`, destructors, and stack-unwinding paths? Use `WARN_IF_ERROR` there
- [ ] Thrift/RPC handlers convert Doris exceptions at the boundary?
- [ ] `WARN_IF_ERROR` limited to cleanup/best-effort paths with non-empty message?
- [ ] `DORIS_CHECK` for invariants only, never speculative defensive checks?
- [ ] New catch blocks keep standard order: `doris::Exception` → `std::exception` → `...`?

## compile_check Mechanism

`compile_check_begin.h` / `compile_check_end.h` raise conversion warnings to errors.

### Checkpoints

- [ ] New declaration-heavy headers use paired `compile_check_begin.h` / `compile_check_end.h` matching neighbors?
- [ ] Narrowing conversions avoided (`int64_t→int32_t`, `size_t→int`, `uint64_t→int64_t`)?
- [ ] Third-party bypass uses `compile_check_avoid_begin.h` / `compile_check_avoid_end.h`, not local weakening?
