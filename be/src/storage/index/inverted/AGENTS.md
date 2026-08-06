# Inverted Index — Review Guide

## Lifecycle

- [ ] Field declaration order preserves `_dir` before `_index_writer` lifetime dependency?
- [ ] Cache-facing code uses `InvertedIndexCacheHandle` over raw handles for paired release/retention?

## CLucene Boundary

CLucene uses `ErrorContext` + local FINALLY helpers, not Doris exception flow.

- [ ] New CLucene integrations use `ErrorContext` + `FINALLY` / `FINALLY_CLOSE` / `FINALLY_EXCEPTION`, not ad hoc try/catch?

## Three-Valued Logic Bitmap

- [ ] Bitmap logic preserves SQL three-valued semantics across `_data_bitmap` and `_null_bitmap`?
- [ ] `op_not` is `const` in signature but mutates shared state — callers handle this?
