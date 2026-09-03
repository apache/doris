### What problem does this PR solve?

Problem Summary:

`DATASKETCHES_HLL_UNION_AGG` merges serialized Apache DataSketches HLL sketches. Doris previously pinned `datasketches-cpp` 5.2.0, which contains a regression introduced by the lazy KxQ/`curMin` rebuild optimization.

When an HLL-mode sketch was downsampled during a union, the register array was updated while the cached estimator state remained pending rebuild. Some subsequent operations did not honor that pending state. Depending on the merge order, the union could therefore:

- Treat a populated union as empty and replace previously accumulated data.
- Apply incremental updates against stale estimator state and return an incorrect estimate.
- Produce different serialized bytes for equivalent merge sequences.

Doris also initialized the union limit from the first serialized sketch, including empty or sparse sketches whose configured `lgK` should not necessarily constrain the effective precision of later inputs. In a parallel aggregation, the first sketch is not deterministic, so the effective precision and memory usage could depend on input and partial-state merge order.

This PR updates the `datasketches-cpp` submodule from the 5.2.0 commit (`de8553ba`) to upstream commit [`46025e9`](https://github.com/apache/datasketches-cpp/commit/46025e9aeed8368b1184cbde9634dd99d0ee47c0). The upstream fix rebuilds the deferred KxQ/`curMin` state before operations that require it and makes union estimates and serialization independent of the affected merge order.

On the Doris side, empty sketches no longer initialize the union, and deserialized intermediate states restore the union from the serialized sketch's own effective `lgK`. This prevents empty inputs and the aggregate transport path from imposing an additional precision reduction.

### Release note

Fixed incorrect and merge-order-dependent results in `DATASKETCHES_HLL_UNION_AGG` for affected mixed-`lgK` HLL sketches. Added an optional constant `lg_max_k` argument so users can explicitly control the precision and memory upper bound of the union.

### Accuracy Control

The function now supports both forms:

```sql
DATASKETCHES_HLL_UNION_AGG(sketch)
DATASKETCHES_HLL_UNION_AGG(sketch, lg_max_k)
```

The one-argument form uses `lg_max_k = 12`. This gives existing queries a stable and conservative precision and memory limit instead of deriving the limit from whichever sketch happens to be processed first.

The two-argument form accepts a constant integer in the inclusive range `[7, 21]`:

```sql
SELECT DATASKETCHES_HLL_UNION_AGG(sketch_column, 16)
FROM sketch_table;
```

The same optional argument is supported by the aliases:

```sql
DS_HLL_ESTIMATE(sketch_column, 16)
DATASKETCHES_HLL_ESTIMATE(sketch_column, 16)
```

The parameter is validated by both FE and BE. Non-constant, non-integral, null, or out-of-range values are rejected.

### Important Behavior and Upgrade Notes

- `lg_max_k` is a strict upper bound, not a requested final precision. A dense input sketch with a smaller `lgK` can reduce the union's effective `lgK`, because a lower-precision dense sketch cannot be upsampled to recover information that is no longer present.
- With the default value of 12, a dense input sketch whose `lgK` is greater than 12 is intentionally downsampled to 12. Users who need to retain a higher available precision must specify an appropriate value explicitly.
- Sparse LIST/SET sketches are merged as coupons and do not immediately allocate a dense HLL array. The configured limit takes effect if the union later transitions to HLL mode or consumes a dense sketch.
- The dense union gadget uses HLL_8 storage and requires approximately `2^lg_max_k` bytes per aggregate state, excluding object and allocator overhead. `lg_max_k=12` is approximately 4 KiB, while `lg_max_k=21` is approximately 2 MiB. Grouped aggregation can hold many such states concurrently, so higher values should be selected with the query's group cardinality and memory limit in mind.
- Updating the submodule from 5.2.0 to `46025e9` includes 129 upstream commits because no newer DataSketches C++ release contains the required fix. The dependency remains pinned to the exact reviewed commit rather than following the upstream branch.
- The serialized value remains a DataSketches compact HLL sketch. Estimates and serialized bytes can change for merge sequences affected by the upstream bug; callers must not rely on byte-for-byte equality between sketches produced before and after this update.
- Existing one-argument queries remain valid. Their precision limit is now deterministic at 12 instead of depending on the first processed serialized sketch.

### Validation

The following validation was performed on the remote Doris development host:

- DataSketches upstream HLL test suite.
- Doris BE unit tests for `DATASKETCHES_HLL_UNION_AGG`, including empty, sparse, dense, mixed-`lgK`, serialization, aliases, explicit limits, and invalid limits.
- Doris FE unit tests for signatures and `lg_max_k` validation.
- Doris regression suite for `DATASKETCHES_HLL_UNION_AGG` in generated-output and normal verification modes.
- ASAN BE build.
- FE build and Checkstyle.
- BE format check and clang-tidy checks for changed production lines.
