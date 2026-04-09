# BE Exec Module — Review Guide

## Pipeline Execution

### Operator Lifecycle

Tasks move through `INITED → RUNNABLE → BLOCKED → FINISHED → FINALIZED`.

- [ ] `SharedState` source/sink dependencies connected through `inject_shared_state()`?

### Memory Reservation and Spill

- [ ] Memory-heavy operators use `_try_to_reserve_memory()` before materializing large structures?
- [ ] `_memory_sufficient_dependency` wired in where pressure should block, not overrun?
- [ ] `revoke_memory()` preserves the existing spill path?

## Dependency Concurrency

- [ ] Default readiness preserved? Source starts blocked; sink starts ready
- [ ] `set_ready()` fast-path precheck vs `is_blocked_by()` lock-first asymmetry respected?
- [ ] New `Dependency` subclasses pair `block()` / `set_ready()` on every path?
- [ ] `CountedFinishDependency::add()` and `sub()` under `_mtx`?

## Atomics

- [ ] Relaxed atomics only for statistics; lifecycle/stop flags use at least acquire/release?
