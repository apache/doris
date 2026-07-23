# Nereids Optimizer — Review Guide

## Rules

- [ ] New rule has correct `RuleType` and registration path?
- [ ] Join rewrites handle all `JoinType` values and preserve mark-join semantics (`markJoinSlotReference`)?
- [ ] Constant predicates and `containsUniqueFunction()` handled before pushdown/inference?
- [ ] No rewrite-loop risk from nodes recreated into a shape matched again in the same stage?
- [ ] Wrapper-shape requirements (e.g. `project(join)`) preserved?

## Stage Order

- [ ] New rewrite rule placed in correct stage w.r.t. dependencies (esp. `PUSH_DOWN_FILTERS`, `InferPredicates`)?
- [ ] Exploration/implementation rules in correct `RuleSet` entry points?

## CTE Producer Rules

- [ ] Rules that modify `LogicalCTEProducer` output must preserve slot order — ExprIds can be replaced, but the sequence must not be reordered. Non-deterministic iteration (e.g., `HashMap.entrySet()`) over output slots is prohibited; use insertion-order-preserving structures (`LinkedHashMap`, or iterate the original `agg.getOutputExpressions()` order) when building projections atop a producer. Reordering breaks `syncCteConsumerSlotMaps` which relies on position-based alignment between old and new producer outputs.

## Property Derivation

- [ ] New physical operator has `RequestPropertyDeriver` logic?
- [ ] All viable property alternatives exposed, not prematurely narrowed to one distribution?
- [ ] Mark join restriction: standalone mark join is broadcast-only?
- [ ] `PhysicalProject` alias penetration limited to bare `SlotRef` and `Alias(SlotRef)`?
