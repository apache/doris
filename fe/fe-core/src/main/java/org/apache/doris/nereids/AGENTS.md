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

## Property Derivation

- [ ] New physical operator has `RequestPropertyDeriver` logic?
- [ ] All viable property alternatives exposed, not prematurely narrowed to one distribution?
- [ ] Mark join restriction: standalone mark join is broadcast-only?
- [ ] `PhysicalProject` alias penetration limited to bare `SlotRef` and `Alias(SlotRef)`?
