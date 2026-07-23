[fix](exec) preserve NULL in pushed-down CHAR MIN/MAX

### What problem does this PR solve?

Issue Number: N/A

Problem Summary: `MIN` on a nullable CHAR column could return an empty string when string min/max aggregation was pushed down to multiple tablets. The zone-map reader produced NULL correctly, but the CHAR padding cleanup rebuilt each value through a non-null `Field`, converting `StringRef{nullptr, 0}` into an empty string. Preserve the original StringRef through `insert_data()` so NULL remains NULL while CHAR padding is still removed.

### Release note

Fix incorrect `MIN` results for nullable CHAR columns when min/max aggregation pushdown is enabled.

### Check List (For Author)

- Test
    - [x] Manual test: verified the multi-bucket nullable CHAR reproduction returns `'0'` for `MIN(c)` after the fix.
    - [x] Unit Test: added `VGenericIteratorsTest.StatisticsIteratorPreservesNullForNullableChar`.
    - [ ] Regression test
- Behavior changed:
    - [x] Yes. Nullable CHAR `MIN` now preserves SQL NULL semantics.
- Does this need documentation:
    - [x] No.

### Check List (For Reviewer who merge this PR)

- [ ] Confirm the release note
- [ ] Confirm test cases
- [ ] Confirm document
- [ ] Add branch pick label
