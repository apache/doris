# BE Unit Test - Review Guide

## Access Control

BE-UT has actually been configured to ignore access control, so it can access all private interfaces.

## Including src .cpp files

- [ ] A test that `#include`s a be/src `.cpp` (to reach file-static helpers) must have that file opted out of unity batching via `doris_skip_unity_inclusion` in the owning `be/src/.../CMakeLists.txt` — otherwise the batch object's copy of the definitions collides with the test's inlined copy and the BE UT link fails with duplicate symbols. Enforced at configure time by `build-support/check-unity-skip-coverage.py`; its error message names the exact entry to add. Prefer not including `.cpp` files at all when the code can be exercised through a header.
