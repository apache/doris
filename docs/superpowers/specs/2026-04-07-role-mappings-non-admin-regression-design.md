# Role Mappings Non-Admin Visibility Regression Design

## Goal

Add regression coverage for the `information_schema.role_mappings` visibility rule so the branch explicitly verifies that non-`ADMIN` users cannot read role-mapping metadata.

## Current Behavior

`role_mappings` is exposed through FE schema-table metadata generation. The FE code checks `checkGlobalPriv(currentUserIdentity, PrivPredicate.ADMIN)` before materializing rows. When the caller lacks `ADMIN`, FE returns `OK` with an empty result set instead of raising an access error.

## Proposed Change

Add a regression-style FE test beside the existing positive schema-table test:

- Keep the current `ADMIN` positive case unchanged.
- Add a new negative case that:
  - creates a role, authentication integration, and role mapping as the default admin test context
  - creates a regular user without `ADMIN`
  - queries `ROLE_MAPPINGS` using that user's thrift identity
  - asserts status is `OK`
  - asserts the result set is empty

## Why This Scope

The behavior under test already exists in production code. The missing piece is coverage for the privilege gate's non-admin branch. A targeted FE regression test is the smallest change that protects the behavior without changing runtime logic.

## Risks

- The test must not accidentally rely on the current admin connect context when building the fetch request.
- Cleanup must drop the created role mapping, integration, user, and roles in a safe order.

## Validation

- Watch the new targeted test fail first.
- Run the same targeted test after the test change until it passes.
- If feasible, run the full `FrontendServiceImplTest` class as a follow-up check.
