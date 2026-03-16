# Authentication Chain Config Simplification Design

**Date:** 2026-03-16

## Goal

Make `authentication_chain` the only configuration that controls whether MySQL login falls back to authentication integrations after primary authentication fails.

## Scope

This change removes three FE configs from the MySQL authentication fallback flow:

- `enable_authentication_chain`
- `enable_jit_user_authentication_chain`
- `authentication_chain_fallback_policy`

The primary authenticator selection logic remains unchanged. In particular, `AuthenticatorManager` still chooses the primary authenticator through `authTypeAuthenticator.canDeal(user)` and falls back to `DefaultAuthenticator` when the configured primary authenticator cannot handle the user.

## Behavioral Design

### Primary authentication

Primary authentication continues to be selected from `authentication_type` and the existing `canDeal` logic:

- LDAP only handles non-`root`/`admin` users that exist in LDAP
- plugin-based primary authenticators still exclude `root` and `admin`
- if the configured primary authenticator cannot handle the user, Doris falls back to `DefaultAuthenticator`

This preserves existing compatibility with the old `rich/master` behavior.

### Authentication chain fallback

After primary authentication fails:

- if `authentication_chain` is empty after parsing, authentication fails
- if `authentication_chain` is non-empty, Doris attempts the integration chain

The fallback no longer depends on:

- whether the user already exists in Doris
- whether the user exists in LDAP
- a separate chain enable switch
- a fallback policy setting
- a separate JIT-chain enable switch

### JIT semantics

The dedicated JIT fallback gate is removed, but JIT user creation behavior inside integration/plugin authenticators is unchanged:

- successful chain auth still maps to an existing Doris user when one exists
- otherwise the integration-level `enable_jit_user` property still determines whether a temporary user may be returned

## Implementation Notes

### Config cleanup

Delete the three FE config fields and their descriptions from `Config.java`.

### Auth flow cleanup

Simplify `AuthenticatorManager` so both fallback attempts are controlled only by parsed `authentication_chain`.

Keep the existing two-step structure if it minimizes code churn:

- first try the chain in the existing JIT-oriented branch
- then try the general fallback branch

But both branches must derive their eligibility from chain presence only, not from removed config flags or fallback policy.

### Test updates

Update `AuthenticatorManagerTest` to express only two meaningful states:

- chain empty: no fallback
- chain configured: fallback attempted after primary failure

Existing tests that asserted policy-specific behavior should be rewritten or removed.

## Risks

- tests or docs may still reference removed config keys
- logs mentioning fallback policy will become stale and must be updated
- any caller that relied on policy-specific branching will now always enter the chain when configured

## Verification

Run focused FE unit tests covering `AuthenticatorManager` and review the resulting diff for any remaining references to the removed config keys.
