# Authentication Chain Config Simplification Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `authentication_chain` the only MySQL fallback control by removing `enable_authentication_chain`, `enable_jit_user_authentication_chain`, and `authentication_chain_fallback_policy`.

**Architecture:** Keep primary authenticator selection unchanged and simplify only the post-failure fallback gates in `AuthenticatorManager`. Remove the deleted config fields from FE config definitions and rewrite focused FE unit tests to assert chain-empty versus chain-configured behavior.

**Tech Stack:** Java, JUnit, Doris FE authentication flow

---

## Chunk 1: Tests First

### Task 1: Rewrite failing auth-manager tests for chain-only semantics

**Files:**
- Modify: `fe/fe-core/src/test/java/org/apache/doris/mysql/authenticate/AuthenticatorManagerTest.java`

- [ ] **Step 1: Write the failing test changes**

Replace setup/teardown and policy-specific assertions so tests use only `Config.authentication_chain`.

- [ ] **Step 2: Run test to verify it fails**

Run: `./run-fe-ut.sh --run org.apache.doris.mysql.authenticate.AuthenticatorManagerTest`
Expected: FAIL because production code still references removed config gates or old behavior.

## Chunk 2: Production Code

### Task 2: Remove obsolete config fields

**Files:**
- Modify: `fe/fe-common/src/main/java/org/apache/doris/common/Config.java`

- [ ] **Step 1: Delete removed config fields**

Remove:

- `enable_authentication_chain`
- `enable_jit_user_authentication_chain`
- `authentication_chain_fallback_policy`

- [ ] **Step 2: Keep only `authentication_chain` as chain control**

Retain `authentication_type` and `authentication_chain` descriptions aligned with the new behavior.

### Task 3: Simplify auth fallback logic

**Files:**
- Modify: `fe/fe-core/src/main/java/org/apache/doris/mysql/authenticate/AuthenticatorManager.java`

- [ ] **Step 1: Remove policy and config-gate branching**

Delete the checks tied to the removed config fields.

- [ ] **Step 2: Make chain presence the only fallback condition**

When primary auth fails, only parsed `authentication_chain` decides whether chain auth is attempted.

- [ ] **Step 3: Update logs and helpers**

Remove stale fallback-policy log text and any helpers that exist only for deleted config behavior.

## Chunk 3: Verification

### Task 4: Turn tests green

**Files:**
- Test: `fe/fe-core/src/test/java/org/apache/doris/mysql/authenticate/AuthenticatorManagerTest.java`

- [ ] **Step 1: Run the focused FE unit test**

Run: `./run-fe-ut.sh --run org.apache.doris.mysql.authenticate.AuthenticatorManagerTest`
Expected: PASS

- [ ] **Step 2: Search for stale config references**

Run: `rg -n "enable_authentication_chain|enable_jit_user_authentication_chain|authentication_chain_fallback_policy" fe`
Expected: no production references remain for the removed config keys.

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/2026-03-16-authentication-chain-config-simplification-design.md docs/superpowers/plans/2026-03-16-authentication-chain-config-simplification.md fe/fe-common/src/main/java/org/apache/doris/common/Config.java fe/fe-core/src/main/java/org/apache/doris/mysql/authenticate/AuthenticatorManager.java fe/fe-core/src/test/java/org/apache/doris/mysql/authenticate/AuthenticatorManagerTest.java
git commit -m "simplify authentication chain fallback config"
```
