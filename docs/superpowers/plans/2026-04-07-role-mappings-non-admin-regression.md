# Role Mappings Non-Admin Regression Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add regression coverage proving non-`ADMIN` users receive an empty `ROLE_MAPPINGS` schema-table result.

**Architecture:** Extend the existing FE schema-table regression test class with one negative privilege test. Reuse the same setup style as the current positive `ROLE_MAPPINGS` test so the new case exercises the real FE metadata path while isolating the requesting identity.

**Tech Stack:** JUnit 4, FE utframe test cluster, Nereids command execution, FE schema-table thrift APIs

---

## Chunk 1: Targeted Regression Test

### Task 1: Add the failing test

**Files:**
- Modify: `fe/fe-core/src/test/java/org/apache/doris/service/FrontendServiceImplTest.java`

- [ ] **Step 1: Write the failing test**

Add a test that creates a role mapping, then fetches `TSchemaTableName.ROLE_MAPPINGS` using a non-admin user's thrift identity and expects `OK` plus an empty `dataBatch`.

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl fe/fe-core -Dskip.plugin=true -DskipITs -Dtest=FrontendServiceImplTest#testFetchRoleMappingsSchemaTableDataWithoutAdmin test`

Expected: FAIL because the test method does not exist yet.

- [ ] **Step 3: Write minimal implementation**

Add the new JUnit method and any minimal helper imports needed. Reuse existing command helpers and cleanup patterns.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl fe/fe-core -Dskip.plugin=true -DskipITs -Dtest=FrontendServiceImplTest#testFetchRoleMappingsSchemaTableDataWithoutAdmin test`

Expected: PASS.

### Task 2: Run a broader verification pass

**Files:**
- Modify: `fe/fe-core/src/test/java/org/apache/doris/service/FrontendServiceImplTest.java`

- [ ] **Step 1: Run the focused class verification**

Run: `mvn -pl fe/fe-core -Dskip.plugin=true -DskipITs -Dtest=FrontendServiceImplTest test`

Expected: PASS.

- [ ] **Step 2: Commit**

Run:

```bash
git add docs/superpowers/specs/2026-04-07-role-mappings-non-admin-regression-design.md \
        docs/superpowers/plans/2026-04-07-role-mappings-non-admin-regression.md \
        fe/fe-core/src/test/java/org/apache/doris/service/FrontendServiceImplTest.java
git commit -m "[test](fe) Add regression for non-admin role mapping visibility"
```
