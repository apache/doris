# Fix PreparedStatement.getMetaData() returning null

## What problem does this PR solve?

Issue Number: close #59037

Problem Summary:
When using PreparedStatement in Apache Doris, calling getMetaData() method returns null instead of the expected metadata information. This issue occurs because the numColumns field in the COM_STMT_PREPARE response was hardcoded to 0, which prevented clients from retrieving the result set metadata.

Specifically, when using `useServerPrepStmts=false` parameter, the JDBC client executes `con.prepareStatement("select 1").getMetaData()` and returns null in Doris 2.1.11, while it works correctly in other versions.

## What is changed and how it works?

This PR addresses the issue by dynamically calculating the actual number of result columns based on the parsed statement type, instead of hardcoding it to 0. The changes include:

1. Modified the sendStmtPrepareOK method in StmtExecutor to calculate numColumns based on:
   - SelectStmt: Using getColLabels() method
   - LogicalPlanAdapter: Using getColLabels(), getFieldInfos(), or getResultExprs() methods
   - ShowStmt: Using getMetaData() method

2. Added proper handling for warning_count and metadata_follows fields that should be sent when there are parameters or columns

3. Enhanced the test cases to verify the correct number of result columns is sent in the response for different statement types:
   - testSendStmtPrepareOKWithResultColumns: Tests with SQL "SELECT col1, col2, col3 FROM test WHERE id = ?"
   - testSendStmtPrepareOKWithLogicalPlanAdapterNullColLabels: Tests with SQL "SELECT col1, col2, col3 FROM test WHERE id = ?" when colLabels is null but fieldInfos exists
   - testSendStmtPrepareOKWithShowStmtAndNullMetadata: Tests with SQL "SHOW DATABASES" when metadata is null

This change ensures that when clients call PreparedStatement.getMetaData(), they receive proper metadata instead of null, following the MySQL protocol specification for COM_STMT_PREPARE response format.

## Check List (For Author)

- Test <!-- At least one of them must be included. -->
    - [x] Regression test
    - [x] Unit Test
    - [ ] Manual test (add detailed scripts or steps below)
    - [ ] No need to test or manual test. Explain why:
        - [ ] This is a refactor/code format and no logic has been changed.
        - [ ] Previous test can cover this change.
        - [ ] No code files have been changed.
        - [ ] Other reason <!-- Add your reason?  -->

- Behavior changed:
    - [x] No.
    - [ ] Yes. <!-- Explain the behavior change -->

- Does this need documentation?
    - [ ] No.
    - [ ] Yes. <!-- Add document PR link here. eg: https://github.com/apache/doris-website/pull/1214 -->

## Check List (For Reviewer who merge this PR)

- [ ] Confirm the release note
- [ ] Confirm test cases
- [ ] Confirm document
- [ ] Add branch pick label <!-- Add branch pick label that this PR should merge into -->