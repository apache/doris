# Quick Start Guide - Applying the Temporary Table Case Fix

## Files Generated

1. **temp_table_case_fix.patch** (29KB)
   - Complete git patch with all changes
   - Ready to apply with `git apply` or `patch -p1`

2. **PATCH_SUMMARY.md**
   - Comprehensive documentation of the fix
   - Root cause analysis, solution details, testing results

3. **COMMIT_MSG.txt**
   - Suggested commit message for git commit

## Option 1: Apply Patch Directly (Recommended)

```bash
cd /path/to/apache/doris

# Apply the patch
git apply temp_table_case_fix.patch

# Or use patch command
patch -p1 < temp_table_case_fix.patch

# Stage changes
git add fe/fe-core/src/main/java/org/apache/doris/catalog/Database.java \
        fe/fe-core/src/main/java/org/apache/doris/common/FeNameFormat.java \
        fe/fe-core/src/main/java/org/apache/doris/common/util/Util.java \
        fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/ShowTableStatusCommand.java \
        fe/fe-core/src/test/java/org/apache/doris/common/util/UtilTest.java \
        fe/fe-core/src/test/java/org/apache/doris/nereids/trees/plans/commands/ShowTableStatusCommandTest.java \
        regression-test/suites/temp_table_p0/

# Commit with suggested message
git commit -F COMMIT_MSG.txt

# Or edit the commit message
git commit
```

## Option 2: Create PR Branch

```bash
cd /path/to/apache/doris

# Create feature branch from your target base (e.g., master or branch-4.0)
git checkout -b fix/temp-table-case-sensitivity-57773

# Apply patch
git apply temp_table_case_fix.patch

# Review changes
git status
git diff --cached

# Commit
git add -A
git commit -F COMMIT_MSG.txt

# Push to your fork
git push origin fix/temp-table-case-sensitivity-57773

# Create PR on GitHub pointing to apache/doris
```

## Verification After Applying

### 1. Set configuration
Edit `conf/fe.conf`:
```properties
lower_case_table_names = 1
```

### 2. Rebuild FE
```bash
cd fe
mvn clean install -DskipTests
```

### 3. Start FE
```bash
bin/start_fe.sh
```

### 4. Run regression tests
```bash
# Scenario 1: CREATE TEMP LIKE on AUTO LIST PARTITION
mysql -h127.0.0.1 -P9030 -uroot < regression-test/suites/temp_table_p0/test_temp_like.sql

# Expected output:
# Scenario1_after_first_insert    1
# Scenario1_after_second_insert   2

# Scenario 2: INSERT OVERWRITE on temp table
mysql -h127.0.0.1 -P9030 -uroot < regression-test/suites/temp_table_p0/test_temp_overwrite.sql

# Expected output:
# Scenario2_after_create_as       1
# Scenario2_after_insert          2
# Scenario2_after_overwrite       1
```

### 5. Run unit tests
```bash
cd fe
mvn test -Dtest=UtilTest
mvn test -Dtest=ShowTableStatusCommandTest
```

## Files Modified

### Source Code (6 files)
- `fe/fe-core/src/main/java/org/apache/doris/catalog/Database.java`
- `fe/fe-core/src/main/java/org/apache/doris/common/FeNameFormat.java`
- `fe/fe-core/src/main/java/org/apache/doris/common/util/Util.java`
- `fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/ShowTableStatusCommand.java`
- `fe/fe-core/src/test/java/org/apache/doris/common/util/UtilTest.java`
- `fe/fe-core/src/test/java/org/apache/doris/nereids/trees/plans/commands/ShowTableStatusCommandTest.java`

### Regression Tests (4 new files)
- `regression-test/suites/temp_table_p0/test_temp_like.sql`
- `regression-test/suites/temp_table_p0/test_temp_like.out`
- `regression-test/suites/temp_table_p0/test_temp_overwrite.sql`
- `regression-test/suites/temp_table_p0/test_temp_overwrite.out`

## Key Changes Summary

1. **Database.java** (37 lines added)
   - registerTable: Maps temp display name → internal name
   - getTableNullable: Resolves temp inner names in mode=1

2. **Util.java** (32 lines modified)
   - Case-insensitive temp table utilities
   - generateTempTableInnerName, getTempTableDisplayName, isTempTable

3. **FeNameFormat.java** (8 lines modified)
   - Added TEMPORARY_TABLE_SIGN_LOWER constant

4. **ShowTableStatusCommand.java** (28 lines modified)
   - LOWER() applied to temp marker filtering

5. **Unit Tests** (243 lines added)
   - Comprehensive coverage of temp table utilities
   - Tests for all three modes (0, 1, 2)

6. **Regression Tests** (63 lines added)
   - Two end-to-end scenarios validating the fix

## Troubleshooting

### Patch doesn't apply cleanly
```bash
# Check which files conflict
git apply --check temp_table_case_fix.patch

# Apply with 3-way merge
git apply -3 temp_table_case_fix.patch

# Or manually resolve conflicts
git apply --reject temp_table_case_fix.patch
# Then edit .rej files and fix conflicts
```

### Tests fail
- Ensure `lower_case_table_names=1` is set in `fe.conf`
- Restart FE after configuration change
- Check BE is running and connected
- Review FE logs: `fe/log/fe.log`

### Need to modify patch
```bash
# Extract specific files from patch
git apply temp_table_case_fix.patch -- fe/fe-core/src/main/java/org/apache/doris/catalog/Database.java

# Or manually edit patch file before applying
vim temp_table_case_fix.patch
```

## Contact & Support

- Issue: https://github.com/apache/doris/issues/57773
- Mailing list: dev@doris.apache.org
- Slack: https://join.slack.com/t/apachedoriscommunity

## Related Documentation

- MySQL `lower_case_table_names`: https://dev.mysql.com/doc/refman/8.0/en/identifier-case-sensitivity.html
- Doris temporary tables: https://doris.apache.org/docs/sql-manual/sql-statements/Data-Definition-Statements/Create/CREATE-TABLE
