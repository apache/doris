# Fix: Temporary Table Case Sensitivity with lower_case_table_names=1

## Issue
Fixes [#57773](https://github.com/apache/doris/issues/57773)

When `lower_case_table_names=1` (stored-lowercase mode), temporary tables failed in several scenarios:
1. **CREATE TEMPORARY TABLE ... LIKE** on AUTO LIST PARTITION tables failed with "Table [<name>] does not exist"
2. **INSERT OVERWRITE TABLE** on temporary tables failed with "Unknown table <internal_temp_name>"

## Root Cause
Temporary tables use an internal naming pattern: `sessionId_#TEMP#_<displayName>`. When `lower_case_table_names=1`:
- The internal name is stored in `nameToTable` map
- The `lowerCaseToTableName` map only mapped the internal name (lowercased) to itself
- **Missing mapping**: display name (lowercased) → internal name
- Result: Lookups by display name failed because there was no reverse resolution

## Solution

### 1. Database.java
**registerTable(TableIf table)**
- Added mapping from temp display name (lowercased) to internal temp name in `lowerCaseToTableName`
- This enables resolution when users query by display name in mode=1

```java
if (olapTable.isTemporary() && displayNameLower != null) {
    lowerCaseToTableName.put(displayNameLower, tableName);
}
```

**getTableNullable(String tableName)**
- Extended temp inner name resolution to work in both mode=2 (case-insensitive) and mode=1 (stored-lowercase)
- Attempt to resolve the temp inner name via `lowerCaseToTableName` before lookup
- Prefer temp table if found, otherwise fallback to regular table

```java
else if (Env.isStoredTableNamesLowerCase()) {
    String resolvedTempName = lowerCaseToTableName.get(tempTableInnerName.toLowerCase());
    if (resolvedTempName != null) {
        tempTableInnerName = resolvedTempName;
    }
}
```

### 2. Util.java
**Normalized temporary table utilities**
- `generateTempTableInnerName(String name)`: Creates internal temp name consistently
- `getTempTableDisplayName(String internalName)`: Extracts user-visible display name
- `isTempTable(String name)`: Case-insensitive temp table detection

All helpers now use case-insensitive comparisons with the temp marker to ensure robust behavior across modes.

### 3. FeNameFormat.java
**Added TEMPORARY_TABLE_SIGN_LOWER constant**
- Provides lowercase version of temporary table marker for consistent comparisons
- Used by utilities to avoid repeated `toLowerCase()` calls

### 4. ShowTableStatusCommand.java
**Case-insensitive filtering**
- Applied `LOWER()` to temp table marker filtering to ensure correct predicate behavior in all modes

### 5. Test Coverage
**Added regression-test/suites/temp_table_p0/**

**test_temp_like.sql / .out** - Scenario 1:
- Creates AUTO LIST PARTITION source table
- Creates TEMPORARY TABLE LIKE source (with mixed-case name)
- Inserts rows and validates counts (1, then 2)
- Verifies CREATE TEMP LIKE works on partitioned tables

**test_temp_overwrite.sql / .out** - Scenario 2:
- CREATE TEMPORARY TABLE AS SELECT (1 row)
- INSERT adds second row (count=2)
- INSERT OVERWRITE replaces with single row (count=1)
- Verifies INSERT OVERWRITE resolves temp table correctly

**Unit tests**:
- UtilTest.java: Comprehensive tests for temp table utilities across all modes
- ShowTableStatusCommandTest.java: Tests for filtering logic

## Behavior Changes

### Before
- Mode=1: Temp tables created successfully but failed on subsequent operations referencing display name
- CREATE TEMP LIKE on partitioned tables: ❌ "Table does not exist"
- INSERT OVERWRITE on temp tables: ❌ "Unknown table"

### After
- Mode=1: Temp tables work correctly with display name references in all cases
- CREATE TEMP LIKE on partitioned tables: ✅ Works
- INSERT OVERWRITE on temp tables: ✅ Works
- Mode=0 and Mode=2: No behavior change (already working)

## Compatibility
- **Mode 0 (case-sensitive)**: No change
- **Mode 1 (stored-lowercase)**: Fixed (users can now reference temp tables by display name)
- **Mode 2 (case-insensitive)**: No behavior change, added temp inner name resolution for consistency

## Testing

### Manual Validation
Both scenarios validated against running FE (port 9030) with `lower_case_table_names=1`:

```bash
mysql --protocol=TCP -h127.0.0.1 -P9030 -uroot --silent < regression-test/suites/temp_table_p0/test_temp_like.sql
# Output: Scenario1_after_first_insert 1
#         Scenario1_after_second_insert 2

mysql --protocol=TCP -h127.0.0.1 -P9030 -uroot --silent < regression-test/suites/temp_table_p0/test_temp_overwrite.sql
# Output: Scenario2_after_create_as 1
#         Scenario2_after_insert 2
#         Scenario2_after_overwrite 1
```

### Files Changed
```
 fe/fe-core/src/main/java/org/apache/doris/catalog/Database.java             |  37 +++++-
 fe/fe-core/src/main/java/org/apache/doris/common/FeNameFormat.java          |   8 +-
 fe/fe-core/src/main/java/org/apache/doris/common/util/Util.java             |  32 +++--
 .../apache/doris/nereids/trees/plans/commands/ShowTableStatusCommand.java   |  28 +++--
 fe/fe-core/src/test/java/org/apache/doris/common/util/UtilTest.java         | 229 ++++++++++++++++++++++++
 .../doris/nereids/trees/plans/commands/ShowTableStatusCommandTest.java      |  14 +++
 regression-test/suites/temp_table_p0/test_temp_like.out                     |   2 +
 regression-test/suites/temp_table_p0/test_temp_like.sql                     |  37 ++++++
 regression-test/suites/temp_table_p0/test_temp_overwrite.out                |   3 +
 regression-test/suites/temp_table_p0/test_temp_overwrite.sql                |  21 ++++
 10 files changed, 380 insertions(+), 31 deletions(-)
```

## How to Apply

### Option 1: Apply patch file
```bash
cd /path/to/doris
patch -p1 < temp_table_case_fix.patch
```

### Option 2: Manual cherry-pick (if creating PR from different branch)
1. Review changes in the 6 modified Java files
2. Add the 4 new regression test files
3. Rebuild FE: `cd fe && mvn clean install -DskipTests`
4. Run regression tests to validate

## Verification Steps
1. Set `lower_case_table_names=1` in `fe.conf`
2. Restart FE
3. Run both regression tests:
   - `test_temp_like.sql` should output counts 1, 2
   - `test_temp_overwrite.sql` should output counts 1, 2, 1
4. Try creating temp tables with mixed-case names and various operations

## Notes
- This fix is scoped to FE-only changes; no BE modifications required
- All changes are backward compatible
- Existing temp table functionality in mode=0 and mode=2 unaffected
- The fix properly handles the distinction between stored-lowercase (mode=1) and case-insensitive (mode=2) modes
