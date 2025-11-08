# Detailed Code Changes Explanation

This document walks through every code change with context and rationale.

---

## 1. Database.java - registerTable() Method

**File:** `fe/fe-core/src/main/java/org/apache/doris/catalog/Database.java`

### Change: Add display name mapping for temporary tables

**Before:**
```java
public boolean registerTable(TableIf table) {
    boolean result = true;
    Table olapTable = (Table) table;
    olapTable.setQualifiedDbName(fullQualifiedName);
    String tableName = olapTable.getName();
    if (Env.isStoredTableNamesLowerCase()) {
        tableName = tableName.toLowerCase();
    }
    if (isTableExist(tableName)) {
        result = false;
    } else {
        idToTable.put(olapTable.getId(), olapTable);
        nameToTable.put(olapTable.getName(), olapTable);
        lowerCaseToTableName.put(tableName.toLowerCase(), tableName);
        // ... rest of method
    }
    // ...
}
```

**After:**
```java
public boolean registerTable(TableIf table) {
    boolean result = true;
    Table olapTable = (Table) table;
    olapTable.setQualifiedName(fullQualifiedName);
    String tableName = olapTable.getName(); // stored (internal for temp)
    String displayNameLower = null;
    if (olapTable.isTemporary()) {
        // derive display name (without session + #TEMP#) for reverse lookup
        String display = Util.getTempTableDisplayName(tableName);
        displayNameLower = display.toLowerCase();
    }
    String lookupName = tableName;
    if (Env.isStoredTableNamesLowerCase()) {
        lookupName = lookupName.toLowerCase();
    }
    if (isTableExist(lookupName)) {
        result = false;
    } else {
        idToTable.put(olapTable.getId(), olapTable);
        nameToTable.put(olapTable.getName(), olapTable);
        lowerCaseToTableName.put(lookupName.toLowerCase(), tableName);
        // In stored-lowercase mode (1), user will query by display name.
        // Add mapping from display lower-case name to internal temp name so getTableNullable(display) succeeds.
        if (olapTable.isTemporary() && displayNameLower != null) {
            lowerCaseToTableName.put(displayNameLower, tableName);
        }
        // ... rest of method
    }
    // ...
}
```

**Why this change:**
- Temp tables have internal names like `sessionId_#TEMP#_DisplayName`
- Users reference them by `DisplayName` (the display name)
- In mode=1, we lowercase for storage, but need both mappings:
  1. `sessionid_#temp#_displayname` → `sessionId_#TEMP#_DisplayName` (internal)
  2. `displayname` → `sessionId_#TEMP#_DisplayName` (display → internal)
- Without mapping #2, lookups by display name fail with "Table not found"

**Key insight:** We extract the display name using `Util.getTempTableDisplayName()` and create an additional mapping entry so users can query by either the internal or display name.

---

## 2. Database.java - getTableNullable() Method

**File:** `fe/fe-core/src/main/java/org/apache/doris/catalog/Database.java`

### Change: Resolve temp inner names in both mode=1 and mode=2

**Before:**
```java
public Table getTableNullable(String tableName) {
    if (Env.isStoredTableNamesLowerCase()) {
        tableName = tableName.toLowerCase();
    }
    if (Env.isTableNamesCaseInsensitive()) {
        tableName = lowerCaseToTableName.get(tableName.toLowerCase());
        if (tableName == null) {
            return null;
        }
    }

    // return temp table first
    Table table = nameToTable.get(Util.generateTempTableInnerName(tableName));
    if (table == null) {
        table = nameToTable.get(tableName);
    }

    return table;
}
```

**After:**
```java
public Table getTableNullable(String tableName) {
    if (Env.isStoredTableNamesLowerCase()) {
        tableName = tableName.toLowerCase();
    }
    if (Env.isTableNamesCaseInsensitive()) {
        tableName = lowerCaseToTableName.get(tableName.toLowerCase());
        if (tableName == null) {
            return null;
        }
    }

    // return temp table first
    String tempTableInnerName = Util.generateTempTableInnerName(tableName);
    // For case-insensitive mode, resolve the temp table inner name as well
    if (Env.isTableNamesCaseInsensitive()) {
        String resolvedTempName = lowerCaseToTableName.get(tempTableInnerName.toLowerCase());
        if (resolvedTempName != null) {
            tempTableInnerName = resolvedTempName;
        }
    } else if (Env.isStoredTableNamesLowerCase()) {
        // Mode 1 (store lower case) still needs to map back from lowercased temp inner name
        // because nameToTable stores the original (non-forced) name string, while we lowered tableName above.
        String resolvedTempName = lowerCaseToTableName.get(tempTableInnerName.toLowerCase());
        if (resolvedTempName != null) {
            tempTableInnerName = resolvedTempName;
        }
    }
    Table table = nameToTable.get(tempTableInnerName);
    if (table == null) {
        table = nameToTable.get(tableName);
    }

    return table;
}
```

**Why this change:**
- When `tableName = "DisplayName"` arrives, we generate the potential temp inner name
- But `generateTempTableInnerName()` produces a lowercased pattern
- We need to resolve that pattern back to the actual stored internal name
- This resolution is now needed in both mode=2 (was missing) and mode=1 (newly added)

**Flow example (mode=1):**
1. User queries: `SELECT * FROM Contacts_Test_GKL`
2. `tableName` becomes `"contacts_test_gkl"` (mode=1 lowercase)
3. Generate temp inner: `"sessionid_#temp#_contacts_test_gkl"` (lowercased)
4. Resolve via map: `lowerCaseToTableName["sessionid_#temp#_contacts_test_gkl"]` → `"sessionId_#TEMP#_contacts_test_gkl"` (actual stored)
5. Lookup: `nameToTable.get("sessionId_#TEMP#_contacts_test_gkl")` → ✅ found!

Without step 4, we'd try to lookup the lowercased version which doesn't exist in `nameToTable`.

---

## 3. Util.java - Temp Table Utilities

**File:** `fe/fe-core/src/main/java/org/apache/doris/common/util/Util.java`

### Change: Make temp table detection case-insensitive

**Key methods modified:**

#### generateTempTableInnerName()
```java
public static String generateTempTableInnerName(String tableName) {
    // Generate internal temp name using session + marker + display name
    return ConnectContext.get().getSessionVariable().getSessionContext()
            .getSessionId() + FeNameFormat.TEMPORARY_TABLE_SIGN + tableName;
}
```

#### getTempTableDisplayName()
```java
public static String getTempTableDisplayName(String internalName) {
    // Extract display name by removing session + #TEMP# prefix
    if (isTempTable(internalName)) {
        int idx = internalName.toLowerCase()
                .indexOf(FeNameFormat.TEMPORARY_TABLE_SIGN_LOWER);
        if (idx != -1) {
            return internalName.substring(
                idx + FeNameFormat.TEMPORARY_TABLE_SIGN_LOWER.length());
        }
    }
    return internalName;
}
```

#### isTempTable()
```java
public static boolean isTempTable(String tableName) {
    // Case-insensitive check for temp marker
    return tableName != null && 
           tableName.toLowerCase().contains(FeNameFormat.TEMPORARY_TABLE_SIGN_LOWER);
}
```

**Why these changes:**
- Original code did case-sensitive checks: `tableName.contains("#TEMP#")`
- In mode=1, names get lowercased, so we need to check `contains("#temp#")`
- Solution: Use `toLowerCase()` consistently and compare against lowercase marker
- This ensures utilities work regardless of input casing

**Impact:**
- `isTempTable("sessionId_#TEMP#_MyTable")` → true (before and after)
- `isTempTable("sessionid_#temp#_mytable")` → true (now works, failed before)

---

## 4. FeNameFormat.java - Lowercase Constant

**File:** `fe/fe-core/src/main/java/org/apache/doris/common/FeNameFormat.java`

### Change: Add lowercase variant of temp marker

**Added:**
```java
public static final String TEMPORARY_TABLE_SIGN = "_#TEMP#_";
public static final String TEMPORARY_TABLE_SIGN_LOWER = "_#temp#_";  // NEW
```

**Why:**
- Avoid repeated `TEMPORARY_TABLE_SIGN.toLowerCase()` calls throughout code
- Centralize the lowercase constant for consistency
- Used by `Util.java` and other temp table detection logic

**Simple but critical:** This makes case-insensitive checks faster and more maintainable.

---

## 5. ShowTableStatusCommand.java - Filter Logic

**File:** `fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/ShowTableStatusCommand.java`

### Change: Apply LOWER() to temp table filtering predicate

**Before:**
```java
// Filter predicate checked: tableName.contains("_#TEMP#_")
where = Optional.of(new Like(
    new UnboundSlot("Name"),
    new StringLiteral("%_#TEMP#_%")
));
```

**After:**
```java
// Filter predicate now checks: LOWER(tableName) LIKE '%_#temp#_%'
where = Optional.of(new Like(
    new Lower(new UnboundSlot("Name")),
    new StringLiteral("%_#temp#_%")
));
```

**Why:**
- Ensures `SHOW TABLE STATUS WHERE Name LIKE '%temp%'` works correctly
- In mode=1, stored names might be `"sessionid_#temp#_mytable"`
- Without `LOWER()`, the filter might miss tables depending on how they're stored
- With `LOWER()`, we normalize for consistent filtering across all modes

**Scenario prevented:**
- User creates temp table `MyTempTable`
- Mode=1 stores as `sessionid_#temp#_mytemptable`
- `SHOW TABLE STATUS` without LOWER() might not detect it correctly
- With LOWER(), detection is reliable

---

## 6. Unit Tests - UtilTest.java

**File:** `fe/fe-core/src/test/java/org/apache/doris/common/util/UtilTest.java`

### Added comprehensive test coverage (229 lines)

**Test categories:**

1. **testIsTempTable()** - Verify temp table detection
```java
@Test
public void testIsTempTable() {
    assertTrue(Util.isTempTable("session123_#TEMP#_MyTable"));
    assertTrue(Util.isTempTable("session123_#temp#_mytable"));  // lowercase
    assertFalse(Util.isTempTable("NormalTable"));
    assertFalse(Util.isTempTable("MyTable_#TEMP"));  // wrong format
}
```

2. **testGetTempTableDisplayName()** - Verify display name extraction
```java
@Test
public void testGetTempTableDisplayName() {
    assertEquals("MyTable", 
        Util.getTempTableDisplayName("session123_#TEMP#_MyTable"));
    assertEquals("mytable", 
        Util.getTempTableDisplayName("session123_#temp#_mytable"));
}
```

3. **testGenerateTempTableInnerName()** - Verify internal name generation
```java
@Test
public void testGenerateTempTableInnerName() {
    String internal = Util.generateTempTableInnerName("MyTable");
    assertTrue(internal.contains("_#TEMP#_MyTable"));
    assertTrue(Util.isTempTable(internal));
}
```

4. **Mode-specific tests** - Verify behavior in each mode (0, 1, 2)

**Why these tests:**
- Prevent regressions in temp table utilities
- Validate case-insensitive behavior across all modes
- Ensure display name ↔ internal name conversions work correctly
- Document expected behavior through examples

---

## 7. Unit Tests - ShowTableStatusCommandTest.java

**File:** `fe/fe-core/src/test/java/org/apache/doris/nereids/trees/plans/commands/ShowTableStatusCommandTest.java`

### Added tests for filtering logic (14 lines)

**Test added:**
```java
@Test
public void testTempTableFiltering() {
    // Verify LOWER() is applied in temp table filter
    ShowTableStatusCommand cmd = new ShowTableStatusCommand(
        "db", "%_#TEMP#_%");
    
    // Check that filter expression uses Lower() function
    assertTrue(cmd.getWhereClause().toString().contains("lower("));
}
```

**Why this test:**
- Ensures the LOWER() application isn't accidentally removed
- Validates filtering works for case-insensitive temp detection

---

## 8. Regression Tests - Scenario 1

**File:** `regression-test/suites/temp_table_p0/test_temp_like.sql`

### CREATE TEMP LIKE on AUTO LIST PARTITION table

```sql
-- Base partitioned source table (AUTO LIST PARTITION)
CREATE TABLE contacts_src_like (
  id INT,
  country VARCHAR(16)
)
ENGINE=OLAP
DUPLICATE KEY(id)
AUTO PARTITION BY LIST (country)
(
)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- Create TEMP table LIKE source (with mixed-case name)
CREATE TEMPORARY TABLE Contacts_Test_GKL LIKE contacts_src_like;

-- Insert and validate counts
INSERT INTO Contacts_Test_GKL VALUES (1, 'US');
SELECT 'Scenario1_after_first_insert' AS tag, COUNT(*) AS cnt FROM Contacts_Test_GKL;
-- Expected: 1

INSERT INTO Contacts_Test_GKL VALUES (2, 'CN');
SELECT 'Scenario1_after_second_insert' AS tag, COUNT(*) AS cnt FROM Contacts_Test_GKL;
-- Expected: 2
```

**What it tests:**
- CREATE TEMP LIKE works on AUTO LIST PARTITION tables
- Mixed-case temp table name resolution
- INSERT works after LIKE creation
- Partitioned source schema is correctly copied

**Expected output:** `test_temp_like.out`
```
Scenario1_after_first_insert    1
Scenario1_after_second_insert   2
```

---

## 9. Regression Tests - Scenario 2

**File:** `regression-test/suites/temp_table_p0/test_temp_overwrite.sql`

### INSERT OVERWRITE on temporary table

```sql
-- Create temp table with CTAS
CREATE TEMPORARY TABLE test123_emp 
PROPERTIES('replication_num' = '1') 
AS SELECT 1 AS id;

SELECT 'Scenario2_after_create_as' AS tag, COUNT(*) AS cnt FROM test123_emp;
-- Expected: 1

-- Regular INSERT
INSERT INTO test123_emp VALUES (2);
SELECT 'Scenario2_after_insert' AS tag, COUNT(*) AS cnt FROM test123_emp;
-- Expected: 2

-- INSERT OVERWRITE (replaces all rows)
INSERT OVERWRITE TABLE test123_emp VALUES (3);
SELECT 'Scenario2_after_overwrite' AS tag, COUNT(*) AS cnt FROM test123_emp;
-- Expected: 1
```

**What it tests:**
- CREATE TEMP AS SELECT works
- Regular INSERT on temp table
- INSERT OVERWRITE resolves temp table correctly (was failing before)
- Count changes correctly: 1 → 2 → 1

**Expected output:** `test_temp_overwrite.out`
```
Scenario2_after_create_as       1
Scenario2_after_insert          2
Scenario2_after_overwrite       1
```

---

## Summary of All Changes

| File | Lines Changed | Type | Key Change |
|------|--------------|------|------------|
| Database.java | +37 | Core Fix | Add display→internal mapping, resolve temp names in mode=1 |
| Util.java | ±32 | Core Fix | Case-insensitive temp table utilities |
| FeNameFormat.java | +8 | Helper | Add TEMPORARY_TABLE_SIGN_LOWER constant |
| ShowTableStatusCommand.java | ±28 | Bug Fix | Apply LOWER() for consistent filtering |
| UtilTest.java | +229 | Test | Comprehensive unit tests for utilities |
| ShowTableStatusCommandTest.java | +14 | Test | Unit tests for filtering logic |
| test_temp_like.sql | +37 | E2E Test | Scenario 1: CREATE TEMP LIKE |
| test_temp_like.out | +2 | E2E Test | Expected output |
| test_temp_overwrite.sql | +21 | E2E Test | Scenario 2: INSERT OVERWRITE |
| test_temp_overwrite.out | +3 | E2E Test | Expected output |

**Total: 10 files, ~411 lines added/modified**

---

## The Complete Flow (Mode=1 Example)

1. **User creates temp table:**
   ```sql
   CREATE TEMPORARY TABLE MyTable (id INT);
   ```

2. **Database.registerTable() executes:**
   - Internal name: `"session123_#TEMP#_MyTable"` (stored in `nameToTable`)
   - Lowercased lookup: `"session123_#temp#_mytable"` → `"session123_#TEMP#_MyTable"`
   - Display lookup: `"mytable"` → `"session123_#TEMP#_MyTable"` ⬅ **NEW MAPPING**

3. **User queries with mixed case:**
   ```sql
   SELECT * FROM MyTable;
   ```

4. **Database.getTableNullable("MyTable") executes:**
   - Input lowercased: `"mytable"` (mode=1)
   - Generate temp inner: `"session123_#temp#_mytable"`
   - Resolve via map: `"session123_#temp#_mytable"` → `"session123_#TEMP#_MyTable"` ⬅ **RESOLUTION**
   - Lookup: `nameToTable.get("session123_#TEMP#_MyTable")` ✅ **FOUND**

5. **Result:** Query succeeds!

**Before the fix:** Step 4 failed because the resolution in step 4.4 didn't exist, so lookup tried `"session123_#temp#_mytable"` which wasn't in `nameToTable`.

---

This completes the detailed explanation of all code changes!
