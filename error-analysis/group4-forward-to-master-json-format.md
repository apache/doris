# Group 4 — ForwardToMasterTest: ClassCastException Due to JSON Format Mismatch (1 failure)

## Affected Tests (1 failure)
- `ForwardToMasterTest.testAddBeDropBe`

## Symptom
```
java.lang.ClassCastException: class org.json.simple.JSONArray cannot be cast to class org.json.simple.JSONObject
    at ForwardToMasterTest.java:53
```

## Root Cause

The test code at line 53 does:
```java
JSONArray columnNames = (JSONArray) ((JSONObject) data.get("columnNames")).get("columnNames");
```

This assumes the response has the nested format:
```json
{
  "columnNames": { "columnNames": [...] },
  "rows":        { "rows": [...] }
}
```

But the current `NodeAction.NodeInfo` implementation returns a flat format:
```json
{
  "columnNames": [...],
  "rows": [...]
}
```

So `data.get("columnNames")` returns a `JSONArray` directly, not a `JSONObject` wrapper — causing the `ClassCastException`.

## Note on Scope
This failure is **pre-existing and unrelated to the SPI refactoring** in this PR. Neither `NodeAction.java` nor `ForwardToMasterTest.java` appears in the git diff for this branch. However, it is a failing test in the CI build, so it must be fixed.

## Fix
Update `ForwardToMasterTest.java` to use the current flat response format:

```java
// Before:
JSONArray columnNames = (JSONArray) ((JSONObject) data.get("columnNames")).get("columnNames");
JSONArray rows = (JSONArray) ((JSONObject) data.get("rows")).get("rows");

// After:
JSONArray columnNames = (JSONArray) data.get("columnNames");
JSONArray rows = (JSONArray) data.get("rows");
```

The same pattern likely appears multiple times in the test file (for different `info` calls like `dropInfo`). All occurrences must be updated.
