---
name: fe-code-style
description: Fix FE (Java) code style issues using Checkstyle
compatibility: opencode
---

## What I do

Diagnose and fix Java code style issues in the FE module using the project's Checkstyle configuration.

## When to use me

- Before committing FE Java code changes
- When FE build fails due to checkstyle violations
- When you need to understand FE style rules

## Procedure

### Step 1: Run checkstyle

Checkstyle is integrated into the Maven build and runs automatically during `mvn validate`. To check style only (without full compilation):

```bash
cd fe && mvn checkstyle:check -pl fe-core
```

Or as part of the normal build:

```bash
./build.sh --fe -j${DORIS_PARALLELISM}
```

If checkstyle fails, the build will fail with error messages showing the file, line number, and rule violated.

### Step 2: Interpret violations

Checkstyle output looks like:

```
[ERROR] src/main/java/.../Foo.java:[42:5] (imports) UnusedImports: Unused import - java.util.List.
[ERROR] src/main/java/.../Bar.java:[10] (header) RegexpHeader: Line does not match expected header line...
```

Each error shows: `[file]:[line:col] (category) RuleName: description`.

### Step 3: Fix common violations

| Violation | Fix |
|-----------|-----|
| `RegexpHeader` | Add/fix Apache License header at file top |
| `UnusedImports` | Remove unused import statements |
| `LineLength` (>120 chars) | Break long lines |
| `IllegalImport` (shaded classes) | Use the non-shaded equivalent (see `import-control.xml`) |
| `FileTabCharacter` | Replace tabs with spaces |
| `NewlineAtEndOfFile` | Ensure file ends with a newline |
| `MergeConflictMarker` | Resolve git merge conflicts |

### Step 4: Verify fix

After fixing, re-run checkstyle to confirm:

```bash
cd fe && mvn checkstyle:check -pl fe-core
```

## Key Configuration

| File | Purpose |
|------|---------|
| `fe/check/checkstyle/checkstyle.xml` | Main rules (license header, line length 120, encoding, imports) |
| `fe/check/checkstyle/suppressions.xml` | Rule exclusions (test files, nereids, large files) |
| `fe/check/checkstyle/import-control.xml` | Import restrictions (no shaded classes, no old logging APIs, no Lombok in nereids) |
| `fe/check/checkstyle/checkstyle-apache-header.txt` | Apache License 2.0 header template |
| `build-support/IntelliJ-code-format.xml` | IntelliJ formatter scheme (120 col, import organization) |

## Suppression Rules

Some files/packages have relaxed rules (see `suppressions.xml`):
- Test files: Javadoc rules suppressed
- Non-nereids code: Some strict rules suppressed
- Specific large files: Individual suppressions

## Excluded Code

The following are excluded from checkstyle (see `fe/pom.xml`):
- `**/apache/doris/thrift/**/*` (generated Thrift code)
- `**/apache/parquet/**/*` (generated Parquet code)

## Import Restrictions (key rules from import-control.xml)

- **No shaded imports**: Do not use `org.apache.doris.thirdparty.*` directly
- **No old logging**: Do not use `org.apache.commons.logging` or `java.util.logging`
- **No SimpleDateFormat**: Use `java.time` APIs instead
- **No Lombok in nereids**: `lombok` is disallowed in `org.apache.doris.nereids` package

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Build fails on `validate` phase | Run `mvn checkstyle:check -pl fe-core` to see specific violations |
| Can't find checkstyle config | Ensure you're running from the `fe/` directory |
| IntelliJ formatting differs | Import `build-support/IntelliJ-code-format.xml` via Settings → Editor → Code Style |
