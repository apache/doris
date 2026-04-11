---
name: be-code-style
description: Fix BE (C++) code formatting issues using clang-format
compatibility: opencode
---

## What I do

Fix C++ code formatting issues in the BE and Cloud modules using the project's clang-format configuration (v16).

## When to use me

- Before committing BE/Cloud C++ code changes
- When CI reports clang-format failures
- When you need to check or fix C++ code style

## Procedure

### Step 1: Auto-fix formatting

Run the project's formatting script, which enforces clang-format v16:

```bash
build-support/clang-format.sh
```

This formats all C++ files under `be/src`, `be/test`, `cloud/src`, `cloud/test` in-place, respecting `.clang-format` and `.clang-format-ignore`.

**Important**: Always use this script instead of invoking `clang-format` directly. The script checks that clang-format version 16 is installed and exits with an error if the wrong version is found. Using a different version will produce inconsistent formatting.

### Step 2: Check-only (no modification)

To verify formatting without modifying files:

```bash
build-support/check-format.sh
```

This outputs a diff of any formatting violations and exits non-zero if there are any.

### Step 3: Review and commit

After running `clang-format.sh`, review the changes with `git diff` to verify only formatting was changed, then stage and commit.

## Key Configuration

| File | Purpose |
|------|---------|
| `.clang-format` | Main formatting rules (Google-based, 100 col, 4-space indent) |
| `.clang-format-ignore` | Files excluded from formatting (third-party, generated) |
| `build-support/run_clang_format.py` | Python wrapper for parallel execution |

## Excluded Directories

The following are excluded from formatting (see `.clang-format-ignore`):
- `be/src/apache-orc/*`, `be/src/clucene/*`, `be/src/gutil/*`
- `be/src/glibc-compatibility/*`
- Specific third-party vendored files (mustache, sse2neon, utf8_check)
- `cloud/src/common/defer.h`

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `clang-format not found` | Install clang-format v16 or set `CLANG_FORMAT_BINARY` env var |
| `version is not 16` | Install clang-format v16; on macOS: `brew install llvm@16` |
| Files not being formatted | Check `.clang-format-ignore` for exclusions |
