---
name: clang-tidy-check
description: Run clang-tidy on newly added/modified BE C++ code
compatibility: opencode
---

## What I do

Run clang-tidy static analysis on newly added or modified C++ files in the BE and Cloud modules, using the project's `.clang-tidy` configuration. The script parses `git diff` to identify changed line ranges and filters clang-tidy output to diagnostics on those lines where possible, reducing noise from pre-existing code. Diagnostics from included headers that were not part of the diff are filtered out, though some edge cases may still appear.

## When to use me

- After building BE, before committing C++ code changes
- When CI reports clang-tidy warnings on your PR
- When you want to proactively check new code for common bugs and style issues

## Prerequisites

1. **The relevant module must be built first** — clang-tidy needs `compile_commands.json` generated during the CMake build. For BE files, build BE; for Cloud files, build Cloud.
2. **clang-tidy must be installed** — the project uses clang-tidy from the LDB toolchain.

## Procedure

### Step 1: Build the relevant module (if not already done)

For **BE** files:
```bash
./build.sh --be -j${DORIS_PARALLELISM}
```
This generates `compile_commands.json` in `be/build_Release/` or `be/build_ASAN/`.

For **Cloud** files:
```bash
./build.sh --cloud -j${DORIS_PARALLELISM}
```
This generates `compile_commands.json` in `cloud/build_Release/` or `cloud/build_ASAN/`.

**Note**: A single compilation database typically covers only one module (BE or Cloud). If your changes span both modules, you may need to run clang-tidy twice with different `--build-dir` values.

### Step 2: Run clang-tidy on changed files

```bash
build-support/run-clang-tidy.sh
```

By default, this script:
- Detects changed C++ files via `git diff` (uncommitted changes + staged changes)
- Automatically finds `compile_commands.json` from the latest BE build
- Runs clang-tidy using the `.clang-tidy` config
- Skips files in excluded directories (third-party, generated code)
- Reports warnings grouped by file

**Options:**

```bash
# Compare against a specific branch (e.g., for PR review)
build-support/run-clang-tidy.sh --base origin/master

# Check specific files (all lines, no line-level filtering)
build-support/run-clang-tidy.sh --files be/src/vec/functions/my_new_func.cpp

# Report ALL warnings in changed files (no line filtering)
build-support/run-clang-tidy.sh --full

# Specify build directory explicitly (required for Cloud if BE build dir was auto-detected)
build-support/run-clang-tidy.sh --build-dir be/build_ASAN

# Check Cloud files with Cloud compilation database
build-support/run-clang-tidy.sh --build-dir cloud/build_ASAN

# Apply auto-fixes where possible (note: fixes may apply beyond changed lines)
build-support/run-clang-tidy.sh --fix
```

**Important**: By default, only warnings on changed lines are reported (diagnostics from headers not in the diff are filtered out). This prevents the agent from "fixing" unrelated pre-existing warnings in large files. Use `--full` to see all warnings if needed.

### Step 3: Interpret and fix warnings

clang-tidy output looks like:

```
be/src/vec/functions/foo.cpp:42:5: warning: use auto when initializing with a cast [modernize-use-auto]
be/src/vec/functions/foo.cpp:55:1: warning: function 'process' exceeds recommended size [readability-function-size]
```

**Fix approach:**
1. Fix all warnings that represent real issues (bugs, performance, readability)
2. For false positives or intentional patterns, add `// NOLINT(check-name)` with a brief justification
3. Report any warnings you cannot reasonably fix

### Step 4: Verify

Re-run the script to confirm all warnings are resolved:

```bash
build-support/run-clang-tidy.sh
```

## Enabled Checks (from `.clang-tidy`)

| Category | Examples |
|----------|----------|
| `clang-diagnostic-*` | Compiler diagnostics |
| `clang-analyzer-*` | Static analysis (null deref, use-after-free, etc.) |
| `bugprone-*` | Use-after-move, redundant branch condition, unused RAII |
| `modernize-*` | Auto, range-for, nullptr (excludes trailing return, nodiscard) |
| `readability-*` | Function size (≤80 lines), cognitive complexity (≤50) |
| `performance-*` | String find, inefficient algorithm, move-const-arg |
| `misc-redundant-expression` | Redundant expressions |

## Excluded Directories

Files in these paths are automatically skipped:
- `be/src/apache-orc/`, `be/src/clucene/`, `be/src/gutil/`
- `be/src/glibc-compatibility/`
- `contrib/` (all third-party code)
- Generated code directories

## NOLINT Usage

When a clang-tidy warning cannot be fixed, suppress it with a comment:

```cpp
// Good: specific check name + reason
int x = (int)y; // NOLINT(modernize-use-auto): explicit cast for clarity

// Bad: blanket suppression without reason
int x = (int)y; // NOLINT
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `compile_commands.json not found` | Build BE first: `./build.sh --be -j${DORIS_PARALLELISM}` |
| `clang-tidy not found` | Install via LDB toolchain or `apt install clang-tidy-16` |
| Too many warnings on old code | Use `--base` to diff against a branch, ensuring only your changes are checked |
| Warning in header included by your file | Only fix if the header is also in your changeset; otherwise note it in your report |
