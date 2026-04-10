# AGENTS.md — Apache Doris

This is the codebase for Apache Doris, an MPP OLAP database. It primarily consists of the Backend module BE (be/, execution and storage engine), the Frontend module FE (fe/, optimizer and transaction core), and the Cloud module (cloud/, storage-compute separation). Your basic development workflow is: modify code, build using standard procedures, add and run tests, and submit relevant changes.

## When running in a WORKTREE directory

To ensure smooth test execution without interference between worktrees, the first thing to do upon entering a worktree directory is to check if `.worktree_initialized` exists. If not, execute `hooks/setup_worktree.sh`, setting `$ROOT_WORKSPACE_PATH` to the base directory (typically `${DORIS_REPO}`) beforehand. After successful execution, verify that `.worktree_initialized` has been touched and that `thirdparty/installed` dependencies exist correctly. Also check if submodules have been properly initialized; if not, do so manually.

When working in worktree mode, all operations must be confined to the current worktree directory. Do not enter `${DORIS_REPO}` or use any resources there. Compilation and execution must be done within the current worktree directory. The compiled Doris cluster must use random ports not used by other worktrees (modify BE and FE conf before compilation, using a uniform offset of `${DORIS_PORT_OFFSET_RANGE}` from default ports without conflicting with other worktrees' ports). Run from the `output` directory within the worktree. To run regression tests, modify `regression-test/conf/regression-conf.groovy` and set the port numbers in jdbcUrl and other configuration items to your new ports so the corresponding worktree cluster can be used for regression testing.

## Coding Standards

Assert correctness only—never use defensive programming with `if` or similar constructs. Any `if` check for errors must have a clearly known inevitable failure path (not speculation). If no such scenario is found, strictly avoid using `if(valid)` checks. However, you may use the `DORIS_CHECK` macro for precondition assertions (If inside performance-sensitive areas like loops, it can only be `DCHECK`). For example, if logically A=true should always imply B=true, then strictly avoid `if(A&&B)` and instead use `if(A){DORIS_CHECK(B);...}`. In short, the principle is: upon discovering errors or unexpected situations, report errors or crash—never allow the process to continue.

When adding code, strictly follow existing similar code in similar contexts, including interface usage, error handling, etc., maintaining consistency. When adding any code, first try to reference existing functionality. Second, you must examine the relevant context paragraphs to fully understand the logic.

After adding code, you must first conduct self-review and refactoring attempts to ensure good abstraction and reuse as much as possible.

### Code Style Enforcement

All code must pass style checks before committing. Use the corresponding skill for detailed step-by-step procedures.

**BE (C++) Formatting**: Run `build-support/clang-format.sh` to auto-fix formatting. This script enforces clang-format v16; do not use other versions. Run `build-support/check-format.sh` to check without modifying files. See the `be-code-style` skill for details.

**BE (C++) Static Analysis**: After building BE (which generates `compile_commands.json`), run `build-support/run-clang-tidy.sh` to check modified C++ files against the `.clang-tidy` config. The script parses `git diff` to filter warnings to changed lines where possible, reducing noise from pre-existing code (diagnostics from included headers may still appear). For Cloud C++ files, pass `--build-dir` pointing to the Cloud compilation database (e.g., `cloud/build_ASAN`). Try to fix all reported warnings; if a warning cannot be reasonably fixed, add a `// NOLINT` comment with justification and report it. See the `clang-tidy-check` skill for details.

**FE (Java) Style**: Checkstyle is integrated into the Maven build (`maven-checkstyle-plugin`). Running `build.sh --fe` automatically validates style via `mvn validate`. If checkstyle fails, fix the reported issues according to `fe/check/checkstyle/checkstyle.xml`. See the `fe-code-style` skill for details.

## Code Review

When conducting code review (including self-review and review tasks), it is necessary to complete the key checkpoints according to our `code-review` skill and provide conclusions for each key checkpoint (if applicable) as part of the final written description. Other content does not require individual responses; just check them during the review process.

## Build and Run Standards

Always use only the `build.sh` script with its correct parameters to build Doris BE and FE. When building, use `-j${DORIS_PARALLELISM}` parallelism. For example, the simplest BE+FE build command is `./build.sh --be --fe -j${DORIS_PARALLELISM}`.
Build type can be set via `BUILD_TYPE` in `custom_env.sh`, but only set it to `RELEASE` when explicitly required for performance testing; otherwise, keep it as `ASAN`.
You may modify BE and FE ports and network settings in `conf/` before compilation to ensure correctness and avoid conflicts.
Build artifacts are in the current directory's `output/`. If starting the service, ensure all process artifacts have their conf set with appropriate non-conflicting ports and `priority_networks = 10.16.10.3/24`. Use `--daemon` when starting. Cluster startup is slow; wait at least 30s for success. If still not ready after waiting, continue waiting. If not ready after a long time, check BE and FE logs to investigate.
For first-time cluster startup, you may need to manually add the backend.

## Testing Standards

All kernel features must have corresponding tests. Prioritize adding regression tests under `regression-test/`, while also having BE unit tests (`be/test/`) and FE unit tests (`fe/fe-core/src/test/`) where possible. Interface usage in test cases must first reference similar cases.

You must use the preset scripts in the codebase with their correct parameters to run tests (`run-regression-test.sh`, `run-be-ut.sh`, `run-fe-ut.sh`). Regression test result files must not be handwritten; they must be auto-generated via test scripts. When running regression tests, if using `-s` to specify a case, also try to use `-d` to specify the parent directory for faster execution. For example, for cases under `nereids_p0`, you can use `-d nereids_p0 -s xxx`, where `xxx` is the name from `suite("xxx")` in the groovy file.

BE-UT compilation must use at most `${DORIS_PARALLELISM}` parallelism also.

Key utility functions in BE code, as well as the core logic (functions) of complete features, must have corresponding unit tests. If it's inconvenient to add unit tests, the module design and function decomposition should be reviewed again to ensure high cohesion and low coupling are properly achieved.

Added regression tests must comply with the following standards:
1. Use `order_qt` prefix or manually add `order by` to ensure ordered results
2. For cases expected to error, use the `test{sql,exception}` pattern
3. After completing tests, do not drop tables; instead drop tables before using them in tests, to preserve the environment for debugging
4. For ordinary single test tables, do not use `def tableName` form; instead hardcode your table name in all SQL
5. Except for variables you explicitly need to adjust for testing current functionality, other variables do not need extra setup before testing. For example, nereids optimizer and pipeline engine settings can use default states

## Commit Standards

Files in git commit should only be related to the current modification task. Environment modifications for running (e.g., `conf/`, `AGENTS.md`, `hooks/`, etc.) must not be `git add`ed. When delivering the final task, you must ensure all actual code modifications have been committed.

Commit messages must follow the format below, which mirrors the PR template (`.github/PULL_REQUEST_TEMPLATE.md`):

```
[<type>](<module>) <Short summary of the change>

### What problem does this PR solve?

Issue Number: close #xxx

Related PR: #xxx

Problem Summary: <Describe the problem this commit addresses>

### Release note

<If applicable, describe user-visible changes; otherwise write "None">

### Check List (For Author)

- Test: <Specify which testing was done>
    - Regression test / Unit Test / Manual test / No need to test (with reason)
- Behavior changed: No / Yes (with explanation)
- Does this need documentation: No / Yes (with doc PR link)
```

Key rules for commit messages:
1. The title must follow the `[type](module)` format validated by the PR title checker (`.github/workflows/title-checker.yml`). Common types include: `fix`, `feature`, `improvement`, `refactor`, `chore`, `test`, `doc`. Common modules include: `fe`, `be`, `cloud`, `regression`, `build`
2. The short summary must be concise and written in imperative mood (e.g., `[fix](fe) Fix null pointer in scan node` not `[fix](fe) Fixed null pointer`)
3. The `Issue Number` field must reference the corresponding GitHub Issue with `close #xxx` syntax when applicable
4. The `Release note` section must be filled in for any user-visible behavior or feature change; write "None" for internal refactoring or test-only changes
5. The test section must honestly reflect the testing performed; do not claim tests that were not actually run
