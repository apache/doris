# Dependency-Check Codex Automation Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an external dependency-check helper under `/mnt/disk1/gq/idea/dependency-check-tools/` that runs OWASP dependency-check for one Maven directory, classifies direct vs transitive vulnerabilities, writes actionable plan/result artifacts, and manages an external suppression file for short-term exceptions.

**Architecture:** Use a thin shell entrypoint plus a small Python 3.6-compatible package. The shell entrypoint handles CLI setup and workspace paths; Python handles YAML config loading, dependency-check invocation, report parsing, dependency-tree analysis, artifact generation, and suppression-file updates. The tool does not add a Maven plugin to Doris; it drives Maven from the command line and leaves actual `pom.xml` edits to the Codex session that consumes the generated plan.

**Tech Stack:** Bash, Python 3.6, PyYAML, `xml.etree`, `json`, Maven CLI, `xmllint`, Doris `fe/` parent POM structure.

---

## Chunk 1: Workspace, File Map, and Test Harness

**Create**
- `/mnt/disk1/gq/idea/dependency-check-tools/bin/run.sh`: CLI entrypoint that resolves config path, mode, suppression override, and dispatches Python.
- `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/__init__.py`: package marker.
- `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/cli.py`: argparse entrypoint.
- `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/config.py`: config loading and validation.
- `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/models.py`: typed plan/result state containers compatible with Python 3.6.
- `/mnt/disk1/gq/idea/dependency-check-tools/tests/test_config.py`: config-path and override tests.
- `/mnt/disk1/gq/idea/dependency-check-tools/tests/test_cli.py`: mode/argument behavior tests.
- `/mnt/disk1/gq/idea/dependency-check-tools/tests/fixtures/minimal-config.yaml`: smallest valid config fixture.
- `/mnt/disk1/gq/idea/dependency-check-tools/configs/incubator-doris-fe.yaml`: real Doris FE config.
- `/mnt/disk1/gq/idea/dependency-check-tools/README.md`: usage and execution constraints.

**Modify**
- `docs/superpowers/specs/2026-03-30-dependency-check-codex-automation-design.md`: only if plan execution needs a spec correction discovered during implementation.

### Task 1: Establish the External Tool Skeleton with TDD

**Files:**
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/tests/test_config.py`
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/tests/test_cli.py`
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/tests/fixtures/minimal-config.yaml`
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/config.py`
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/cli.py`
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/models.py`
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/bin/run.sh`
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/configs/incubator-doris-fe.yaml`
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/README.md`

- [ ] **Step 1: Write a failing config-load test**

Add `test_load_config_reads_repo_root_and_scan_dir()` in `test_config.py` that loads `minimal-config.yaml` and asserts:
- `repo_root`
- `scan_dir`
- `working_dir`
- `suppression_file`
- `report_root`
- `check_cmd`
- `build_cmd`

Expected initial failure: `ModuleNotFoundError` or missing loader function.

- [ ] **Step 2: Write a failing CLI override test**

Add `test_cli_override_replaces_suppression_file()` in `test_cli.py` that invokes `parse_args()` with:

```text
--config tests/fixtures/minimal-config.yaml --mode analyze --suppression-file /tmp/override.xml
```

and asserts the parsed override path is retained.

Expected initial failure: `parse_args` missing.

- [ ] **Step 3: Run the config/CLI tests and confirm RED**

Run:

```bash
python3 -m unittest \
  /mnt/disk1/gq/idea/dependency-check-tools/tests/test_config.py \
  /mnt/disk1/gq/idea/dependency-check-tools/tests/test_cli.py
```

Expected: FAIL because loader and CLI parsing are not implemented yet.

- [ ] **Step 4: Implement the minimal config and CLI layer**

Implement:
- a `Config` object with validated absolute-path fields
- `load_config(path)` using `yaml.safe_load`
- `parse_args(argv=None)` with `--config`, `--mode`, `--suppression-file`, `--dry-run`
- `bin/run.sh` that executes `python3 -m dcfix.cli "$@"`

Keep the validation minimal:
- required keys must exist
- `mode` must be one of `analyze`, `fix`, `fix-and-suppress`
- override suppression path replaces config value in memory only

- [ ] **Step 5: Re-run the config/CLI tests and confirm GREEN**

Run the same `python3 -m unittest ...` command.

Expected: PASS

- [ ] **Step 6: Add the real Doris FE config and README**

Populate `configs/incubator-doris-fe.yaml` with the approved absolute paths and document:
- external directory location
- required `nvdApiServerId`
- single scan directory scope
- live scan network dependency

- [ ] **Step 7: Commit the skeleton**

Commit only the external tool skeleton and tests with a message such as:

```bash
git add docs/superpowers/plans/2026-03-30-dependency-check-codex-automation.md
git commit -m "[chore](build) Add dependency-check tool scaffold plan"
```

For the external directory itself, record the file inventory in `README.md` because it is outside the Doris git tree.

## Chunk 2: Scan Invocation and Report Parsing

**Create**
- `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/run_context.py`: run directory and timestamp helpers.
- `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/scan.py`: dependency-check command composition and execution.
- `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/report_parser.py`: JSON/XML report parsing.
- `/mnt/disk1/gq/idea/dependency-check-tools/tests/test_scan.py`: scan command composition tests.
- `/mnt/disk1/gq/idea/dependency-check-tools/tests/test_report_parser.py`: report parsing tests.
- `/mnt/disk1/gq/idea/dependency-check-tools/tests/fixtures/dependency-check-report.json`: minimized fixture with direct and transitive cases.

### Task 2: Build Scan Execution with Deterministic Artifact Layout

**Files:**
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/run_context.py`
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/scan.py`
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/tests/test_scan.py`

- [ ] **Step 1: Write a failing scan command test**

Add `test_build_check_command_adds_formats_and_suppression_file()` asserting the composed Maven command:
- starts with `mvn`
- contains `org.owasp:dependency-check-maven:12.2.0:check`
- appends `-DsuppressionFiles=...` when a suppression path exists
- appends output-format flags needed for JSON, XML, and HTML

Use a synthetic config object and a fixed run directory.

- [ ] **Step 2: Run the scan test and confirm RED**

Run:

```bash
python3 -m unittest /mnt/disk1/gq/idea/dependency-check-tools/tests/test_scan.py
```

Expected: FAIL because scan helpers do not exist.

- [ ] **Step 3: Implement run-directory and command composition**

Implement:
- `make_run_dir(report_root, clock=None)` returning `<report_root>/<timestamp>`
- `build_check_command(config, run_dir)` that adds:
  - suppression file when configured
  - output directory under the run dir
  - JSON/XML/HTML formats
- `run_scan(config, run_dir, exec_runner=subprocess.check_call)` that executes inside `working_dir`

Do not swallow Maven failures. Surface the full failing command and exit code.

- [ ] **Step 4: Re-run the scan test and confirm GREEN**

Expected: PASS

### Task 3: Parse dependency-check Reports into Stable Findings

**Files:**
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/report_parser.py`
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/tests/test_report_parser.py`
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/tests/fixtures/dependency-check-report.json`

- [ ] **Step 1: Write a failing parser test for vulnerable package extraction**

Add `test_parse_report_extracts_package_url_and_vuln_ids()` that loads the fixture and asserts each finding contains:
- Maven coordinates
- package URL
- vulnerability IDs
- highest CVSS
- dependency file path

Expected initial failure: parser missing.

- [ ] **Step 2: Write a failing parser test for filtering non-Maven entries**

Add `test_parse_report_skips_non_maven_packages()` using a fixture entry with a non-Maven PURL and assert it is excluded.

- [ ] **Step 3: Run parser tests and confirm RED**

Run:

```bash
python3 -m unittest /mnt/disk1/gq/idea/dependency-check-tools/tests/test_report_parser.py
```

Expected: FAIL

- [ ] **Step 4: Implement the parser minimally**

Implement JSON-first parsing that:
- reads `dependencies[*].packages[*].id`
- keeps only `pkg:maven/...`
- normalizes `groupId`, `artifactId`, `version`
- collects CVE/GHSA IDs and highest CVSS
- preserves evidence needed later for suppression generation

Keep XML parsing as a fallback helper only if JSON is missing.

- [ ] **Step 5: Re-run parser tests and confirm GREEN**

Expected: PASS

- [ ] **Step 6: Commit scan and parser support**

Commit the scan/report code and tests with a message such as:

```bash
git commit -m "[feature](build) Add dependency-check scan and report parser"
```

## Chunk 3: Dependency Tree Analysis and Action Planning

**Create**
- `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/dependency_tree.py`: Maven dependency-tree invocation and parser.
- `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/planner.py`: direct vs transitive classification and action grouping.
- `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/pom_locator.py`: locate version properties, BOM imports, and dependencyManagement anchors in `fe/pom.xml`.
- `/mnt/disk1/gq/idea/dependency-check-tools/tests/test_dependency_tree.py`
- `/mnt/disk1/gq/idea/dependency-check-tools/tests/test_planner.py`
- `/mnt/disk1/gq/idea/dependency-check-tools/tests/fixtures/dependency-tree-direct.txt`
- `/mnt/disk1/gq/idea/dependency-check-tools/tests/fixtures/dependency-tree-transitive.txt`

### Task 4: Classify Direct vs Transitive Findings with `dependency:tree`

**Files:**
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/dependency_tree.py`
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/tests/test_dependency_tree.py`
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/tests/fixtures/dependency-tree-direct.txt`
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/tests/fixtures/dependency-tree-transitive.txt`

- [ ] **Step 1: Write a failing direct-dependency classification test**

Add `test_parse_tree_marks_first_level_as_direct()` that parses a fixture where the vulnerable artifact is an immediate child of the root module.

- [ ] **Step 2: Write a failing transitive-dependency classification test**

Add `test_parse_tree_marks_nested_path_as_transitive_and_tracks_parent()` that asserts:
- classification is `transitive`
- the first non-root parent is recorded as the upgrade candidate

- [ ] **Step 3: Run dependency-tree tests and confirm RED**

Run:

```bash
python3 -m unittest /mnt/disk1/gq/idea/dependency-check-tools/tests/test_dependency_tree.py
```

Expected: FAIL

- [ ] **Step 4: Implement dependency-tree execution and parsing**

Implement:
- `build_dependency_tree_command(module_name, group_id, artifact_id)`
- `parse_dependency_tree(stdout, target_gav)`
- result shape containing:
  - module name
  - path from root to target
  - `direct` or `transitive`
  - upgrade parent candidate for transitive findings

Preserve raw path strings in the result so `summary.md` can show them.

- [ ] **Step 5: Re-run dependency-tree tests and confirm GREEN**

Expected: PASS

### Task 5: Group Findings by Upgrade Control Point

**Files:**
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/planner.py`
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/pom_locator.py`
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/tests/test_planner.py`

- [ ] **Step 1: Write a failing planner test for property grouping**

Add `test_planner_groups_findings_by_parent_property()` using a synthetic locator result such as:

```python
{
    ("com.fasterxml.jackson.core", "jackson-databind"): {
        "control_type": "property",
        "control_name": "jackson.version",
        "pom_path": "/mnt/disk1/gq/idea/incubator-doris/fe/pom.xml",
    }
}
```

and assert multiple Jackson findings collapse into one planned action.

- [ ] **Step 2: Write a failing planner test for transitive override fallback**

Add `test_planner_uses_dependency_management_override_when_no_property_exists()` and assert the planned action kind becomes `dependency_management_override`.

- [ ] **Step 3: Run planner tests and confirm RED**

Run:

```bash
python3 -m unittest /mnt/disk1/gq/idea/dependency-check-tools/tests/test_planner.py
```

Expected: FAIL

- [ ] **Step 4: Implement the plan synthesizer**

Implement:
- `locate_control_point(repo_root, scan_dir, finding)` that inspects `fe/pom.xml` for:
  - `${property}` references
  - BOM imports
  - `dependencyManagement` declarations
- `build_plan(findings, tree_results, locator)` that emits grouped actions:
  - `property_upgrade`
  - `bom_upgrade`
  - `dependency_management_override`
  - `module_local_upgrade`
  - `suppression_candidate`

Do not try to edit the POM here. The output is an actionable plan for the Codex session.

- [ ] **Step 5: Re-run planner tests and confirm GREEN**

Expected: PASS

- [ ] **Step 6: Commit tree analysis and planning**

Commit the planner layer and tests with a message such as:

```bash
git commit -m "[feature](build) Add dependency vulnerability action planner"
```

## Chunk 4: Run Outputs, Suppression Updates, and End-to-End Dry Run

**Create**
- `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/output_writer.py`: write `plan.json`, `result.json`, `summary.md`.
- `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/suppressions.py`: XML suppression read/update/write.
- `/mnt/disk1/gq/idea/dependency-check-tools/tests/test_output_writer.py`
- `/mnt/disk1/gq/idea/dependency-check-tools/tests/test_suppressions.py`
- `/mnt/disk1/gq/idea/dependency-check-tools/tests/fixtures/suppressions.xml`

### Task 6: Persist Actionable Artifacts and Manage the External Suppression File

**Files:**
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/output_writer.py`
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/suppressions.py`
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/tests/test_output_writer.py`
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/tests/test_suppressions.py`
- Create: `/mnt/disk1/gq/idea/dependency-check-tools/tests/fixtures/suppressions.xml`

- [ ] **Step 1: Write a failing output-writer test**

Add `test_write_outputs_creates_plan_result_and_summary_files()` asserting a run directory receives:
- `plan.json`
- `result.json`
- `summary.md`

and that `summary.md` contains both a fixed-action section and a suppression-candidate section.

- [ ] **Step 2: Write a failing suppression update test**

Add `test_add_suppression_writes_precise_package_and_vulnerability_matchers()` asserting a new suppression entry includes:
- package URL or exact GAV evidence
- vulnerability ID
- note text explaining incompatibility

- [ ] **Step 3: Run artifact and suppression tests and confirm RED**

Run:

```bash
python3 -m unittest \
  /mnt/disk1/gq/idea/dependency-check-tools/tests/test_output_writer.py \
  /mnt/disk1/gq/idea/dependency-check-tools/tests/test_suppressions.py
```

Expected: FAIL

- [ ] **Step 4: Implement artifact writing and suppression mutation**

Implement:
- stable JSON serialization for plan/result
- `summary.md` with sections:
  - scan metadata
  - grouped upgrade actions
  - suppression candidates
  - blocked items
- suppression XML helpers that:
  - create a file if missing
  - append exact matches
  - avoid duplicate entries for the same package/vulnerability pair

- [ ] **Step 5: Re-run the artifact/suppression tests and confirm GREEN**

Expected: PASS

### Task 7: Wire the CLI End-to-End in Dry-Run Mode

**Files:**
- Modify: `/mnt/disk1/gq/idea/dependency-check-tools/dcfix/cli.py`
- Modify: `/mnt/disk1/gq/idea/dependency-check-tools/README.md`
- Test: `/mnt/disk1/gq/idea/dependency-check-tools/tests/test_cli.py`

- [ ] **Step 1: Write a failing end-to-end CLI dry-run test**

Add `test_cli_analyze_writes_run_artifacts_without_executing_maven_when_stubbed()` that:
- stubs scan execution
- feeds fixture findings and tree results
- asserts the run directory contains `plan.json`, `result.json`, and `summary.md`

- [ ] **Step 2: Run the CLI tests and confirm RED**

Run:

```bash
python3 -m unittest /mnt/disk1/gq/idea/dependency-check-tools/tests/test_cli.py
```

Expected: FAIL because orchestration is incomplete.

- [ ] **Step 3: Implement the orchestration flow**

Wire `cli.py` so `main()` performs:
- config load
- run directory creation
- scan execution
- report parse
- dependency-tree lookups
- plan build
- artifact writes
- optional suppression update for `fix-and-suppress`

In `--dry-run`, skip actual Maven execution and operate only on fixtures or injected runners for tests.

- [ ] **Step 4: Re-run the CLI tests and confirm GREEN**

Expected: PASS

- [ ] **Step 5: Perform a live smoke run if permissions and network are available**

Run:

```bash
/mnt/disk1/gq/idea/dependency-check-tools/bin/run.sh \
  --config /mnt/disk1/gq/idea/dependency-check-tools/configs/incubator-doris-fe.yaml \
  --mode analyze
```

Expected:
- Maven starts from `/mnt/disk1/gq/idea/incubator-doris/fe`
- dependency-check produces reports under `/mnt/disk1/gq/idea/dependency-check-tools/runs/incubator-doris/fe/<timestamp>/`
- plan and summary files are written

If network or credentials are unavailable, record that explicitly in `README.md` and stop at passing unit tests.

- [ ] **Step 6: Commit the artifact/output wiring**

Commit only the new orchestration code, tests, and README updates with a message such as:

```bash
git commit -m "[feature](build) Add dependency-check automation workflow"
```

## Chunk 5: Codex Usage Contract and Repo Integration Notes

**Modify**
- `/mnt/disk1/gq/idea/dependency-check-tools/README.md`
- `docs/superpowers/specs/2026-03-30-dependency-check-codex-automation-design.md` if implementation reality forces a contract adjustment

### Task 8: Document the Human/Codex Handoff Cleanly

**Files:**
- Modify: `/mnt/disk1/gq/idea/dependency-check-tools/README.md`

- [ ] **Step 1: Add a Codex execution playbook**

Document the exact usage pattern:
- run `analyze`
- inspect `plan.json`
- let Codex apply grouped `pom.xml` upgrades in the repo
- run `mvn clean install -DskipTests` after each upgrade group
- add only truly incompatible residual findings to `suppression.xml`

- [ ] **Step 2: Add troubleshooting notes**

Document:
- network/NVD API failures
- missing `PyYAML`
- Maven dependency-tree false negatives due to wrong module choice
- why the external tool does not directly edit POM files

- [ ] **Step 3: Final verification pass**

Run the full local test suite for the tool:

```bash
python3 -m unittest discover /mnt/disk1/gq/idea/dependency-check-tools/tests
```

Expected: PASS

- [ ] **Step 4: Final commit or inventory checkpoint**

Because the executable tool lives outside the Doris git tree, capture:
- exact external file list
- exact command used to run tests
- any live-run limitations

If an implementation-side Doris repo commit is needed, use a message such as:

```bash
git commit -m "[doc](build) Document dependency-check automation workflow"
```

Plan complete and saved to `docs/superpowers/plans/2026-03-30-dependency-check-codex-automation.md`. User has already approved the design and asked to proceed, so implementation may begin immediately after plan review and workspace setup.
