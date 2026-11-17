# CI configuration

Fixed build/test settings and suite exclusions belong to the tested source tree's
`regression-test/pipeline/` directory. Every changed setting or temporary exclusion
includes its reason beside the value. Exclusions also identify the prerequisite
or condition for restoring coverage. Diagnose product assertions, memory-limit
failures and timeouts before changing the case selection.

Pipeline scripts load these public files after verifying the tested source and
artifact identities. Configuration loading, environment preparation, component
selection and result processing are maintained with the pipeline scripts.

`DORIS_CI_PRISTINE_FRAMEWORK=true` selects the reviewed regression settings for
rootless CI using the unmodified regression framework. Runtime addresses, paths,
temporary credential references and build IDs are supplied at deployment.
Secrets stay in CI credential storage.

Concurrency belongs to the pipeline, including build jobs, Maven forks, JVM
processor allocation and regression parallelism. `CI_TEST_GROUPS` and
`CI_RUN_NON_CONCURRENT` describe test selection. Performance retains its existing
configuration.

## Configuration files

| Profile | Public configuration |
| --- | --- |
| Compile | `compile/conf/ci.env` |
| FEUT | `feut/conf/ci.env`, `feut/conf/excluded-tests.txt` |
| BEUT | `beut/conf/ci.env` |
| CloudUT | `cloudut/conf/ci.env` |
| P0 / NonConcurrent | Profile `conf/ci.env`, `conf/session_variables.sql`, FE/BE and regression configs |
| CloudP0 / VaultP0 | Profile `conf/ci.env` and existing custom configs / session SQL |
| External | `external/conf/ci.env`, `conf/components.json`, `conf/session_variables.sql`, FE/BE and regression configs |

The pipeline supplies `CI_BRANCH` as the tested target branch when loading FEUT,
P0 and NonConcurrent settings. FEUT JVM options are appended to the pipeline's
CPU-allocation options. BEUT uses the same public `BEUT_FILTER` for enumeration
and execution.

Regression configuration statements retain their source order, including
`excludeSuites += ...`. Runtime bindings and configuration hashes are recorded
by the pipeline. Changes to suite selection, storage provider, test groups and
feature settings belong in the public configuration files.

The pipeline executes nonblank, noncomment statements from
`session_variables.sql` in order and propagates execution errors. The comments
explain the audit logging, statistics scheduling, insert visibility, SQL converter
and connection-budget settings.

External component selection starts from the tested branch's native
`ALL_COMPONENTS` declaration. `external/conf/components.json` records the Hudi and
OceanBase fixture omissions, their reasons and readiness requirements. Suite
exclusions are declared separately in the public regression config. The pipeline
reads the component policy and records its hash with the selected components.

FEUT's temporary mocked-FE port-collision exclusion is documented in its exclusion
file. BEUT restores `BlockFileCacheTest.test_factory_1`. P0, CloudP0 and
NonConcurrent restore the audited migration-era assertion, memory, timeout and
obsolete-adapter exclusions. Remaining rootless/fixture prerequisites include
restoration conditions. Existing baseline exclusions outside the audited blocks
retain their current values.

CloudUT's compatibility-image condition and sanitizer/JVM settings are declared
in `cloudut/conf/ci.env`. Pipeline scripts prepare the source-owned
`cloud/test/fdb_getentropy_compat.c` shim for that image and apply its preload to
the test command.

## Result handling

The pipeline retains the existing report import, completeness, exit-code, timeout,
sanitizer, coverage and cleanup handling. TeamCity applies its configured mute
semantics to muted test cases. Result processing and log upload remain maintained
with the pipeline scripts.

## Timeouts and rollout

`CI_EXECUTION_TIMEOUT_MIN` declares the whole-build timeout. At pipeline rollout,
mirror it to TeamCity's `executionTimeoutMin` and read it back.
`CI_TEST_TIMEOUT_SECONDS` declares the test-stage budget. Compile and BEUT retain
their longer whole-build budgets for third-party rebuilds. Credential leases and
watchdogs derive their budgets from the test-stage value with the existing
lifecycle margins.

`ci-config-version` remains `1`. The pipeline verifies that marker, required
configuration files and source/artifact identities before consuming the settings.

Rollout requires both the reviewed source configuration and its corresponding
managed pipeline scripts.

1. Merge/backport the configuration to each enabled branch and verify that Compile
   packages it with the exact tested cases revision.
2. Update the managed scripts to read these files. Remove private suite filters,
   component omissions, feature flags, provider overrides and SQL settings that
   have moved into the public configuration. Preserve pipeline concurrency and
   TeamCity mute handling.
3. Update duplicated scripts, bundle manifests and checksum pins together. Verify
   the generated modules and later file overlays, and read back whole-build
   timeout values.
4. Validate complete source-bound lanes, artifact download, report import with
   muted and unmuted cases, log upload, cleanup and Compile downstream dispatch.

Retain a matched source revision and pipeline configuration export for rollback.
Enabled branches need the public configuration before switching their managed
scripts. Local configuration checks cover only the preparation for rollout.
