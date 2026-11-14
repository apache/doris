# CI configuration

Fixed build/test settings and suite exclusions belong to the source tree that is
being tested. A CI adapter must first verify the source and artifact identities,
then consume that tree's `regression-test/pipeline/` files. Every changed setting
or temporary exclusion needs its reason beside the value. An exclusion also needs
an environment prerequisite or a condition for restoring coverage. A product
assertion, memory-limit failure, timeout, or failure observed through an obsolete
adapter is not a reason to exclude a suite.

`DORIS_CI_PRISTINE_FRAMEWORK=true` enables the reviewed regression settings for
rootless CI using the unmodified regression framework. Other regression environments
keep their existing defaults. Runtime addresses, paths, temporary credential
references and build IDs are supplied at deployment; secrets are never committed.

Concurrency belongs to the pipeline. Do not move scheduler capacity, build jobs,
Maven forks, JVM processor allocation, or regression parallelism into these files.
`CI_TEST_GROUPS` and `CI_RUN_NON_CONCURRENT` describe test selection, not worker
counts. Performance is outside this migration: its settings and scripts are unchanged.

## Files and consumers

| Profile | Source of policy | Consumer |
| --- | --- | --- |
| Compile | `compile/conf/ci.env` | Source `common/ci-policy.sh compile` before third-party and Doris compilation. |
| FEUT | `feut/conf/ci.env`, `feut/conf/excluded-tests.txt` | Source the policy in the container; append `FEUT_TEST_JVM_OPTIONS` to the pipeline's CPU-allocation options. Pass the source exclusion file to Surefire. |
| BEUT | `beut/conf/ci.env` | Source the policy before build, enumeration and execution. Use the same `BEUT_FILTER` for enumeration and execution; its value is `*`. |
| CloudUT | `cloudut/conf/ci.env` | Source before compilation/environment setup. Source `cloudut/prepare-test-runtime.sh` before tests. |
| P0 / NonConcurrent | Profile `conf/ci.env`, `conf/session_variables.sql`, FE/BE and regression configs | Use `common/source-config.py`, source the profile policy and execute the source SQL. |
| CloudP0 / VaultP0 | Profile `conf/ci.env` and existing custom configs / session SQL | Use `common/source-config.py` and source the profile policy. Keep the existing source SQL consumer. |
| External | `external/conf/ci.env`, `components.json`, `session_variables.sql`, FE/BE and regression configs | Use `common/source-config.py`, `external/fixture-components.py` and the source SQL. |

The policy loader exports the profile settings. Supply `CI_BRANCH` with the tested
**target** branch before loading FEUT, P0 or NonConcurrent. It deliberately fails
when a profile file or required branch identity is missing. For example:

```bash
source "$DORIS_ROOT/regression-test/pipeline/common/ci-policy.sh" "$PROFILE"
python3 "$DORIS_ROOT/regression-test/pipeline/common/source-config.py" \
    "$DORIS_ROOT" "$REPORT_DIR" "$PROFILE" "$PYTHON_UDF_INTERPRETER" "$CI_BUILD_ID"
```

The regression configuration adapter preserves source statement order, including
`excludeSuites += ...`. It adds only runtime bindings, records source/runtime
hashes and hashes each source policy file. It must not append another exclusion
assignment, storage provider, group selection or feature override. P0 and CloudP0
now declare the OSS provider alongside their OSS endpoint in the source config.

Execute every nonblank, noncomment statement in the profile's
`session_variables.sql` through the existing SQL client, preserving order and
propagating any error. Do not retain a second SQL heredoc in CI. These files record
why audit logging, statistics scheduling, insert visibility, the SQL converter and
the connection budget differ from product defaults.

External component selection starts from the tested branch's native
`ALL_COMPONENTS` declaration. `components.json` documents the temporarily missing
Hudi and OceanBase fixtures and their readiness requirements. It does **not**
create a suite exclusion list. The selector ignores the former private
`EXTERNAL_COMPONENT_EXCLUSIONS` variable and records a hash of the public policy.
The previously injected External assertion/memory exclusions are removed from the
adapter; the three documented rootless exclusions remain in the Groovy file.

FEUT retains one temporary mocked-FE port-collision exclusion in the public file.
BEUT restores `BlockFileCacheTest.test_factory_1`. P0, CloudP0 and NonConcurrent
restore the migration-era assertion, memory, timeout and obsolete-adapter exclusions;
the remaining rootless/fixture prerequisites have inline restoration conditions.
Existing baseline exclusions outside that migration block are not rewritten here.

CloudUT's entropy shim remains restricted to the documented old runtime image.
Its C implementation comes from the tested source's
`cloud/test/fdb_getentropy_compat.c`, not an embedded private copy. The helper sets
`CI_CLOUDUT_TEST_PRELOAD`; apply it to the test command only, without preloading
unrelated tools. Compiler optimization flags must still be unset when compiling
third-party dependencies, as in the existing build path.

## Result handling

Keep the existing case-identity, scope/completeness, exit-code, timeout, sanitizer,
coverage, cleanup and archive checks. After the report has been produced and these
checks have passed, run:

```bash
python3 "$DORIS_ROOT/regression-test/pipeline/common/check-test-results.py" \
    "$REPORT_DIR/reports/regression.xml"
```

Pass the complete Surefire/GTest XML collection for unit-test lanes. A raw
`failure` or `error` fails the step even if CI has muted that test; empty reports,
missing reports and an entirely skipped run also fail. Preserve the XML and failure
logs for review. Report/archive steps must still run after a failure. This change
does not delete mute records or retry tests. Vault's existing strict failure gate
remains in force.

## Timeouts and rollout

`CI_EXECUTION_TIMEOUT_MIN` is the reviewed whole-build timeout. At pipeline
configuration release, mirror it to TeamCity's `executionTimeoutMin` and read it
back. `CI_TEST_TIMEOUT_SECONDS` is a separate test-stage guard. Compile and BEUT
keep their longer whole-build budgets for third-party rebuilds; this migration
preserves the other already-reviewed timeout values too. The outer credential
lease and watchdog budgets must be derived from the same source test-stage value,
with their existing lifecycle margins, rather than a private test timeout override.

`ci-config-version` remains `1` for the existing source config format. The new
adapter requires that marker and the new policy files. It has **no** legacy
configuration/exclusion fallback. The marker still does not replace artifact or
PR commit verification.

The public source changes and the managed TeamCity adapter switch are two parts
of the rollout. Merging this PR alone does not deploy TeamCity configuration:

1. Review and merge/backport the public files to every branch enabled for the new
   adapter. Verify that Compile packages these files with the exact cases SHA.
2. Replace embedded configuration generators with calls to `source-config.py`;
   remove the External post-processing list and private component parameter.
   Replace FEUT's generated exclusion file, BEUT's fixed filter, build/test flags,
   SQL heredocs and provider overrides with the consumers above. Keep pipeline
   concurrency unchanged.
3. Update every duplicate embedded module, bundle manifest and checksum pin
   together. Verify the final generated bundle, including later file overlays,
   not just the compressed archive. Mirror/read back whole-build timeout values.
4. Exercise the complete source-bound lanes, artifact download, report import,
   raw failure verdict, log upload, cleanup and Compile downstream dispatch.
   Offline contract tests are not evidence of a completed CI run.

For rollback, retain the matched source revision and pipeline export together.
Do not remove the marker or silently restore hidden exclusions. Old branches that
lack the new public files need a backport before switching their adapter.

Offline checks:

```bash
python3 -m unittest discover -s regression-test/pipeline/common/tests \
    -p test_source_policy.py -v
```
