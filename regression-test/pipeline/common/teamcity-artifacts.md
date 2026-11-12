# Regression artifact preflight

The comment workflow submits only the PR number, requested head SHA and target
branch. Compile still owns automatic fanout to the five regression consumers.

Install `teamcity_artifacts.py` as trusted TeamCity runner code, independent of the
PR checkout. Run it at the beginning of the first shell step of P0, CloudP0,
VaultP0, External and NonConcurrent, before their existing source/package guards:

```bash
set -euo pipefail
set +x
umask 077
binding=$(mktemp)
trap 'rm -f "$binding"' EXIT
python3 /trusted/ci/teamcity_artifacts.py \
  --server '%teamcity.serverUrl%' --environment "$binding"
source "$binding"
rm -f "$binding"
trap - EXIT
# Existing environment preparation follows in the same shell.
```

Remove each consumer's static `Doris_Compile` artifact dependency using
`%env.CI_PREPARED_BUILD_NUMBER%`: it resolves before any step runs and prevents
unbound direct requests from reaching this preflight. Do not add a static
snapshot dependency or a fallback Compile trigger. Existing automatic fanout may
continue to specify its exact snapshot producer and all manifest pins.

The preflight uses TeamCity's build-scoped credentials from
`TEAMCITY_BUILD_PROPERTIES_FILE`; no personal token is needed. It reads the actual
consumer revision, finds an existing successful Compile, validates its manifest,
and downloads the binary, cases and tools with size/SHA256 verification. Missing,
expired, mismatched or incomplete artifacts fail this TeamCity step. An existing
fanout binding must match exactly and is never replaced by a newer Compile.

The downloaded directories match the existing runner paths: `packages` for P0
and NonConcurrent, `compile-artifacts` for CloudP0 and VaultP0, and `compile-pair`
for External. Only after every download verifies does the helper export bindings
for the current shell and emit `setParameter` messages for subsequent steps.
The existing PR-head checks, rootless cluster lifecycle and regression steps
continue afterward.

When embedding the Python source in an inline TeamCity script, escape literal
percent characters as `%%`, retaining the `%teamcity.serverUrl%` reference in the
shell invocation. Keep the source readable in the step settings.
