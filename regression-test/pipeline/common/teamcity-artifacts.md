# Regression artifact binding

Comments submit only the PR number, requested head SHA and target branch.
Compile continues to trigger P0, CloudP0, VaultP0, External and NonConcurrent
with its exact producer binding. Direct regression requests never trigger Compile.

Configure a native `Doris_Compile` artifact dependency in each consumer:

- Get artifacts from: **Latest finished build** (`revisionName=lastFinished`).
- Build branch filter: `+:pull/%env.PR_NUMBER%` (`revisionBranch`).
- Artifact rules: `regression/** => <destination>`; clean the destination first.
- Destinations: `packages` for P0/NonConcurrent, `compile-artifacts` for
  CloudP0/VaultP0, and `compile-pair` for External.

Do not add a static snapshot dependency: a direct request must not start Compile.
Automatic Compile fanout already supplies its exact snapshot dependency. TeamCity
uses that chain when present; otherwise it selects the last finished Compile for
the requested PR. Missing artifacts fail native dependency resolution.

Install `teamcity_artifacts.py` as trusted pipeline code, independent of the PR
checkout. Run it in a separate first shell step, before the existing package checks:

```bash
set -euo pipefail
set +x
python3 /trusted/ci/teamcity_artifacts.py \
  --server 'https://<teamcity-host>' \
  --manifest '<destination>/manifest.json'
```

This step checks the downloaded manifest against the actual requested PR/revision
and the native dependency's terminal status. A failed Compile, different commit,
or inconsistent automatic fanout binding fails immediately; it does not search
older builds or rebuild anything. It then fills the parameter aliases expected by
the existing runners. Those runners retain their full manifest/checksum checks.

The single metadata read gets the current build's actual revision and resolved
artifact dependency, using TeamCity's temporary build credentials. Artifact
selection and download remain entirely native. Use a trusted HTTPS endpoint;
`teamcity.serverUrl` can be an internal HTTP address. When embedding the Python
source, escape its literal percent characters as `%%`.

Gate always-run cleanup on `teamcity.build.step.status.<binding-step-id> == success`.
A failed binding step cannot create cluster resources; the later preparation has
not initialized its cleanup tools yet. After binding succeeds, cleanup must still
run on workload failures and cancellation.
