# Preconfigured Performance CI runtime

The default `DORIS_CI_ENV_MODE=managed` retains host provisioning. Operators of
unprivileged containers can explicitly set `DORIS_CI_ENV_MODE=preconfigured`.
This mode validates prerequisites and fails without installing packages, writing
sysctls or disabling host swap. It calls the repository's original FE/BE launchers.

Set `JAVA_HOME` to an existing absolute JDK directory and `DORIS_CI_JAVA_MAJOR`
to its expected Java specification version (for current master, `17`). The Java
binary must execute successfully and report that version. Supply the mysql client
and a readable local JaCoCo agent, using `DORIS_CI_JACOCO_AGENT` if it is outside
`/usr/local/jacoco/lib/jacocoagent.jar`. FE JaCoCo stays enabled with the existing
exclusions, append mode and destination. Native compiler coverage settings do not
change the FE runtime contract.

Before starting BE, the host must expose `vm.max_map_count >= 2000000`, the
container soft nofile limit must be at least 200000, its core limit must be
unlimited, and `/proc/swaps` must be readable with no active swap entries. The
launcher does not silently relax any of these conditions.

Performance comments use `DORIS_CI_REPORT_BACKEND=github` by default and require
`GITHUB_TOKEN`. Explicitly select `artifact` and an existing absolute
`DORIS_CI_REPORT_DIR` to save each report body as a separate file. Configure CI
artifact publication for that directory. Missing GitHub credentials never select
artifact mode automatically; unknown backend names fail.

Untrusted test containers should explicitly select `DORIS_CI_LOG_BACKEND=artifact`
and an existing absolute `DORIS_CI_LOG_DIR`. The existing log-upload call then
collects each file into a unique artifact directory, without any storage credential.
A separate trusted publisher can redact and upload archived logs to the authorized
account. Artifact collection is not a claim of remote publication. Never deliver a
trusted publisher role or GitHub token to PR-controlled code.

For log upload from a trusted process in preconfigured mode, select
`DORIS_CI_LOG_BACKEND=oss` (the default) and provision a validated ossutil 2.4.1 binary
at the absolute `DORIS_CI_OSSUTIL` path. Configure `OSS_REGION`, an HTTPS
`OSS_ENDPOINT` and a build-specific `DORIS_CI_LOG_OSS_PREFIX` such as
`oss://example-bucket/build-123/logs`. Supply short-lived `OSS_ACCESS_KEY_ID`,
`OSS_ACCESS_KEY_SECRET` and `OSS_SESSION_TOKEN` through the uploader's environment.
The helper uses no credential file and passes no secret values in CLI arguments.
It fails on upload or subsequent stat failure. Object stat is an existence check,
not an independent full-object integrity proof or a backup guarantee.

Use a separate least-privilege identity for report upload; business-test STS and
GitHub comment credentials are distinct. Operators must provision and authorize
the destination, redact logs before upload, and configure retention. This change
does not deploy credentials, grant storage permissions or change benchmark data,
row counts, query thresholds or expected results.

Run local shell behavior checks with:

```sh
bash regression-test/pipeline/common/tests/test-preconfigured-runtime.sh
```

These checks use mocked JDK, kernel interfaces, launchers and upload tools. They
cover failure propagation and configuration routing; they do not claim a live
Doris startup, OSS upload or benchmark result.
