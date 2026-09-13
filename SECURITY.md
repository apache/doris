# Security Policy

Apache Doris security findings should be reported to
`security@apache.org`. The Apache Security Team will route reports to
the Doris project maintainers.

For security scope, trust boundaries, attacker roles, explicit
non-goals, and vulnerability triage classification, use
`threat-model.md` as the canonical source for this repository. Security
scanners, review agents, and vulnerability triagers should read
`threat-model.md` before classifying findings.

Findings that are out of model or by design under `threat-model.md`
should be reported with that disposition instead of being treated as
Doris vulnerabilities.

Before testing the FE HTTP surface, confirm `enable_all_http_auth` is
`true` on the running FE — it is the default, and it must not be
turned off for the test run. Confirm it against the *effective*
runtime value, not against `fe.conf` alone: FE reads `fe.conf` and
then `${custom_config_dir}/fe_custom.conf`, and the latter overwrites
the former. In releases where this flag was mutable, `ADMIN SET
FRONTEND CONFIG (...) PROPERTIES("persist" = "true")` could write
`enable_all_http_auth=false` into `fe_custom.conf`; making the flag
non-mutable does not remove or migrate such a value, so an upgraded
cluster can have no `false` entry anywhere in `fe.conf` and still be
running with authentication off. Read the value back from
`/api/show_config` (or `ADMIN SHOW FRONTEND CONFIG`) and check both
files. With it off, FE serves part of its HTTP surface (metadata,
statistics and import REST endpoints) without checking credentials,
and every finding that depends on it being off is out of model. On BE
the same flag still defaults to `false`; BE 8040 is an internal port
that operators are required to keep off end-user networks, so findings
there are disclaimed rather than valid. See the security-testing
baseline in §4.5a of `threat-model.md`.
