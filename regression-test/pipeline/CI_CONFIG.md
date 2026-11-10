# CI configuration

Fixed settings and suite exclusions live in each profile's existing configuration
files. `DORIS_CI_PRISTINE_FRAMEWORK=true` enables the reviewed settings for CI
running the unmodified regression framework. Deployment supplies runtime addresses,
paths and credentials.

`ci-config-version` contains `1` so deployment can distinguish these source-owned
settings from older cases packages. Marked packages use the source configuration;
unmarked packages retain the legacy adapter additions, including their exclusions.
Removing the marker would select that fallback again. Unknown versions are rejected.
The marker does not replace the artifact manifest or PR commit checks.

CloudP0 runs `cloud_p0/conf/init.sql` after cluster readiness and before regression
workers to create the shared nested-types parent database. It is safe to repeat.
Older version-1 packages without this file retain their prior initialization;
unmarked master and release-branch packages retain the complete legacy behavior.
