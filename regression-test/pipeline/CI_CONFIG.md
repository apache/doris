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
