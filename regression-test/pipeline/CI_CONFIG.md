# CI configuration ownership

The five rootless regression profiles (`p0`, `cloud_p0`, `vault_p0`,
`external`, and `nonConcurrent`) keep fixed settings and suite exclusions in
this directory. FE/BE settings belong in each profile's existing `.conf` files;
MetaService settings belong in `ms_custom.conf`; regression settings belong in
`regression-conf.groovy` or `regression-conf-custom.groovy`.

`DORIS_CI_PRISTINE_FRAMEWORK=true` opts into the regression defaults and
compatibility exclusions for CI running the unmodified framework. The comments
beside exclusions describe the recorded failure and the condition for restoring
the suite. An unresolved failure is not proof of a framework defect. Do not
patch the framework or assertions while deploying a cases package.

Deployment still supplies addresses, writable directories, Python interpreter
path and actual version, Trino plugin paths, build IDs, and temporary credential
environment references. Those values describe the particular runtime and must
not be serialized into shared cases packages as credentials or build identities.

## Cases capability version

`ci-config-version` containing `1` means all five profiles own their fixed
rootless CI settings and compatibility exclusion lists. This is a capability
marker, not a replacement for the Compile artifact manifest or commit binding.
Consumers must validate that binding before reading configuration.

A compatible deployment consumer must:

1. Keep the unchanged legacy configuration adapter when the marker is absent.
   This includes older master cases and branch-4.1/branch-4.2 packages. Never
   select the new behavior using the target branch name alone.
2. Accept version `1` only with all required configuration files for the selected
   profile present and the rootless CI opt-in enabled. Reject unknown versions or
   incomplete packages before deploying the cluster.
3. Copy the source configuration and inject only runtime inputs. Do not append
   another fixed-setting block or exclusion list. When combining cloud binary
   defaults with profile custom settings, the profile settings must take
   precedence, including `enable_logging_conflict_keys`.
4. Record the cases identity, configuration version, source hash, and effective
   configuration hash with the build evidence.

Deploy the backward-compatible consumer **before consuming new marked cases**.
Only a package carrying version `1` can stop using the old script additions;
merging this source change alone does not remove any live pipeline fallback.
Rolling back to an older cases package automatically selects the legacy adapter.
This change does not backport configuration to release branches.
