# Native Azure Iceberg acceptance test

The suite `test_iceberg_azure_native_credentials` uses a real, pre-provisioned Azure
Iceberg REST catalog. It is opt-in and never contains credentials.

## Fixture requirements

Set these values in the regression test configuration (or in the equivalent CI secret-backed
configuration):

```groovy
enableIcebergAzureNativeTest = "true"
icebergAzureNativeCatalog = "azure_test_catalog"
icebergAzureNativeDatabase = "azure_test_db"
icebergAzureNativeFormatVersions = "2,3"
icebergAzureNativeFileFormats = "parquet,orc"
```

The catalog must already exist, point at a writable Iceberg REST service, and grant the test
user permission to create, read, delete, update, merge, and overwrite tables. Configure the
catalog's SharedKey, SAS, or OAuth2 material outside the suite; do not put tokens in the Groovy
file or in a checked-in result file.

Run the suite with the normal regression runner, for example:

```bash
./run-regression-test.sh --run -d external_table_p2/iceberg -s test_iceberg_azure_native_credentials
```

If the opt-in flag is absent, the suite is not registered. A real run with an invalid catalog,
permission, or capability fails instead of being silently skipped. Use the dry-run mode to
validate only the fixture names and format selections.

The current checkout does not include a complete expected-output baseline for this external
suite. When provisioning the fixture, generate it with the preset runner, not by hand:

```bash
./run-regression-test.sh --run -d external_table_p2/iceberg \
  -s test_iceberg_azure_native_credentials -genOut
```

Review the generated `.out` for all selected version/format combinations, then rerun without
`-genOut` to compare results. Generating a baseline is not itself a passing comparison run.
Do not reuse a partial result from a failed run or claim that a dry-run generated a baseline.

## Rolling upgrade rule

Upgrade the BE before enabling native SAS or OAuth2 data access from a new FE. The new FE emits
the provider-owned `AZURE_*` fields; SharedKey also carries an explicitly matching legacy wire
group so an old BE can continue a SharedKey request during the rollout. The new BE rejects
conflicting mixed credential groups. SAS and OAuth2 have no safe legacy wire representation and
must not be used until all BEs understand `AZURE_*`.

For rollback, stop native SAS/OAuth2 use first, then roll back the FE/BE pair. Do not roll back
only the BE while a new FE is still sending native SAS/OAuth2 fields.
