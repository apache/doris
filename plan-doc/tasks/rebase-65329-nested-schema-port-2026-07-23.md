# Rebase onto upstream 60ba6aef642 + port #65329 (iceberg nested column schema change) to connector SPI

Date: 2026-07-23. Branch: branch-catalog-spi (rebased onto upstream/master 60ba6aef642, 48 commits).

## Rebase status: DONE
3 conflict commits resolved (all verified, fe-core main compiles except AlterTableCommand — see below):
- P3b kerberos #64655: IcebergMetadataOpsValidationTest import (kept both ExternalTable + kerberos.ExecutionAuthenticator).
- hive #65473 (eb9ade27810): ColumnDefinition kept both methods; git rm IcebergMetadataOps.java + its test.
- ExternalMetadataOps #65736 (f1d3096ffa2): ExternalCatalog took throw side + fixed 2 dangling #65329 ColumnPath methods; git rm ExternalMetadataOps.java.

## The #65329 integration problem (user chose Tier 1 + Tier 2 full)
Upstream #65329 (70a82532325) landed nested iceberg column schema change in the GENERIC fe-core layer,
coupled to legacy IcebergExternalTable / IcebergMetadataOps / ExternalMetadataOps which this branch removed.
Rebase applied its generic changes cleanly but they (a) break compile, (b) regress flat iceberg DDL.

### Tier 1 (compile + no-regression) — foundation — DONE (commit pending)
- [x] T1.1 ConnectorCapability.SUPPORTS_NESTED_COLUMN_SCHEMA_CHANGE + IcebergConnector declares it +
      PluginDrivenExternalTable.supportsNestedColumnSchemaChange() (via hasScanCapability, covers iceberg-on-HMS).
- [x] T1.2 AlterTableCommand.java: replace instanceof IcebergExternalTable (lines 40/145/383) with SPI check.
- [x] T1.3 PluginDrivenExternalCatalog: override 5 ColumnPath overloads (addColumn/dropColumn/renameColumn/
      modifyColumn/modifyColumnComment): non-nested -> existing flat override -> connector; nested -> throw (Tier 2).
- [x] T1.4 AlterTableCommandTest: mock(IcebergExternalTable) -> mock(PluginDrivenMvccExternalTable) + stub cap.
- [x] T1.5 verify: full FE build SUCCESS; AlterTableCommandTest 17/IcebergNestedSchemaEvolutionParserTest 18/
      ColumnDefinitionTest 2 = 37/0/0 GREEN.

### Tier 2 (full nested port)
- [ ] T2.1 fe-connector-api: neutral ConnectorColumnPath DTO (parts, isNested, topLevel/leaf/parent).
- [ ] T2.2 ConnectorTableOps: path-addressed ops (nested add-under-parent / drop / rename / modify / modifyComment).
- [ ] T2.3 PluginDrivenExternalCatalog: thread analysis.ColumnPath -> ConnectorColumnPath -> new SPI ops.
- [ ] T2.4 IcebergConnectorMetadata + IcebergCatalogOps: replicate #65329 IcebergMetadataOps ~550-line spec
      (resolveColumnPath struct/array.element/map.value; reject map.key; identifier-field fixup on rename;
      primitive promotion; nullable-nested; position moveFirst/moveAfter; row-lineage guards; no partial commit).
- [ ] T2.5 migrate +1310-line IcebergMetadataOpsValidationTest coverage to connector-side unit tests.
- [ ] T2.6 verify build + connector unit tests.

### Tests
Keep-as-is: IcebergNestedSchemaEvolutionParserTest, PruneNestedColumnTest, ColumnDefinitionTest, SchemaChangeHandlerTest.
E2E (user runs): iceberg_schema_change_ddl, test_iceberg_nested_schema_evolution_ddl,
test_iceberg_nested_schema_evolution_spark_doris_interop.

### Key evidence (file:line)
- Regression: Alter.java:415-434 calls ColumnPath overloads; PluginDrivenExternalCatalog:805-894 overrides only flat;
  ExternalCatalog:1481-1539 ColumnPath methods throw.
- SPI idiom: LogicalFileScan.supportPruneNestedColumn() / PluginDrivenExternalTable:263 supportsNestedColumnPrune()
  / hasScanCapability():290; iceberg-on-HMS reflection: HiveConnectorMetadata.reflectSiblingScanCapabilities():566.
- Connector current: ConnectorTableOps:248-285 flat only; IcebergComplexTypeDiff whole-type diff (nested add/widen only,
  no nested drop/rename); IcebergCatalogOps.modifyColumn:429; IcebergConnectorMetadata.modifyColumn:1166.
