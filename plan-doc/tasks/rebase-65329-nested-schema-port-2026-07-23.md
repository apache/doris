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
- [x] T2.1 fe-connector-api: neutral ConnectorColumnPath DTO (parts, isNested, topLevel/leaf/parent, getFullPath).
      = connector/api/ddl/ConnectorColumnPath.java (JDK-only, mirrors analysis.ColumnPath).
- [x] T2.2 ConnectorTableOps: 5 path-addressed default-throw ops (addColumn(path,col,pos)/dropColumn(path)/
      renameColumn(path,new)/modifyColumn(path,col,pos)/modifyColumnComment(path,comment)).
- [x] T2.3 PluginDrivenExternalCatalog: 5 ColumnPath overrides now route non-nested->flat, nested->new SPI path
      ops; modifyColumnComment (flat+nested)->SPI path op; toConnectorPath helper.
- [x] T2.4 DONE: new IcebergNestedColumnEvolution.java (resolveColumnPath struct/list.element/map.value +
      reject map.key; validateNestedStructFieldPath; applyPosition; applyRenameColumn identifier-field fixup;
      validateCollectionPseudoFieldComment); IcebergConnectorMetadata 5 nested SPI overrides (build type outside
      auth, single executeAuthenticated commit); IcebergCatalogOps 5 nested seam methods; ConnectorColumn
      +nullableSpecified/commentSpecified (excluded from equals/hashCode) + ConnectorColumnConverter threads them.
      Deviations (accepted): row-lineage guard omitted (matches flat ops); upgradeNestedModifyError generic msg.
- [x] T2.5 DONE: IcebergNestedColumnEvolutionTest.java = 25 tests (resolveColumnPath 6 / nested ADD 5 / DROP+RENAME
      4 incl. identifier fixup / MODIFY 7 / COMMENT 3). Engine needed ZERO fixes.
- [x] T2.6 DONE: full FE build6 install SUCCESS (compile+testCompile+checkstyle all 70 modules). Tests:
      IcebergNestedColumnEvolution 25 + CatalogBackedIcebergCatalogOpsColumnEvolution 25 + IcebergConnectorMetadata
      ColumnEvolution 20 = 70/0/0; HudiReadOnlyWriteReject 4/0/0. fe-core AlterTableCommand+DdlRouting pending.
      NOTE: new path SPI ops RENAMED to add/drop/rename/modifyNestedColumn (distinct names) to avoid overload
      ambiguity with flat String/ConnectorColumn ops at Mockito.any()/null call sites (HudiReadOnlyWriteRejectTest,
      PluginDrivenExternalCatalogDdlRoutingTest). ConnectorTableOps+IcebergConnectorMetadata+PluginDrivenExternalCatalog updated.
      Offline test-run gotcha: iceberg tests need NO -am (IcebergCatalogFactoryTest needs hive.conf from installed shade jar).

Tier 1 committed: 67addbbb5cb. Tier 2 commit pending.

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
