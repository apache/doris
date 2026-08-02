// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.spi.ConnectorBrokerAddress;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.ConnectorStorageContext;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorTransaction;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.write.ConnectorSinkPlan;
import org.apache.doris.connector.spi.write.ConnectorWritePartitionField;
import org.apache.doris.connector.spi.write.ConnectorWritePartitionSpec;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;
import org.apache.doris.connector.spi.write.ConnectorWriteSortColumn;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergDeleteSink;
import org.apache.doris.thrift.TIcebergMergeSink;
import org.apache.doris.thrift.TIcebergRewritableDeleteFileSet;
import org.apache.doris.thrift.TIcebergTableSink;
import org.apache.doris.thrift.TIcebergWriteType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TSortField;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.LocationUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Write plan provider for iceberg INSERT / INSERT OVERWRITE.
 *
 * <p>Builds the opaque {@link TIcebergTableSink} for a bound write and binds the write to the current
 * {@link IcebergConnectorTransaction}: it opens the SDK transaction (via
 * {@link IcebergConnectorTransaction#beginWrite}, which loads the table and applies the begin-guards),
 * then assembles the sink Thrift from the loaded table. The Thrift is byte-identical to the legacy
 * fe-core {@code planner.IcebergTableSink.bindDataSink} (C2, zero BE change), so the BE writer is
 * unaffected by the migration.</p>
 *
 * <p><b>Scope.</b> INSERT / OVERWRITE ({@code TIcebergTableSink}, T06), DELETE ({@code TIcebergDeleteSink})
 * and UPDATE / MERGE ({@code TIcebergMergeSink}, T07a). REWRITE (procedures, P6.4) is not built here. The
 * write distribution and the vended-credentials overlay of the hadoop config are registered deviations
 * (DV-T0x-vended / -broker / -materialize) closed at the P6.6 cutover. At format-version&ge;3 the DELETE /
 * MERGE sink's {@code rewritable_delete_file_sets} is filled here from the scan-time supply the
 * per-statement {@link IcebergStatementScope} carried across the scan&rarr;write seam (commit-bridge S4 part 2),
 * replacing the legacy fe-resident rewritable-delete planner.</p>
 *
 * <p><b>Live since the iceberg SPI cutover.</b> An {@code iceberg} catalog is served by this connector
 * plugin, so a plugin-driven iceberg write routes through this provider; {@link #planWrite} requires the executor-bound
 * connector transaction and fails loud if absent.</p>
 */
public class IcebergWritePlanProvider implements ConnectorWritePlanProvider {

    // Legacy IcebergUtils compression-codec property keys (connector-local copies; iceberg SDK has no
    // constant for the doris/spark-sql forms).
    private static final String COMPRESSION_CODEC = "compression-codec";
    private static final String SPARK_SQL_COMPRESSION_CODEC = "spark.sql.iceberg.compression-codec";

    // Connector-local literal copy of fe-core's Column.ICEBERG_ROWID_COL (connectors must not import
    // fe-core). The connector is the sole source of this row-id column identity; the fe-core
    // ConnectorColumnConverterTest contract pin asserts that converting this exact declared shape yields the
    // Doris hidden row-id column fe-core's getFullSchema appends (name / STRUCT / invisible / not-null), so a
    // drift on either side turns one of the two tests red.
    private static final String DORIS_ICEBERG_ROWID_COL = "__DORIS_ICEBERG_ROWID_COL__";

    // The single request-scoped synthetic write column iceberg declares: the row-id STRUCT carrying the
    // per-row write metadata (file_path / row_position / partition_spec_id / partition_data). Same for
    // every iceberg table regardless of format/partitioning, so it is a shared immutable instance.
    private static final List<ConnectorColumn> SYNTHETIC_WRITE_COLUMNS =
            Collections.singletonList(buildRowIdColumn());

    private static ConnectorColumn buildRowIdColumn() {
        ConnectorType rowIdStruct = ConnectorType.structOf(
                Arrays.asList("file_path", "row_position", "partition_spec_id", "partition_data"),
                Arrays.asList(ConnectorType.of("STRING"), ConnectorType.of("BIGINT"),
                        ConnectorType.of("INT"), ConnectorType.of("STRING")));
        return new ConnectorColumn(DORIS_ICEBERG_ROWID_COL, rowIdStruct,
                "Iceberg row position metadata", false, null, false).invisible();
    }

    private final Map<String, String> properties;
    // Per-request catalog-ops resolver: applied with the current ConnectorSession to obtain the IcebergCatalogOps
    // for that request. For a iceberg.rest.session=user catalog the connector passes this::newCatalogBackedOps so
    // the write-side read helpers (explain sort clause / write sort columns / write partitioning) resolve the
    // table through the querying user's per-request delegated REST catalog (fail-closed). The actual INSERT/DELETE/
    // MERGE commit is already per-user: it loads through IcebergConnectorTransaction, opened by
    // IcebergConnectorMetadata.beginTransaction over the session-aware metadata ops. Offline-test ctors resolve the
    // single shared ops regardless of session (constant s -> catalogOps).
    private final Function<ConnectorSession, IcebergCatalogOps> catalogOpsResolver;
    private final ConnectorContext context;

    public IcebergWritePlanProvider(Map<String, String> properties, IcebergCatalogOps catalogOps,
            ConnectorContext context) {
        // Constant resolver: these ctors (offline tests) bind a single ops that ignores the session, so existing
        // behaviour/tests are byte-identical.
        this(properties, session -> catalogOps, context);
    }

    /**
     * Session-aware ctor used by {@link IcebergConnector#getWritePlanProvider()}: {@code catalogOpsResolver} is
     * applied per request with the current {@link ConnectorSession} so a {@code iceberg.rest.session=user} catalog
     * resolves the querying user's per-request delegated catalog for the write-side read helpers (the connector
     * passes {@code this::newCatalogBackedOps}); every other catalog resolves the single shared ops.
     */
    public IcebergWritePlanProvider(Map<String, String> properties,
            Function<ConnectorSession, IcebergCatalogOps> catalogOpsResolver,
            ConnectorContext context) {
        this.properties = properties;
        this.catalogOpsResolver = catalogOpsResolver;
        this.context = context;
    }

    @Override
    public ConnectorSinkPlan planWrite(ConnectorSession session, ConnectorWriteHandle handle) {
        IcebergTableHandle tableHandle = (IcebergTableHandle) handle.getTableHandle();
        IcebergConnectorTransaction transaction = currentTransaction(session);

        // Open the SDK transaction (loads the table inside the auth context and applies the begin-guards).
        // The op-context is derived from the bound write handle: the generic handle carries only an
        // isOverwrite() boolean, so an overwriting INSERT is promoted to the OVERWRITE operation the
        // transaction switches on at commit time (Append vs ReplacePartitions / OverwriteFiles).
        IcebergWriteSchemaContext schemaContext = handle.getWriteOperation() == WriteOperation.REWRITE
                ? null : resolveWriteSchema(session, tableHandle, handle.getBranchName());
        IcebergWriteContext writeContext = buildWriteContext(handle, schemaContext);
        transaction.beginWrite(session, tableHandle.getDbName(), tableHandle.getTableName(), writeContext);
        Table table = transaction.getTable();
        validateBoundWriteColumns(table, handle, writeContext.getWriteOperation());

        // commit-bridge supply (S4 part 2): read the non-equality delete supply the scan seam accumulated into the
        // per-statement scope. DELETE/MERGE attach it to the sink so the BE OR-merges old deletes into the new
        // deletion vector (a missing supply silently resurrects deleted rows); INSERT/OVERWRITE/REWRITE ignore it
        // (buildRewritableDeleteFileSets no-ops an empty map, same as the former null).
        // Fail loud (never silently resurrect): a format-version>=3 row-level DML under an absent statement scope
        // (ConnectorStatementScope.NONE — offline / no live statement) cannot have received the scan's supply (each
        // NONE lookup is a throwaway map), so reject it rather than write a deletion vector that drops old deletes.
        WriteOperation writeOp = writeContext.getWriteOperation();
        if ((writeOp == WriteOperation.DELETE || writeOp == WriteOperation.UPDATE || writeOp == WriteOperation.MERGE)
                && session.getStatementScope() == ConnectorStatementScope.NONE
                && IcebergWriterHelper.getFormatVersion(table) >= 3) {
            throw new DorisConnectorException("Iceberg row-level " + writeOp + " on a format-version>=3 table requires "
                    + "a per-statement scope to carry the rewritable-delete supply from scan to write; none is present "
                    + "(ConnectorStatementScope.NONE). Refusing to write a deletion vector that would drop old deletes "
                    + "and resurrect previously-deleted rows.");
        }
        Map<String, List<TIcebergDeleteFileDesc>> rewritableDeletes =
                IcebergStatementScope.rewritableDeleteSupply(session);

        // Dispatch on the write operation to the matching BE sink dialect (each is a distinct TDataSinkType,
        // byte-identical to the legacy fe-core planner sink). OVERWRITE shares TIcebergTableSink with INSERT
        // (the overwrite flag is read from the handle); UPDATE shares TIcebergMergeSink with MERGE.
        switch (writeContext.getWriteOperation()) {
            case INSERT:
            case OVERWRITE: {
                TDataSink dataSink = new TDataSink(TDataSinkType.ICEBERG_TABLE_SINK);
                dataSink.setIcebergTableSink(buildSink(table, tableHandle, handle, schemaContext));
                return new ConnectorSinkPlan(dataSink);
            }
            case DELETE: {
                TDataSink dataSink = new TDataSink(TDataSinkType.ICEBERG_DELETE_SINK);
                dataSink.setIcebergDeleteSink(
                        buildDeleteSink(table, tableHandle, rewritableDeletes, schemaContext));
                return new ConnectorSinkPlan(dataSink);
            }
            case UPDATE:
            case MERGE: {
                TDataSink dataSink = new TDataSink(TDataSinkType.ICEBERG_MERGE_SINK);
                dataSink.setIcebergMergeSink(buildMergeSink(table, tableHandle, rewritableDeletes,
                        handle.isRequireMergeCardinalityCheck(), schemaContext));
                return new ConnectorSinkPlan(dataSink);
            }
            case REWRITE: {
                // Compaction rewrite (ALTER TABLE ... EXECUTE rewrite_data_files): same TIcebergTableSink
                // dialect as INSERT but tagged REWRITE so the BE routes to RewriteFiles semantics. The
                // rewritableDeletes supply does not apply to rewrite (it deals with no position deletes), and
                // is already evicted above.
                TDataSink dataSink = new TDataSink(TDataSinkType.ICEBERG_TABLE_SINK);
                dataSink.setIcebergTableSink(buildRewriteSink(table, tableHandle, handle));
                return new ConnectorSinkPlan(dataSink);
            }
            default:
                throw new DorisConnectorException(
                        "Unsupported iceberg write operation: " + writeContext.getWriteOperation());
        }
    }

    private void validateBoundWriteColumns(Table table, ConnectorWriteHandle handle,
            WriteOperation writeOperation) {
        // V3 row-lineage columns are engine-generated metadata, not fields in table.schema(). Excluding
        // their neutral marker keeps the comparison on the complete user schema for INSERT and MERGE.
        List<ConnectorColumn> boundColumns = handle.getBoundTargetColumns().stream()
                .filter(column -> !column.isReservedPassthrough())
                .collect(Collectors.toList());
        if (writeOperation == WriteOperation.DELETE || boundColumns.isEmpty()) {
            return;
        }
        List<NestedField> currentColumns = table.schema().columns();
        boolean hasSyntheticRowId = boundColumns.size() == currentColumns.size() + 1
                && DORIS_ICEBERG_ROWID_COL.equals(boundColumns.get(boundColumns.size() - 1).getName());
        if (boundColumns.size() != currentColumns.size() && !hasSyntheticRowId) {
            throw new DorisConnectorException("Iceberg table schema changed after the write was bound; retry the "
                    + "statement with the latest schema");
        }
        boolean enableVarbinary = Boolean.parseBoolean(properties.getOrDefault(
                IcebergConnectorProperties.ENABLE_MAPPING_VARBINARY, "false"));
        boolean enableTimestampTz = Boolean.parseBoolean(properties.getOrDefault(
                IcebergConnectorProperties.ENABLE_MAPPING_TIMESTAMP_TZ, "false"));
        for (int i = 0; i < currentColumns.size(); ++i) {
            NestedField current = currentColumns.get(i);
            ConnectorColumn bound = boundColumns.get(i);
            ConnectorType currentType = IcebergTypeMapping.fromIcebergType(
                    current.type(), enableVarbinary, enableTimestampTz);
            // Do not compare top-level nullability: Doris widens Iceberg required columns in its read schema
            // so evolution default-fill may yield NULL. Nested requiredness remains authoritative in
            // sameBoundType, while current schema JSON enforces writes at the root.
            if (!current.name().equalsIgnoreCase(bound.getName())
                    || !sameBoundType(currentType, bound.getType())
                    || (bound.getUniqueId() >= 0 && current.fieldId() != bound.getUniqueId())) {
                // BE maps write expressions to schema-json by ordinal, so accepting a reordered live
                // schema here could silently place values under the wrong Iceberg field names.
                throw new DorisConnectorException("Iceberg table schema changed after the write was bound; retry "
                        + "the statement with the latest schema");
            }
        }
    }

    private static boolean sameBoundType(ConnectorType current, ConnectorType bound) {
        String currentName = canonicalTypeName(current.getTypeName());
        String boundName = canonicalTypeName(bound.getTypeName());
        if (!currentName.equals(boundName)
                || current.getChildren().size() != bound.getChildren().size()
                || current.getFieldNames().size() != bound.getFieldNames().size()) {
            return false;
        }
        if (hasMeaningfulTypeParameters(currentName)
                && (current.getPrecision() != bound.getPrecision() || current.getScale() != bound.getScale())) {
            return false;
        }
        for (int i = 0; i < current.getChildren().size(); i++) {
            if (!current.getFieldNames().isEmpty()
                    && !current.getFieldNames().get(i).equalsIgnoreCase(bound.getFieldNames().get(i))) {
                return false;
            }
            int boundFieldId = bound.getChildFieldId(i);
            if ((boundFieldId >= 0 && current.getChildFieldId(i) != boundFieldId)
                    || current.isChildNullable(i) != bound.isChildNullable(i)
                    || !sameBoundType(current.getChildren().get(i), bound.getChildren().get(i))) {
                // Nested field identity and requiredness are part of the write contract even though the SPI
                // type's general equals() deliberately excludes them for non-write consumers.
                return false;
            }
        }
        return true;
    }

    private static String canonicalTypeName(String typeName) {
        String normalized = typeName.toUpperCase(Locale.ROOT);
        // Doris chooses a physical DECIMAL width after conversion, while Iceberg exposes one logical decimal.
        // Width aliases with the same precision/scale are one schema, not concurrent evolution.
        return normalized.startsWith("DECIMAL") && !"DECIMALV2".equals(normalized)
                ? "DECIMALV3" : normalized;
    }

    private static boolean hasMeaningfulTypeParameters(String typeName) {
        return typeName.startsWith("DECIMAL")
                || "CHAR".equals(typeName)
                || "VARCHAR".equals(typeName)
                || "VARBINARY".equals(typeName)
                || "DATETIMEV2".equals(typeName)
                || "TIMESTAMPTZ".equals(typeName);
    }

    @Override
    public Optional<List<ConnectorColumn>> getWriteColumns(ConnectorSession session,
            ConnectorTableHandle tableHandle, Optional<String> branchName) {
        return Optional.of(resolveWriteSchema(
                session, (IcebergTableHandle) tableHandle, branchName).getColumns());
    }

    @Override
    public void appendExplainInfo(StringBuilder output, String prefix,
            ConnectorSession session, ConnectorWriteHandle handle) {
        // Surface the connector-specific write detail the generic plugin-driven sink line cannot (mirrors
        // the legacy IcebergTableSink.getExplainString "ICEBERG TABLE SINK / Table: <name>" block).
        IcebergTableHandle tableHandle = (IcebergTableHandle) handle.getTableHandle();
        output.append(prefix).append("  ICEBERG TABLE: ")
                .append(tableHandle.getDbName()).append(".").append(tableHandle.getTableName()).append("\n");
        // Legacy IcebergTableSink also rendered the table's write sort order (getSortOrderSql) when sorted;
        // reuse the SHOW CREATE TABLE renderer so EXPLAIN INSERT surfaces the same "ORDER BY (...)" the BE
        // write applies (getWriteSortColumns), keeping EXPLAIN and SHOW CREATE TABLE consistent.
        String sortClause = IcebergConnectorMetadata.buildShowSortClause(resolveTable(session, tableHandle));
        if (!sortClause.isEmpty()) {
            output.append(prefix).append("  ").append(sortClause).append("\n");
        }
    }

    /**
     * Declares the table's write-side sort columns (a {@code WRITE ORDERED BY} sort order) so the engine
     * can build the {@code TSortInfo} from the bound sink output. Ports legacy
     * {@code IcebergTableSink.bindDataSink}'s sort-order loop: only identity sort fields contribute, each
     * mapped from its iceberg field id to the column's position in the table schema (1:1 with the sink
     * output). An unsorted table yields an empty list (no sort).
     */
    @Override
    public List<ConnectorWriteSortColumn> getWriteSortColumns(ConnectorSession session,
            ConnectorTableHandle tableHandle) {
        return getWriteSortColumns(session, tableHandle, Collections.emptyList());
    }

    @Override
    public List<ConnectorWriteSortColumn> getWriteSortColumns(ConnectorSession session,
            ConnectorTableHandle tableHandle, List<ConnectorColumn> boundTargetColumns) {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        Optional<IcebergWriteSchemaContext> active =
                IcebergStatementScope.activeWriteSchema(
                        session, handle.getDbName(), handle.getTableName());
        Table table = active.isPresent() ? null : resolveTable(session, handle);
        SortOrder sortOrder = active.isPresent()
                ? active.get().getSortOrder() : table.sortOrder();
        if (!sortOrder.isSorted()) {
            // null == "no write sort order" (legacy gates setSortInfo on isSorted()). A sorted table
            // returns a (possibly empty) list so the engine still emits a TSortInfo, matching legacy's
            // unconditional setSortInfo inside the isSorted() branch even when no identity column resolves.
            return null;
        }
        Map<Integer, Integer> positionsByFieldId = new HashMap<>();
        if (boundTargetColumns.isEmpty()) {
            List<NestedField> currentColumns = active.isPresent()
                    ? active.get().getSchema().columns() : table.schema().columns();
            for (int i = 0; i < currentColumns.size(); i++) {
                positionsByFieldId.put(currentColumns.get(i).fieldId(), i);
            }
        } else {
            for (int i = 0; i < boundTargetColumns.size(); i++) {
                positionsByFieldId.put(boundTargetColumns.get(i).getUniqueId(), i);
            }
        }
        List<ConnectorWriteSortColumn> result = new ArrayList<>();
        for (SortField sortField : sortOrder.fields()) {
            if (!sortField.transform().isIdentity()) {
                continue;
            }
            Integer position = positionsByFieldId.get(sortField.sourceId());
            if (position != null) {
                // Resolve against the bound field id, never a newly refreshed live ordinal; otherwise
                // schema reorder can sort one output expression using another column's ordering contract.
                result.add(new ConnectorWriteSortColumn(position,
                        sortField.direction() == SortDirection.ASC,
                        sortField.nullOrder() == NullOrder.NULLS_FIRST));
            }
        }
        return result;
    }

    @Override
    public ConnectorWritePartitionSpec getWritePartitioning(ConnectorSession session,
            ConnectorTableHandle tableHandle) {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        Optional<IcebergWriteSchemaContext> active =
                IcebergStatementScope.activeWriteSchema(
                        session, handle.getDbName(), handle.getTableName());
        Table table = active.isPresent() ? null : resolveTable(session, handle);
        PartitionSpec spec = active.isPresent()
                ? active.get().getPartitionSpec() : table.spec();
        if (spec == null || !spec.isPartitioned()) {
            // null == "unpartitioned" (legacy PhysicalExternalRowLevelMergeSink.buildInsertPartitionFields gates on
            // spec().isPartitioned()) -> the engine uses its non-partitioned merge distribution.
            return null;
        }
        Schema schema = active.isPresent()
                ? active.get().getSchema() : table.schema();
        List<ConnectorWritePartitionField> fields = new ArrayList<>();
        for (PartitionField field : spec.fields()) {
            // sourceColumnName mirrors the legacy schema.findField(field.sourceId()).name() lookup the engine
            // used to map a partition field back to a bound output expr id. transform/param mirror
            // field.transform().toString() + parseTransformParam (kept connector-side so fe-core never parses).
            NestedField sourceField = schema.findField(field.sourceId());
            String sourceColumnName = sourceField == null ? null : sourceField.name();
            String transform = field.transform().toString();
            fields.add(new ConnectorWritePartitionField(
                    transform, parseTransformParam(transform), sourceColumnName, field.name(), field.sourceId()));
        }
        return new ConnectorWritePartitionSpec(spec.specId(), fields);
    }

    @Override
    public List<ConnectorColumn> getSyntheticWriteColumns(ConnectorSession session,
            ConnectorTableHandle tableHandle) {
        // The iceberg row-id hidden column is the same for every iceberg table regardless of
        // format/partitioning. It is the sole source of the row-id column identity: fe-core's getFullSchema
        // appends the converted column whenever a DML (or show-hidden) is in flight, and gates the actual
        // injection request-side (show-hidden / synthetic-write-column ctx flag); here we only declare it, so
        // neither the session nor the table handle is consulted.
        return SYNTHETIC_WRITE_COLUMNS;
    }

    @Override
    public Set<WriteOperation> supportedOperations() {
        return EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE,
                WriteOperation.DELETE, WriteOperation.MERGE, WriteOperation.REWRITE);
    }

    @Override
    public boolean supportsWriteBranch() {
        return true;
    }

    @Override
    public boolean requiresParallelWrite() {
        return true;
    }

    @Override
    public boolean requiresFullSchemaWriteOrder() {
        return true;
    }

    @Override
    public boolean requiresMaterializeStaticPartitionValues() {
        return true;
    }

    /** Parses the bracket argument of an iceberg transform string ({@code bucket[16] -> 16}); null when absent. */
    private static Integer parseTransformParam(String transform) {
        int start = transform.indexOf('[');
        int end = transform.indexOf(']');
        if (start < 0 || end <= start) {
            return null;
        }
        try {
            return Integer.parseInt(transform.substring(start + 1, end));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private IcebergWriteContext buildWriteContext(
            ConnectorWriteHandle handle, IcebergWriteSchemaContext schemaContext) {
        WriteOperation op = handle.getWriteOperation();
        if (op == WriteOperation.INSERT && handle.isOverwrite()) {
            op = WriteOperation.OVERWRITE;
        }
        // [SHOULD-2] / Fix B: the statement's MVCC read-snapshot pin (S_read) is threaded onto the write
        // table handle by the engine (PluginDrivenScanNode-style applyMvccSnapshotPin on the write path).
        // Carry it on the op-context so beginWrite anchors the RowDelta baseSnapshotId at S_read, keeping
        // the commit-time removeDeletes (option D) and BE's scan-time DV union on one snapshot. -1 (no pin)
        // preserves the legacy begin-time current snapshot.
        IcebergTableHandle readHandle = handle.getTableHandle() instanceof IcebergTableHandle
                ? (IcebergTableHandle) handle.getTableHandle() : null;
        long readSnapshotId = readHandle != null ? readHandle.getSnapshotId() : -1L;
        boolean readSnapshotResolved = readHandle != null && readHandle.isSnapshotResolved();
        // Branch-targeted INSERT (INSERT INTO tbl@branch): the branch is threaded from the generic insert
        // command context onto the write handle; beginWrite validates it against the table refs and points
        // the commit at the branch. Empty for a default-ref write.
        return new IcebergWriteContext(op, handle.isOverwrite(), handle.getStaticPartitionSpec(),
                handle.getBranchName(), readSnapshotId, readSnapshotResolved, schemaContext);
    }

    private TIcebergTableSink buildSink(Table table, IcebergTableHandle tableHandle,
            ConnectorWriteHandle handle, IcebergWriteSchemaContext schemaContext) {
        TIcebergTableSink tSink = new TIcebergTableSink();
        tSink.setDbName(tableHandle.getDbName());
        tSink.setTbName(tableHandle.getTableName());

        // Schema (no v3 row-lineage append — that is REWRITE/procedures, P6.4).
        tSink.setSchemaJson(schemaContext.getSchemaJson());
        // #65782: gate BE-side column-stats collection on the table's iceberg metrics policy; a fully-disabled
        // table (all columns metrics=none) skips BE collection entirely. Same schema the sink advertises.
        tSink.setCollectColumnStats(
                IcebergWriterHelper.shouldCollectColumnStats(schemaContext, schemaContext.getSchema()));

        // Partition spec (only for a partitioned table, mirroring legacy spec().isPartitioned()).
        PartitionSpec partitionSpec = schemaContext.getPartitionSpec();
        if (partitionSpec.isPartitioned()) {
            tSink.setPartitionSpecsJson(Collections.singletonMap(
                    partitionSpec.specId(), schemaContext.getPartitionSpecJson()));
            tSink.setPartitionSpecId(partitionSpec.specId());
        }

        // Sort info: the engine builds the TSortInfo from the connector-declared write-sort columns
        // (getWriteSortColumns) and threads it back on the handle; the connector stamps it verbatim.
        if (handle.getSortInfo() != null) {
            tSink.setSortInfo(handle.getSortInfo());
        }

        // File format / compression.
        tSink.setFileFormat(toTFileFormatType(schemaContext.getFileFormat()));
        tSink.setCompressionType(toTFileCompressType(schemaContext.getFileCompression()));

        // Hadoop config: BE-canonical static catalog creds (AWS_*/dfs) plus the REST per-table vended overlay
        // (see buildHadoopConfig), mirroring legacy IcebergTableSink + the scan-side credential assembly.
        tSink.setHadoopConfig(buildHadoopConfig(table));

        // Output location: normalized for the BE writer, raw kept as the original; the BE file type comes
        // from the engine (broker-aware). All vended-aware so a REST catalog's path still resolves.
        LocationFields location = resolveLocationFields(table, schemaContext.getDataLocation());
        tSink.setOutputPath(location.outputPath);
        tSink.setFileType(location.fileType);
        if (!location.brokerAddresses.isEmpty()) {
            tSink.setBrokerAddresses(location.brokerAddresses);
        }
        tSink.setOriginalOutputPath(location.rawLocation);

        // Overwrite + static partition values (INSERT OVERWRITE ... PARTITION).
        tSink.setOverwrite(handle.isOverwrite());
        Map<String, String> staticPartitionSpec = handle.getStaticPartitionSpec();
        if (handle.isOverwrite() && staticPartitionSpec != null && !staticPartitionSpec.isEmpty()) {
            tSink.setStaticPartitionValues(staticPartitionSpec);
        }
        return tSink;
    }

    /**
     * Builds the {@code TIcebergTableSink} for a compaction REWRITE. Byte-identical to legacy
     * {@code planner.IcebergTableSink.bindDataSink} under {@code isRewriting}: the INSERT baseline
     * ({@link #buildSink}) plus exactly two deltas — {@code write_type = REWRITE} (the marker the BE uses to
     * route to RewriteFiles semantics) and, at format-version&ge;3, the row-lineage fields appended to the
     * schema-json (the BE rewrite writer expects {@code _row_id} / {@code _last_updated_sequence_number}).
     * All other fields are inherited unchanged from the INSERT path.
     */
    private TIcebergTableSink buildRewriteSink(Table table, IcebergTableHandle tableHandle,
            ConnectorWriteHandle handle) {
        // A compaction REWRITE atomically replaces a file set; it is never a user INSERT OVERWRITE, and the
        // BE writer does not accept the REWRITE write-type together with the overwrite flag.
        if (handle.isOverwrite()) {
            throw new DorisConnectorException("REWRITE writes cannot be overwrite operations");
        }
        TIcebergTableSink tSink = buildSink(table, tableHandle, handle,
                IcebergWriteSchemaContext.create(table, tableHandle.getTableName(), Optional.empty(),
                        mappingVarbinaryEnabled(), mappingTimestampTzEnabled()));
        tSink.setWriteType(TIcebergWriteType.REWRITE);
        if (IcebergWriterHelper.getFormatVersion(table) >= 3) {
            // iceberg v3 format requires the row-lineage fields when rewriting data files.
            Schema rewriteSchema = IcebergWriterHelper.appendRowLineageFieldsForV3(table.schema());
            tSink.setSchemaJson(SchemaParser.toJson(rewriteSchema));
            // #65782: the collect flag must reflect the same (v3-appended) schema the sink advertises.
            tSink.setCollectColumnStats(IcebergWriterHelper.shouldCollectColumnStats(table, rewriteSchema));
        }
        return tSink;
    }

    /**
     * Builds the {@code TIcebergDeleteSink} (port of legacy {@code planner.IcebergDeleteSink.bindDataSink}).
     * Iceberg delete is always a position delete. ⚠️ The delete sink carries {@code compress_type} (thrift
     * field 6), NOT the table/merge sink's {@code compression_type}. The format-version&ge;3
     * {@code rewritable_delete_file_sets} is attached from the scan-time supply (commit-bridge S4 part 2),
     * mirroring legacy {@code IcebergDeleteExecutor.finalizeSinkForDelete} +
     * {@code IcebergDeleteSink.toThrift}'s {@code formatVersion>=3 && !empty} gate.
     */
    private TIcebergDeleteSink buildDeleteSink(Table table, IcebergTableHandle tableHandle,
            Map<String, List<TIcebergDeleteFileDesc>> rewritableDeletes,
            IcebergWriteSchemaContext schemaContext) {
        TIcebergDeleteSink tSink = new TIcebergDeleteSink();
        tSink.setDbName(tableHandle.getDbName());
        tSink.setTbName(tableHandle.getTableName());
        tSink.setDeleteType(TFileContent.POSITION_DELETES);
        tSink.setFileFormat(toTFileFormatType(schemaContext.getFileFormat()));
        tSink.setCompressType(toTFileCompressType(schemaContext.getFileCompression()));
        tSink.setHadoopConfig(buildHadoopConfig(table));

        LocationFields location = resolveLocationFields(table, schemaContext.getDataLocation());
        tSink.setOutputPath(location.outputPath);
        tSink.setTableLocation(location.rawLocation);
        tSink.setFileType(location.fileType);
        if (!location.brokerAddresses.isEmpty()) {
            tSink.setBrokerAddresses(location.brokerAddresses);
        }

        PartitionSpec partitionSpec = schemaContext.getPartitionSpec();
        if (partitionSpec.isPartitioned()) {
            tSink.setPartitionSpecId(partitionSpec.specId());
        }
        int formatVersion = schemaContext.getFormatVersion();
        tSink.setFormatVersion(formatVersion);
        List<TIcebergRewritableDeleteFileSet> sets =
                buildRewritableDeleteFileSets(formatVersion, rewritableDeletes);
        if (!sets.isEmpty()) {
            tSink.setRewritableDeleteFileSets(sets);
        }
        return tSink;
    }

    /**
     * Builds the {@code TIcebergMergeSink} (port of legacy {@code planner.IcebergMergeSink.bindDataSink}),
     * the UPDATE / MERGE dialect. Two parity traps vs the table/delete sinks: it carries
     * {@code compression_type} (field 8, NOT {@code compress_type}) and {@code sort_fields} (field 6, a
     * {@code List<TSortField>} built directly from the iceberg sort order, NOT the INSERT path's
     * {@code sort_info}). At format-version&ge;3 the schema-json includes the row-lineage fields. The
     * {@code rewritable_delete_file_sets} is attached from the scan-time supply (commit-bridge S4 part 2),
     * mirroring legacy {@code IcebergMergeExecutor.finalizeSinkForMerge} + {@code IcebergMergeSink.toThrift}'s
     * {@code formatVersion>=3 && !empty} gate.
     */
    private TIcebergMergeSink buildMergeSink(Table table, IcebergTableHandle tableHandle,
            Map<String, List<TIcebergDeleteFileDesc>> rewritableDeletes,
            boolean requireMergeCardinalityCheck, IcebergWriteSchemaContext schemaContext) {
        TIcebergMergeSink tSink = new TIcebergMergeSink();
        tSink.setDbName(tableHandle.getDbName());
        tSink.setTbName(tableHandle.getTableName());

        int formatVersion = schemaContext.getFormatVersion();
        tSink.setFormatVersion(formatVersion);
        Schema schema = schemaContext.getMergeSchema();
        tSink.setSchemaJson(schemaContext.getMergeSchemaJson());
        // #65782: gate BE-side column-stats collection on the table's iceberg metrics policy (v3-appended schema).
        tSink.setCollectColumnStats(
                IcebergWriterHelper.shouldCollectColumnStats(schemaContext, schema));
        // #66112: UPDATE and SQL MERGE share this sink, but only SQL MERGE has the one-source-row invariant.
        tSink.setRequireMergeCardinalityCheck(requireMergeCardinalityCheck);

        PartitionSpec partitionSpec = schemaContext.getPartitionSpec();
        if (partitionSpec.isPartitioned()) {
            tSink.setPartitionSpecsJson(Collections.singletonMap(
                    partitionSpec.specId(), schemaContext.getPartitionSpecJson()));
            tSink.setPartitionSpecId(partitionSpec.specId());
        }

        // Sort fields: identity sort-order fields whose source id is a base column, carrying the iceberg
        // source field id directly (BE merge writer field 6) — distinct from the INSERT path's sort_info(16).
        // A sorted table with no resolving identity column still emits an (empty) sort_fields list (legacy
        // sets it unconditionally inside the isSorted() branch).
        SortOrder sortOrder = schemaContext.getSortOrder();
        if (sortOrder.isSorted()) {
            tSink.setSortFields(buildMergeSortFields(schemaContext.getSchema(), sortOrder));
        }

        tSink.setFileFormat(toTFileFormatType(schemaContext.getFileFormat()));
        tSink.setCompressionType(toTFileCompressType(schemaContext.getFileCompression()));
        tSink.setHadoopConfig(buildHadoopConfig(table));

        LocationFields location = resolveLocationFields(table, schemaContext.getDataLocation());
        tSink.setOutputPath(location.outputPath);
        tSink.setOriginalOutputPath(location.rawLocation);
        tSink.setTableLocation(location.rawLocation);
        tSink.setFileType(location.fileType);
        if (!location.brokerAddresses.isEmpty()) {
            tSink.setBrokerAddresses(location.brokerAddresses);
        }

        // Delete side (position delete only).
        tSink.setDeleteType(TFileContent.POSITION_DELETES);
        tSink.setPartitionSpecIdForDelete(partitionSpec.specId());
        List<TIcebergRewritableDeleteFileSet> sets =
                buildRewritableDeleteFileSets(formatVersion, rewritableDeletes);
        if (!sets.isEmpty()) {
            tSink.setRewritableDeleteFileSets(sets);
        }
        return tSink;
    }

    /**
     * Builds the format-version&ge;3 {@code rewritable_delete_file_sets} thrift from the scan-time supply: one
     * {@code TIcebergRewritableDeleteFileSet} per touched data file (its raw path + its old non-equality delete
     * descs), so the BE OR-merges those old deletes into the new deletion vector. Returns empty — so the caller
     * leaves the thrift field unset, byte-identical to a no-rewrite write — for {@code formatVersion < 3} (v2
     * deletes are plain position-delete files, not DV-merged) or when there is no supply. Ports legacy
     * {@code IcebergRewritableDeletePlanner} (which keys on the same raw {@code originalPath}).
     */
    private static List<TIcebergRewritableDeleteFileSet> buildRewritableDeleteFileSets(
            int formatVersion, Map<String, List<TIcebergDeleteFileDesc>> rewritableDeletes) {
        if (formatVersion < 3 || rewritableDeletes == null || rewritableDeletes.isEmpty()) {
            return Collections.emptyList();
        }
        List<TIcebergRewritableDeleteFileSet> sets = new ArrayList<>(rewritableDeletes.size());
        for (Map.Entry<String, List<TIcebergDeleteFileDesc>> entry : rewritableDeletes.entrySet()) {
            TIcebergRewritableDeleteFileSet set = new TIcebergRewritableDeleteFileSet();
            set.setReferencedDataFilePath(entry.getKey());
            set.setDeleteFiles(entry.getValue());
            sets.add(set);
        }
        return sets;
    }

    private static List<TSortField> buildMergeSortFields(Schema schema, SortOrder sortOrder) {
        Set<Integer> baseColumnFieldIds = new HashSet<>();
        for (NestedField column : schema.columns()) {
            baseColumnFieldIds.add(column.fieldId());
        }
        List<TSortField> sortFields = new ArrayList<>();
        for (SortField sortField : sortOrder.fields()) {
            if (!sortField.transform().isIdentity()) {
                continue;
            }
            if (!baseColumnFieldIds.contains(sortField.sourceId())) {
                continue;
            }
            TSortField tSortField = new TSortField();
            tSortField.setSourceColumnId(sortField.sourceId());
            tSortField.setAscending(sortField.direction() == SortDirection.ASC);
            tSortField.setNullFirst(sortField.nullOrder() == NullOrder.NULLS_FIRST);
            sortFields.add(tSortField);
        }
        return sortFields;
    }

    /**
     * Resolves the shared sink location fields (port of legacy {@code LocationPath.of(dataLocation(table))}):
     * the raw data location, the normalized BE write path, and the BE file type — all vended-aware so a REST
     * catalog's object-store path still resolves. Used by all three sink dialects.
     */
    private LocationFields resolveLocationFields(Table table) {
        return resolveLocationFields(table, dataLocation(table));
    }

    private LocationFields resolveLocationFields(Table table, String rawLocation) {
        Map<String, String> vendedToken = IcebergScanPlanProvider.extractVendedToken(
                table, IcebergScanPlanProvider.restVendedCredentialsEnabled(properties));
        if (context != null) {
            TFileType fileType = TFileType.valueOf(storage().getBackendFileType(rawLocation, vendedToken));
            // A broker backend (ofs://, gfs:// -> FILE_BROKER) must also carry the broker addresses, or BE
            // gets a broker sink with an empty broker list and the write fails. Mirrors legacy
            // IcebergTableSink: resolve broker addresses only when fileType == FILE_BROKER (S3/HDFS/local
            // never touch the broker registry).
            List<TNetworkAddress> brokerAddresses = fileType == TFileType.FILE_BROKER
                    ? resolveBrokerAddresses() : Collections.emptyList();
            return new LocationFields(rawLocation,
                    storage().normalizeStorageUri(rawLocation, vendedToken), fileType, brokerAddresses);
        }
        return new LocationFields(rawLocation, rawLocation, TFileType.FILE_S3, Collections.emptyList());
    }

    /**
     * Resolves the broker backend addresses for a FILE_BROKER write through the neutral SPI (the engine owns
     * the broker registry + the catalog's bound broker name; the connector maps the neutral host/port pairs
     * to Thrift). Fails loud {@code "No alive broker."} when none is resolved — the same message legacy
     * {@code BaseExternalTableDataSink.getBrokerAddresses} threw — so a broker-backed write never silently
     * ships an empty broker list to BE.
     */
    private List<TNetworkAddress> resolveBrokerAddresses() {
        List<ConnectorBrokerAddress> addresses = storage().getBrokerAddresses();
        if (addresses.isEmpty()) {
            throw new DorisConnectorException("No alive broker.");
        }
        List<TNetworkAddress> result = new ArrayList<>(addresses.size());
        for (ConnectorBrokerAddress address : addresses) {
            result.add(new TNetworkAddress(address.getHost(), address.getPort()));
        }
        return result;
    }

    /** Immutable holder for the location fields shared by the sink dialects (broker addresses are populated
     * only for a FILE_BROKER target — empty otherwise). */
    private static final class LocationFields {
        private final String rawLocation;
        private final String outputPath;
        private final TFileType fileType;
        private final List<TNetworkAddress> brokerAddresses;

        LocationFields(String rawLocation, String outputPath, TFileType fileType,
                List<TNetworkAddress> brokerAddresses) {
            this.rawLocation = rawLocation;
            this.outputPath = outputPath;
            this.fileType = fileType;
            this.brokerAddresses = brokerAddresses;
        }
    }

    private Map<String, String> buildHadoopConfig(Table table) {
        Map<String, String> merged = new HashMap<>();
        if (context != null) {
            // Static catalog credentials in BE-canonical form (AWS_* for object stores, dfs/hadoop for HDFS),
            // sourced from the typed fe-filesystem StorageProperties bound by the catalog and handed over via
            // storage().getStorageProperties(): each backend's toBackendProperties().toMap() yields the canonical map
            // (design S3 — the write derives its BE creds from the SAME typed fe-filesystem source as the scan
            // path IcebergScanPlanProvider.getScanNodeProperties, retiring the redundant fe-core
            // getBackendStorageProperties() second parse). The BE S3 sink (s3_util.cpp
            // convert_properties_to_s3_conf) reads ONLY AWS_*, so the fs.s3a.* hadoop form (correct for the FE
            // iceberg-catalog Configuration) would leave the BE writer with no creds.
            for (StorageProperties sp : storage().getStorageProperties()) {
                sp.toBackendProperties().ifPresent(b -> merged.putAll(b.toMap()));
            }
            // REST per-table vended overlay (colliding key takes the vended value — legacy/scan precedence): a
            // vending catalog's static storage map is empty by design, so the vended creds are the only ones.
            merged.putAll(storage().vendStorageCredentials(
                    IcebergScanPlanProvider.extractVendedToken(
                            table, IcebergScanPlanProvider.restVendedCredentialsEnabled(properties))));
        }
        return merged;
    }

    private IcebergConnectorTransaction currentTransaction(ConnectorSession session) {
        Optional<ConnectorTransaction> transaction = session.getCurrentTransaction();
        if (!transaction.isPresent()) {
            throw new DorisConnectorException(
                    "Iceberg write requires an active connector transaction bound to the session; none is "
                            + "present. The executor must open it via beginTransaction and bind it to the "
                            + "session (wired at the iceberg cutover).");
        }
        return (IcebergConnectorTransaction) transaction.get();
    }

    private Table resolveTable(ConnectorSession session, IcebergTableHandle handle) {
        // Write shaping and beginWrite share a mutable table under a namespace separate from frozen reads;
        // newTransaction refreshes this object without changing a concurrently planned statement generation.
        // Resolve the per-request ops before the auth scope so a session=user fail-closed surfaces verbatim.
        IcebergCatalogOps ops = catalogOpsResolver.apply(session);
        return IcebergStatementScope.sharedWritableTable(session, handle.getDbName(), handle.getTableName(), () -> {
            if (context == null) {
                return ops.loadTable(handle.getDbName(), handle.getTableName());
            }
            try {
                return context.executeAuthenticated(
                        () -> ops.loadTable(handle.getDbName(), handle.getTableName()));
            } catch (Exception e) {
                throw new DorisConnectorException("Failed to load iceberg table "
                        + handle.getDbName() + "." + handle.getTableName() + ": " + e.getMessage(), e);
            }
        });
    }

    private IcebergWriteSchemaContext resolveWriteSchema(ConnectorSession session,
            IcebergTableHandle handle, Optional<String> branchName) {
        // Resolve the statement-pinned table before entering the write-schema cache loader. Both caches share
        // ConnectorStatementScope; nesting one computeIfAbsent inside the other can make ConcurrentHashMap reject
        // two distinct keys that happen to occupy the same bin as a recursive update.
        Table table = resolveTable(session, handle);
        return IcebergStatementScope.writeSchema(
                session, handle.getDbName(), handle.getTableName(), branchName,
                () -> IcebergWriteSchemaContext.create(
                        table, handle.getTableName(), branchName,
                        mappingVarbinaryEnabled(), mappingTimestampTzEnabled()));
    }

    private boolean mappingVarbinaryEnabled() {
        return Boolean.parseBoolean(properties.getOrDefault(
                IcebergConnectorProperties.ENABLE_MAPPING_VARBINARY, "false"));
    }

    private boolean mappingTimestampTzEnabled() {
        return Boolean.parseBoolean(properties.getOrDefault(
                IcebergConnectorProperties.ENABLE_MAPPING_TIMESTAMP_TZ, "false"));
    }

    private static TFileFormatType toTFileFormatType(FileFormat format) {
        switch (format) {
            case ORC:
                return TFileFormatType.FORMAT_ORC;
            case PARQUET:
                return TFileFormatType.FORMAT_PARQUET;
            default:
                throw new DorisConnectorException("Unsupported iceberg write file format: " + format);
        }
    }

    /** Port of legacy {@code BaseExternalTableDataSink.getTFileCompressType} (iceberg codecs). */
    private static TFileCompressType toTFileCompressType(String compressType) {
        if ("snappy".equalsIgnoreCase(compressType)) {
            return TFileCompressType.SNAPPYBLOCK;
        } else if ("lz4".equalsIgnoreCase(compressType)) {
            return TFileCompressType.LZ4BLOCK;
        } else if ("lzo".equalsIgnoreCase(compressType)) {
            return TFileCompressType.LZO;
        } else if ("zlib".equalsIgnoreCase(compressType)) {
            return TFileCompressType.ZLIB;
        } else if ("zstd".equalsIgnoreCase(compressType)) {
            return TFileCompressType.ZSTD;
        } else if ("gzip".equalsIgnoreCase(compressType)) {
            return TFileCompressType.GZ;
        } else if ("bzip2".equalsIgnoreCase(compressType)) {
            return TFileCompressType.BZ2;
        } else if ("uncompressed".equalsIgnoreCase(compressType)) {
            return TFileCompressType.PLAIN;
        } else {
            return TFileCompressType.PLAIN;
        }
    }

    /** Port of legacy {@code IcebergUtils.getFileCompress}. */
    static String getFileCompress(Table table) {
        Map<String, String> tableProps = table.properties();
        if (tableProps.containsKey(COMPRESSION_CODEC)) {
            return tableProps.get(COMPRESSION_CODEC);
        } else if (tableProps.containsKey(SPARK_SQL_COMPRESSION_CODEC)) {
            return tableProps.get(SPARK_SQL_COMPRESSION_CODEC);
        }
        FileFormat fileFormat = IcebergWriterHelper.getFileFormat(table);
        if (fileFormat == FileFormat.PARQUET) {
            return tableProps.getOrDefault(
                    TableProperties.PARQUET_COMPRESSION, TableProperties.PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0);
        } else if (fileFormat == FileFormat.ORC) {
            return tableProps.getOrDefault(
                    TableProperties.ORC_COMPRESSION, TableProperties.ORC_COMPRESSION_DEFAULT);
        }
        throw new DorisConnectorException("Unsupported iceberg write file format: " + fileFormat);
    }

    /** Port of legacy {@code IcebergUtils.dataLocation}. */
    static String dataLocation(Table table) {
        Map<String, String> tableProps = table.properties();
        if (tableProps.containsKey(TableProperties.WRITE_LOCATION_PROVIDER_IMPL)) {
            throw new DorisConnectorException("Table " + table.name() + " specifies "
                    + tableProps.get(TableProperties.WRITE_LOCATION_PROVIDER_IMPL) + " as a location provider. "
                    + "Writing to Iceberg tables with custom location provider is not supported.");
        }
        String dataLocation = tableProps.get(TableProperties.WRITE_DATA_LOCATION);
        if (dataLocation == null) {
            dataLocation = Boolean.parseBoolean(tableProps.get(TableProperties.OBJECT_STORE_ENABLED))
                    ? tableProps.get(TableProperties.OBJECT_STORE_PATH) : null;
            if (dataLocation == null) {
                dataLocation = tableProps.get(TableProperties.WRITE_FOLDER_STORAGE_LOCATION);
                if (dataLocation == null) {
                    dataLocation = String.format("%s/data", LocationUtil.stripTrailingSlash(table.location()));
                }
            }
        }
        return dataLocation;
    }

    /** This catalog's engine-owned storage services (see {@link ConnectorContext#getStorageContext()}). */
    private ConnectorStorageContext storage() {
        return context.getStorageContext();
    }
}
