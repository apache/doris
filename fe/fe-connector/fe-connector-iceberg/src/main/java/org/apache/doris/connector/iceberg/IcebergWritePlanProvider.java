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

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.handle.ConnectorWriteHandle;
import org.apache.doris.connector.api.handle.WriteOperation;
import org.apache.doris.connector.api.write.ConnectorSinkPlan;
import org.apache.doris.connector.api.write.ConnectorWritePartitionField;
import org.apache.doris.connector.api.write.ConnectorWritePartitionSpec;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;
import org.apache.doris.connector.api.write.ConnectorWriteSortColumn;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TIcebergDeleteSink;
import org.apache.doris.thrift.TIcebergMergeSink;
import org.apache.doris.thrift.TIcebergTableSink;
import org.apache.doris.thrift.TSortField;

import com.google.common.collect.Maps;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
 * (DV-T0x-vended / -broker / -materialize) closed at the P6.6 cutover. The DELETE / MERGE sink's
 * format-version&ge;3 {@code rewritable_delete_file_sets} is a post-finalize injection driven by the
 * fe-resident rewritable-delete planner and lands with the command shell (T07c); the {@code bindDataSink}
 * port here never stamps it.</p>
 *
 * <p><b>Gate-closed / dormant.</b> Iceberg is not in {@code SPI_READY_TYPES} until P6.6, so nothing
 * routes iceberg writes through this provider yet; {@link #planWrite} requires the executor-bound
 * connector transaction and fails loud if absent.</p>
 */
public class IcebergWritePlanProvider implements ConnectorWritePlanProvider {

    // Legacy IcebergUtils compression-codec property keys (connector-local copies; iceberg SDK has no
    // constant for the doris/spark-sql forms).
    private static final String COMPRESSION_CODEC = "compression-codec";
    private static final String SPARK_SQL_COMPRESSION_CODEC = "spark.sql.iceberg.compression-codec";

    // Connector-local literal copy of fe-core's Column.ICEBERG_ROWID_COL (connectors must not import
    // fe-core). The fe-core ConnectorColumnConverterTest contract pin asserts that converting this exact
    // declared shape yields the legacy IcebergRowId.createHiddenColumn() (name / STRUCT / invisible /
    // not-null), so a drift on either side turns one of the two tests red.
    private static final String DORIS_ICEBERG_ROWID_COL = "__DORIS_ICEBERG_ROWID_COL__";

    // The single request-scoped synthetic write column iceberg declares: the row-id STRUCT carrying the
    // per-row write metadata (file_path / row_position / partition_spec_id / partition_data). Same for
    // every iceberg table regardless of format/partitioning (mirrors legacy IcebergRowId), so it is a
    // shared immutable instance.
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
    private final IcebergCatalogOps catalogOps;
    private final ConnectorContext context;

    public IcebergWritePlanProvider(Map<String, String> properties, IcebergCatalogOps catalogOps,
            ConnectorContext context) {
        this.properties = properties;
        this.catalogOps = catalogOps;
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
        IcebergWriteContext writeContext = buildWriteContext(handle);
        transaction.beginWrite(session, tableHandle.getDbName(), tableHandle.getTableName(), writeContext);
        Table table = transaction.getTable();

        // Dispatch on the write operation to the matching BE sink dialect (each is a distinct TDataSinkType,
        // byte-identical to the legacy fe-core planner sink). OVERWRITE shares TIcebergTableSink with INSERT
        // (the overwrite flag is read from the handle); UPDATE shares TIcebergMergeSink with MERGE.
        switch (writeContext.getWriteOperation()) {
            case INSERT:
            case OVERWRITE: {
                TDataSink dataSink = new TDataSink(TDataSinkType.ICEBERG_TABLE_SINK);
                dataSink.setIcebergTableSink(buildSink(table, tableHandle, handle));
                return new ConnectorSinkPlan(dataSink);
            }
            case DELETE: {
                TDataSink dataSink = new TDataSink(TDataSinkType.ICEBERG_DELETE_SINK);
                dataSink.setIcebergDeleteSink(buildDeleteSink(table, tableHandle));
                return new ConnectorSinkPlan(dataSink);
            }
            case UPDATE:
            case MERGE: {
                TDataSink dataSink = new TDataSink(TDataSinkType.ICEBERG_MERGE_SINK);
                dataSink.setIcebergMergeSink(buildMergeSink(table, tableHandle));
                return new ConnectorSinkPlan(dataSink);
            }
            default:
                throw new DorisConnectorException(
                        "Unsupported iceberg write operation: " + writeContext.getWriteOperation());
        }
    }

    @Override
    public void appendExplainInfo(StringBuilder output, String prefix,
            ConnectorSession session, ConnectorWriteHandle handle) {
        // Surface the connector-specific write detail the generic plugin-driven sink line cannot (mirrors
        // the legacy IcebergTableSink.getExplainString "ICEBERG TABLE SINK / Table: <name>" block).
        IcebergTableHandle tableHandle = (IcebergTableHandle) handle.getTableHandle();
        output.append(prefix).append("  ICEBERG TABLE: ")
                .append(tableHandle.getDbName()).append(".").append(tableHandle.getTableName()).append("\n");
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
        Table table = resolveTable((IcebergTableHandle) tableHandle);
        SortOrder sortOrder = table.sortOrder();
        if (!sortOrder.isSorted()) {
            // null == "no write sort order" (legacy gates setSortInfo on isSorted()). A sorted table
            // returns a (possibly empty) list so the engine still emits a TSortInfo, matching legacy's
            // unconditional setSortInfo inside the isSorted() branch even when no identity column resolves.
            return null;
        }
        List<NestedField> columns = table.schema().columns();
        List<ConnectorWriteSortColumn> result = new ArrayList<>();
        for (SortField sortField : sortOrder.fields()) {
            if (!sortField.transform().isIdentity()) {
                continue;
            }
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).fieldId() == sortField.sourceId()) {
                    result.add(new ConnectorWriteSortColumn(i,
                            sortField.direction() == SortDirection.ASC,
                            sortField.nullOrder() == NullOrder.NULLS_FIRST));
                    break;
                }
            }
        }
        return result;
    }

    @Override
    public ConnectorWritePartitionSpec getWritePartitioning(ConnectorSession session,
            ConnectorTableHandle tableHandle) {
        Table table = resolveTable((IcebergTableHandle) tableHandle);
        PartitionSpec spec = table.spec();
        if (spec == null || !spec.isPartitioned()) {
            // null == "unpartitioned" (legacy PhysicalIcebergMergeSink.buildInsertPartitionFields gates on
            // spec().isPartitioned()) -> the engine uses its non-partitioned merge distribution.
            return null;
        }
        Schema schema = table.schema();
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
        // format/partitioning — it mirrors legacy IcebergExternalTable.getFullSchema appending
        // IcebergRowId.createHiddenColumn() whenever a DML (or show-hidden) is in flight. fe-core gates the
        // actual injection request-side (show-hidden / synthetic-write-column ctx flag); here we only
        // declare it, so neither the session nor the table handle is consulted.
        return SYNTHETIC_WRITE_COLUMNS;
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

    private IcebergWriteContext buildWriteContext(ConnectorWriteHandle handle) {
        WriteOperation op = handle.getWriteOperation();
        if (op == WriteOperation.INSERT && handle.isOverwrite()) {
            op = WriteOperation.OVERWRITE;
        }
        // Branch-targeted INSERT (INSERT INTO tbl@branch): the branch is threaded from the generic insert
        // command context onto the write handle; beginWrite validates it against the table refs and points
        // the commit at the branch. Empty for a default-ref write.
        return new IcebergWriteContext(op, handle.isOverwrite(), handle.getWriteContext(), handle.getBranchName());
    }

    private TIcebergTableSink buildSink(Table table, IcebergTableHandle tableHandle,
            ConnectorWriteHandle handle) {
        TIcebergTableSink tSink = new TIcebergTableSink();
        tSink.setDbName(tableHandle.getDbName());
        tSink.setTbName(tableHandle.getTableName());

        // Schema (no v3 row-lineage append — that is REWRITE/procedures, P6.4).
        tSink.setSchemaJson(SchemaParser.toJson(table.schema()));

        // Partition spec (only for a partitioned table, mirroring legacy spec().isPartitioned()).
        if (table.spec().isPartitioned()) {
            tSink.setPartitionSpecsJson(Maps.transformValues(table.specs(), PartitionSpecParser::toJson));
            tSink.setPartitionSpecId(table.spec().specId());
        }

        // Sort info: the engine builds the TSortInfo from the connector-declared write-sort columns
        // (getWriteSortColumns) and threads it back on the handle; the connector stamps it verbatim.
        if (handle.getSortInfo() != null) {
            tSink.setSortInfo(handle.getSortInfo());
        }

        // File format / compression.
        tSink.setFileFormat(toTFileFormatType(IcebergWriterHelper.getFileFormat(table)));
        tSink.setCompressionType(toTFileCompressType(getFileCompress(table)));

        // Hadoop config (static storage properties). The vended-credentials overlay (REST object-store
        // writes) is DV-T06-vended: vendStorageCredentials yields BE-canonical creds, not the hadoop
        // (fs.s3a.*) form the sink needs — closed at the P6.6 cutover.
        tSink.setHadoopConfig(buildHadoopConfig());

        // Output location: normalized for the BE writer, raw kept as the original; the BE file type comes
        // from the engine (broker-aware). All vended-aware so a REST catalog's path still resolves.
        LocationFields location = resolveLocationFields(table);
        tSink.setOutputPath(location.outputPath);
        tSink.setFileType(location.fileType);
        tSink.setOriginalOutputPath(location.rawLocation);

        // Overwrite + static partition values (INSERT OVERWRITE ... PARTITION).
        tSink.setOverwrite(handle.isOverwrite());
        if (handle.isOverwrite() && handle.getWriteContext() != null && !handle.getWriteContext().isEmpty()) {
            tSink.setStaticPartitionValues(handle.getWriteContext());
        }
        return tSink;
    }

    /**
     * Builds the {@code TIcebergDeleteSink} (port of legacy {@code planner.IcebergDeleteSink.bindDataSink}).
     * Iceberg delete is always a position delete. ⚠️ The delete sink carries {@code compress_type} (thrift
     * field 6), NOT the table/merge sink's {@code compression_type}. The format-version&ge;3
     * {@code rewritable_delete_file_sets} is injected post-finalize (T07c), never here.
     */
    private TIcebergDeleteSink buildDeleteSink(Table table, IcebergTableHandle tableHandle) {
        TIcebergDeleteSink tSink = new TIcebergDeleteSink();
        tSink.setDbName(tableHandle.getDbName());
        tSink.setTbName(tableHandle.getTableName());
        tSink.setDeleteType(TFileContent.POSITION_DELETES);
        tSink.setFileFormat(toTFileFormatType(IcebergWriterHelper.getFileFormat(table)));
        tSink.setCompressType(toTFileCompressType(getFileCompress(table)));
        tSink.setHadoopConfig(buildHadoopConfig());

        LocationFields location = resolveLocationFields(table);
        tSink.setOutputPath(location.outputPath);
        tSink.setTableLocation(location.rawLocation);
        tSink.setFileType(location.fileType);

        if (table.spec().isPartitioned()) {
            tSink.setPartitionSpecId(table.spec().specId());
        }
        tSink.setFormatVersion(IcebergWriterHelper.getFormatVersion(table));
        return tSink;
    }

    /**
     * Builds the {@code TIcebergMergeSink} (port of legacy {@code planner.IcebergMergeSink.bindDataSink}),
     * the UPDATE / MERGE dialect. Two parity traps vs the table/delete sinks: it carries
     * {@code compression_type} (field 8, NOT {@code compress_type}) and {@code sort_fields} (field 6, a
     * {@code List<TSortField>} built directly from the iceberg sort order, NOT the INSERT path's
     * {@code sort_info}). At format-version&ge;3 the schema-json includes the row-lineage fields. The
     * {@code rewritable_delete_file_sets} is injected post-finalize (T07c), never here.
     */
    private TIcebergMergeSink buildMergeSink(Table table, IcebergTableHandle tableHandle) {
        TIcebergMergeSink tSink = new TIcebergMergeSink();
        tSink.setDbName(tableHandle.getDbName());
        tSink.setTbName(tableHandle.getTableName());

        int formatVersion = IcebergWriterHelper.getFormatVersion(table);
        tSink.setFormatVersion(formatVersion);
        Schema schema = formatVersion >= 3
                ? IcebergWriterHelper.appendRowLineageFieldsForV3(table.schema()) : table.schema();
        tSink.setSchemaJson(SchemaParser.toJson(schema));

        if (table.spec().isPartitioned()) {
            tSink.setPartitionSpecsJson(Maps.transformValues(table.specs(), PartitionSpecParser::toJson));
            tSink.setPartitionSpecId(table.spec().specId());
        }

        // Sort fields: identity sort-order fields whose source id is a base column, carrying the iceberg
        // source field id directly (BE merge writer field 6) — distinct from the INSERT path's sort_info(16).
        // A sorted table with no resolving identity column still emits an (empty) sort_fields list (legacy
        // sets it unconditionally inside the isSorted() branch).
        SortOrder sortOrder = table.sortOrder();
        if (sortOrder.isSorted()) {
            tSink.setSortFields(buildMergeSortFields(table, sortOrder));
        }

        tSink.setFileFormat(toTFileFormatType(IcebergWriterHelper.getFileFormat(table)));
        tSink.setCompressionType(toTFileCompressType(getFileCompress(table)));
        tSink.setHadoopConfig(buildHadoopConfig());

        LocationFields location = resolveLocationFields(table);
        tSink.setOutputPath(location.outputPath);
        tSink.setOriginalOutputPath(location.rawLocation);
        tSink.setTableLocation(location.rawLocation);
        tSink.setFileType(location.fileType);

        // Delete side (position delete only).
        tSink.setDeleteType(TFileContent.POSITION_DELETES);
        if (table.spec().isPartitioned()) {
            tSink.setPartitionSpecIdForDelete(table.spec().specId());
        }
        return tSink;
    }

    private static List<TSortField> buildMergeSortFields(Table table, SortOrder sortOrder) {
        Set<Integer> baseColumnFieldIds = new HashSet<>();
        for (NestedField column : table.schema().columns()) {
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
        String rawLocation = dataLocation(table);
        Map<String, String> vendedToken = IcebergScanPlanProvider.extractVendedToken(
                table, IcebergScanPlanProvider.restVendedCredentialsEnabled(properties));
        if (context != null) {
            return new LocationFields(rawLocation,
                    context.normalizeStorageUri(rawLocation, vendedToken),
                    TFileType.valueOf(context.getBackendFileType(rawLocation, vendedToken)));
        }
        return new LocationFields(rawLocation, rawLocation, TFileType.FILE_S3);
    }

    /** Immutable holder for the three location fields shared by the sink dialects. */
    private static final class LocationFields {
        private final String rawLocation;
        private final String outputPath;
        private final TFileType fileType;

        LocationFields(String rawLocation, String outputPath, TFileType fileType) {
            this.rawLocation = rawLocation;
            this.outputPath = outputPath;
            this.fileType = fileType;
        }
    }

    private Map<String, String> buildHadoopConfig() {
        Map<String, String> merged = new HashMap<>();
        if (context != null) {
            for (StorageProperties sp : context.getStorageProperties()) {
                sp.toHadoopProperties().ifPresent(h -> merged.putAll(h.toHadoopConfigurationMap()));
            }
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

    private Table resolveTable(IcebergTableHandle handle) {
        if (context == null) {
            return catalogOps.loadTable(handle.getDbName(), handle.getTableName());
        }
        try {
            return context.executeAuthenticated(
                    () -> catalogOps.loadTable(handle.getDbName(), handle.getTableName()));
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to load iceberg table "
                    + handle.getDbName() + "." + handle.getTableName() + ": " + e.getMessage(), e);
        }
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
    private static String getFileCompress(Table table) {
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
    private static String dataLocation(Table table) {
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
}
