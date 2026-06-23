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

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.handle.ConnectorWriteHandle;
import org.apache.doris.connector.api.handle.WriteOperation;
import org.apache.doris.connector.api.write.ConnectorSinkPlan;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;
import org.apache.doris.connector.api.write.ConnectorWriteSortColumn;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TIcebergTableSink;

import com.google.common.collect.Maps;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.LocationUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
 * <p><b>Scope (P6.3-T06).</b> INSERT / OVERWRITE only ({@code TIcebergTableSink}); the DELETE / MERGE
 * sink dialects come with the row-level DML command shell (T07). REWRITE (procedures, P6.4) is not
 * built here. The write distribution and the vended-credentials overlay of the hadoop config are
 * registered deviations closed at the P6.6 cutover (see the T06 design doc).</p>
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

        TIcebergTableSink tSink = buildSink(table, tableHandle, handle);
        TDataSink dataSink = new TDataSink(TDataSinkType.ICEBERG_TABLE_SINK);
        dataSink.setIcebergTableSink(tSink);
        return new ConnectorSinkPlan(dataSink);
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

    private IcebergWriteContext buildWriteContext(ConnectorWriteHandle handle) {
        WriteOperation op = handle.getWriteOperation();
        if (op == WriteOperation.INSERT && handle.isOverwrite()) {
            op = WriteOperation.OVERWRITE;
        }
        // Branch-targeted INSERT (INSERT INTO tbl@branch) is threaded at the P6.6 cutover via the generic
        // command context; the generic write handle carries no branch (DV-T06-branch).
        return new IcebergWriteContext(op, handle.isOverwrite(), handle.getWriteContext(), Optional.empty());
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
        String rawLocation = dataLocation(table);
        Map<String, String> vendedToken = IcebergScanPlanProvider.extractVendedToken(
                table, IcebergScanPlanProvider.restVendedCredentialsEnabled(properties));
        if (context != null) {
            tSink.setOutputPath(context.normalizeStorageUri(rawLocation, vendedToken));
            tSink.setFileType(TFileType.valueOf(context.getBackendFileType(rawLocation, vendedToken)));
        } else {
            tSink.setOutputPath(rawLocation);
            tSink.setFileType(TFileType.FILE_S3);
        }
        tSink.setOriginalOutputPath(rawLocation);

        // Overwrite + static partition values (INSERT OVERWRITE ... PARTITION).
        tSink.setOverwrite(handle.isOverwrite());
        if (handle.isOverwrite() && handle.getWriteContext() != null && !handle.getWriteContext().isEmpty()) {
            tSink.setStaticPartitionValues(handle.getWriteContext());
        }
        return tSink;
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
