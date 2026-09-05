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

package org.apache.doris.planner;

import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.credentials.CredentialUtils;
import org.apache.doris.datasource.credentials.VendedCredentialsFactory;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonTransaction;
import org.apache.doris.datasource.paimon.PaimonUtil;
import org.apache.doris.datasource.paimon.PaimonWriteBinding;
import org.apache.doris.datasource.paimon.PaimonWriteTarget;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.PaimonInsertCommandContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPaimonNativeWriteInfo;
import org.apache.doris.thrift.TPaimonTableSink;
import org.apache.doris.thrift.TPaimonWriteBackendType;
import org.apache.doris.thrift.TPaimonWriteMode;

import com.google.common.base.Preconditions;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Paimon table sink.
 *
 * Generates TPaimonTableSink payload consumed by BE, including serialized table
 * metadata, Hadoop authentication config, transaction identity, write mode,
 * and sink column names.
 *
 * The upstream Exchange may establish concurrent writer ownership. Partition
 * and fixed-bucket routing remain on the SDK path until later native phases.
 */
public class PaimonTableSink extends BaseExternalTableDataSink {
    public static final String ROW_KIND_COLUMN = "__DORIS_PAIMON_ROW_KIND__";
    private final PaimonExternalTable targetTable;
    private final PaimonWriteTarget writeTarget;
    private final DMLCommandType dmlCommandType;
    private List<Expr> outputExprs;
    private List<Column> cols;

    private static final HashSet<TFileFormatType> supportedTypes = new HashSet<TFileFormatType>() {{
            add(TFileFormatType.FORMAT_ORC);
            add(TFileFormatType.FORMAT_PARQUET);
        }};

    public PaimonTableSink(PaimonWriteTarget writeTarget, DMLCommandType dmlCommandType) {
        super();
        this.writeTarget = writeTarget;
        this.targetTable = writeTarget.getDorisTable();
        this.dmlCommandType = dmlCommandType;
    }

    public void setCols(List<Column> cols) {
        this.cols = cols;
    }

    public void setOutputExprs(List<Expr> outputExprs) {
        this.outputExprs = outputExprs;
    }

    @Override
    protected Set<TFileFormatType> supportedFileFormatTypes() {
        return supportedTypes;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix).append("PAIMON TABLE SINK\n");
        if (explainLevel == TExplainLevel.BRIEF) {
            return strBuilder.toString();
        }
        strBuilder.append(prefix).append("  table: ").append(targetTable.getName()).append("\n");
        return strBuilder.toString();
    }

    @Override
    public void bindDataSink(Optional<InsertCommandContext> insertCtx) throws AnalysisException {
        TPaimonTableSink tSink = new TPaimonTableSink();
        PaimonInsertCommandContext ctx = (PaimonInsertCommandContext) insertCtx.get();
        Preconditions.checkState(ctx.getTxnId() > 0,
                "Paimon transaction must begin before sink binding");

        PaimonTransaction transaction;
        PaimonWriteBinding binding;
        try {
            transaction = (PaimonTransaction) targetTable.getCatalog()
                    .getTransactionManager().getTransaction(ctx.getTxnId());
            binding = PaimonWriteBinding.create(writeTarget, ctx);
        } catch (AnalysisException e) {
            throw e;
        } catch (UserException e) {
            throw new AnalysisException("Failed to bind Paimon write transaction: "
                    + e.getMessage(), e);
        }
        transaction.bind(binding);

        tSink.setTransactionId(ctx.getTxnId());
        tSink.setCommitUser(ctx.getCommitUser());

        // Thrift column_names is the single column-order protocol shared by BE
        // Arrow conversion and the Java writer schema.
        List<String> outputColumnNames = outputColumnNames();

        // FE owns table metadata resolution. BE and the JNI writer consume this
        // exact table instance instead of loading catalog metadata independently.
        tSink.setSerializedTable(binding.getSerializedTable());

        if (isChangelogWrite()) {
            tSink.setWriteMode(TPaimonWriteMode.CHANGELOG);
        } else if (ctx.isOverwrite()) {
            tSink.setWriteMode(TPaimonWriteMode.OVERWRITE);
        } else {
            tSink.setWriteMode(TPaimonWriteMode.APPEND);
        }

        if (usePhaseOneNativeWriter(binding.getTable(), ctx)) {
            tSink.setBackendType(TPaimonWriteBackendType.NATIVE);
            configurePhaseOneNativeWriter(tSink, binding);
        } else {
            tSink.setBackendType(TPaimonWriteBackendType.JNI);
            tSink.setHadoopConfig(binding.getHadoopConfig());
        }

        tSink.setColumnNames(outputColumnNames);

        tDataSink = new TDataSink(TDataSinkType.PAIMON_TABLE_SINK);
        tDataSink.setPaimonTableSink(tSink);
    }

    private List<String> outputColumnNames() throws AnalysisException {
        int columnOffset = isChangelogWrite() ? 1 : 0;
        if (cols.size() + columnOffset != outputExprs.size()) {
            throw new AnalysisException("Paimon sink output column size mismatch, columns="
                    + cols.size() + ", exprs=" + outputExprs.size());
        }
        List<String> names = new ArrayList<>(outputExprs.size());
        if (isChangelogWrite()) {
            names.add(ROW_KIND_COLUMN);
        }
        for (Column col : cols) {
            names.add(col.getName());
        }
        return names;
    }

    private boolean isChangelogWrite() {
        return dmlCommandType == DMLCommandType.UPDATE
                || dmlCommandType == DMLCommandType.DELETE
                || dmlCommandType == DMLCommandType.MERGE;
    }

    private boolean usePhaseOneNativeWriter(FileStoreTable table,
            PaimonInsertCommandContext ctx) {
        ConnectContext connectContext = ConnectContext.get();
        if ((connectContext != null
                && !connectContext.getSessionVariable().isPaimonNativeInsertMode())
                || isChangelogWrite()
                || ctx.isOverwrite()
                || !table.primaryKeys().isEmpty()
                || !table.partitionKeys().isEmpty()
                || table.bucketMode() != BucketMode.BUCKET_UNAWARE) {
            return false;
        }

        CoreOptions options = CoreOptions.fromMap(table.options());
        String dataFilePrefix = options.dataFilePrefix();
        if (!CoreOptions.FILE_FORMAT_PARQUET.equals(options.formatType())
                || options.rowTrackingEnabled()
                || !isPhaseOneStatsMode(table, options)
                || options.dataFilePathDirectory() != null
                || options.fileSuffixIncludeCompression()
                || !isPhaseOneCompression(options.fileCompression())
                || dataFilePrefix == null
                || dataFilePrefix.isEmpty()
                || dataFilePrefix.contains("/")
                || dataFilePrefix.contains("\\")) {
            return false;
        }

        // Defaults and partial-column inserts are still normalized by the Java writer in phase 1.
        // Native mode is selected only when the BE receives the complete pinned table schema.
        List<DataField> fields = table.schema().fields();
        if (cols.size() != fields.size()) {
            return false;
        }
        for (int i = 0; i < fields.size(); i++) {
            if (!fields.get(i).name().equalsIgnoreCase(cols.get(i).getName())
                    || !isPhaseOneNativeType(fields.get(i).type())) {
                return false;
            }
        }

        // Native phase 1 does not emit Paimon sidecar indexes or external data paths.
        for (String key : table.options().keySet()) {
            if (key.startsWith("file-index.")
                    || key.startsWith("index.")
                    || (key.startsWith("fields.") && key.endsWith(".file-index"))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isPhaseOneStatsMode(FileStoreTable table, CoreOptions options) {
        if (!"none".equalsIgnoreCase(options.statsMode())) {
            return false;
        }
        for (String levelMode : options.statsModePerLevel().values()) {
            if (!"none".equalsIgnoreCase(levelMode)) {
                return false;
            }
        }
        for (Map.Entry<String, String> entry : table.options().entrySet()) {
            if (entry.getKey().startsWith("fields.")
                    && entry.getKey().endsWith(".stats-mode")
                    && !"none".equalsIgnoreCase(entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    private static boolean isPhaseOneNativeType(DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case DATE:
                return true;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                // Paimon encodes precision > 6 as INT96. Doris DATETIMEV2 is microsecond based,
                // so keeping those tables on the SDK path avoids silent precision loss.
                return ((TimestampType) type).getPrecision() <= 6;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return ((LocalZonedTimestampType) type).getPrecision() <= 6;
            case ARRAY:
                return isPhaseOneNativeType(((ArrayType) type).getElementType());
            case MAP:
                MapType map = (MapType) type;
                return isPhaseOneNativeType(map.getKeyType())
                        && isPhaseOneNativeType(map.getValueType());
            case ROW:
                for (DataField field : ((RowType) type).getFields()) {
                    if (!isPhaseOneNativeType(field.type())) {
                        return false;
                    }
                }
                return true;
            default:
                // VARIANT is added in phase 2. TIME, MULTISET and BLOB are intentionally
                // kept on the SDK path until Doris has lossless native representations.
                return false;
        }
    }

    private static boolean isPhaseOneCompression(String compression) {
        if (compression == null) {
            return false;
        }
        switch (compression.toLowerCase(Locale.ROOT)) {
            case "none":
            case "uncompressed":
            case "snappy":
            case "zstd":
            case "lz4":
            case "gzip":
                return true;
            default:
                return false;
        }
    }

    private void configurePhaseOneNativeWriter(TPaimonTableSink tSink, PaimonWriteBinding binding)
            throws AnalysisException {
        FileStoreTable table = binding.getTable();
        CoreOptions options = CoreOptions.fromMap(table.options());
        PaimonExternalCatalog catalog = (PaimonExternalCatalog) targetTable.getCatalog();
        Map<StorageProperties.Type, StorageProperties> storagePropertiesMap =
                VendedCredentialsFactory.getStoragePropertiesMapWithVendedCredentials(
                        catalog.getCatalogProperty().getMetastoreProperties(),
                        catalog.getCatalogProperty().getStoragePropertiesMap(), table);
        TPaimonNativeWriteInfo nativeInfo = new TPaimonNativeWriteInfo();
        nativeInfo.setSchema(PaimonUtil.getSchemaInfo(table.schema(), true, true));

        String tableLocation = table.location().toString();
        String bucketPath = (tableLocation.endsWith("/") ? tableLocation : tableLocation + "/")
                + "bucket-0";
        LocationPath locationPath = LocationPath.of(bucketPath, storagePropertiesMap);
        nativeInfo.setOutputPath(locationPath.toStorageLocation().toString());
        TFileType fileType = locationPath.getTFileTypeForBE();
        nativeInfo.setFileType(fileType);
        nativeInfo.setFileFormat(TFileFormatType.FORMAT_PARQUET);
        nativeInfo.setCompressionType(getTFileCompressType(options.fileCompression()));
        nativeInfo.setTargetFileSizeBytes(options.targetFileSize(false));
        nativeInfo.setDataFilePrefix(options.dataFilePrefix());
        if (fileType == TFileType.FILE_BROKER) {
            nativeInfo.setBrokerAddresses(getBrokerAddresses(targetTable.getCatalog().bindBrokerName()));
        }

        Map<String, String> backendProperties = new java.util.HashMap<>(binding.getHadoopConfig());
        backendProperties.putAll(
                CredentialUtils.getBackendPropertiesFromStorageMap(storagePropertiesMap));
        // Hadoop authentication settings in the transaction binding are also required by HDFS.
        tSink.setHadoopConfig(backendProperties);
        tSink.setNativeWriteInfo(nativeInfo);
    }

}
