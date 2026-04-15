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

import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.nereids.trees.plans.commands.insert.BaseExternalTableInsertCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TPaimonTableSink;
import org.apache.doris.thrift.TPaimonWriteShuffleMode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.utils.InstantiationUtil;

import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Paimon table sink
 *
 * This class materializes the TPaimonTableSink payload consumed by BE,
 * including table location, write options, partition keys, bucket keys,
 * shuffle mode and sink column names.
 */
public class PaimonTableSink extends BaseExternalTableDataSink {
    private static final Logger LOG = LogManager.getLogger(PaimonTableSink.class);
    private final PaimonExternalTable targetTable;
    private static final Base64.Encoder BASE64_ENCODER = java.util.Base64.getUrlEncoder().withoutPadding();
    private static final HashSet<TFileFormatType> supportedTypes = new HashSet<TFileFormatType>() {{
            add(TFileFormatType.FORMAT_ORC);
            add(TFileFormatType.FORMAT_PARQUET);
        }};

    public PaimonTableSink(PaimonExternalTable targetTable) {
        super();
        this.targetTable = targetTable;
    }

    // List of columns to be written to the sink, used to populate columnNames in Thrift
    private List<Column> cols;

    public void setCols(List<Column> cols) {
        this.cols = cols;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix).append("PAIMON TABLE SINK\n");
        if (explainLevel == TExplainLevel.BRIEF) {
            return strBuilder.toString();
        }
        return strBuilder.toString();
    }

    @Override
    protected Set<TFileFormatType> supportedFileFormatTypes() {
        return supportedTypes;
    }

    @Override
    public void bindDataSink(Optional<InsertCommandContext> insertCtx) throws AnalysisException {
        TPaimonTableSink tSink = new TPaimonTableSink();

        // basic identifiers
        tSink.setCatalogName(targetTable.getCatalog().getName());
        tSink.setDbName(targetTable.getDbName());
        tSink.setTbName(targetTable.getName());

        Map<String, String> catalogProps = targetTable.getCatalog().getCatalogProperty().getHadoopProperties();
        Map<String, String> options = new HashMap<>();
        options.putAll(catalogProps);
        String warehouse = ((PaimonExternalCatalog) targetTable.getCatalog()).getPaimonOptionsMap()
                .get(CatalogOptions.WAREHOUSE.key());
        String defaultFsName = resolveDefaultFsName(warehouse);
        if (defaultFsName != null && !defaultFsName.isEmpty()) {
            String currentDefaultFs = options.get("fs.defaultFS");
            if (currentDefaultFs == null || currentDefaultFs.isEmpty() || currentDefaultFs.startsWith("file:/")) {
                options.put("fs.defaultFS", defaultFsName);
            }
        }
        if (insertCtx.isPresent() && insertCtx.get() instanceof BaseExternalTableInsertCommandContext) {
            BaseExternalTableInsertCommandContext ctx = (BaseExternalTableInsertCommandContext) insertCtx.get();
            if (ctx.getTxnId() > 0) {
                options.put("doris.commit_identifier", String.valueOf(ctx.getTxnId()));
            }
            if (ctx.getCommitUser() != null && !ctx.getCommitUser().isEmpty()) {
                options.put("doris.commit_user", ctx.getCommitUser());
            }
        }

        if (ConnectContext.get() != null) {
            options.put("target-file-size",
                    String.valueOf(ConnectContext.get().getSessionVariable().paimonTargetFileSize));
            options.put("write-buffer-size",
                    String.valueOf(ConnectContext.get().getSessionVariable().paimonWriteBufferSize));
            String hadoopUser = options.get("hadoop.username");
            if (hadoopUser == null || hadoopUser.isEmpty()) {
                hadoopUser = options.get("hadoop.user.name");
            }
            if (hadoopUser == null || hadoopUser.isEmpty()) {
                hadoopUser = "hadoop";
            }
            options.put("hadoop.user.name", hadoopUser);
            options.put("hadoop.username", hadoopUser);
        }

        String tableLocation = null;
        org.apache.paimon.table.Table paimonTable =
                targetTable.getPaimonTable(MvccUtil.getSnapshotFromContext(targetTable));
        if (paimonTable instanceof org.apache.paimon.table.FileStoreTable) {
            tableLocation = ((org.apache.paimon.table.FileStoreTable) paimonTable).location().toString();
        }
        tableLocation = normalizeTableLocation(tableLocation);
        if (tableLocation == null || tableLocation.isEmpty()) {
            if (warehouse != null && !warehouse.isEmpty()) {
                String base = warehouse.endsWith("/") ? warehouse : warehouse + "/";
                tableLocation = base + targetTable.getDbName() + ".db/" + targetTable.getName() + "/";
            }
        } else if (defaultFsName != null && !defaultFsName.isEmpty() && tableLocation.startsWith("hdfs://")) {
            try {
                URI tableLocationUri = URI.create(tableLocation);
                String path = tableLocationUri.getPath();
                if (path != null && !path.isEmpty()) {
                    tableLocation = defaultFsName + path;
                }
            } catch (Exception e) {
                LOG.warn("paimon: failed to align table location {} with default fs {}", tableLocation, defaultFsName);
            }
        } else if (!tableLocation.contains("://") && defaultFsName != null && !defaultFsName.isEmpty()) {
            String tablePath = tableLocation.startsWith("/") ? tableLocation : "/" + tableLocation;
            tableLocation = defaultFsName + tablePath;
        }
        if (tableLocation != null && !tableLocation.isEmpty()) {
            tSink.setTableLocation(tableLocation);
        }

        if (paimonTable != null) {
            tSink.setSerializedTable(encodeObjectToString(paimonTable));
        }

        ArrayList<String> partitionKeys = new ArrayList<>();
        try {
            targetTable.getPartitionColumns(MvccUtil.getSnapshotFromContext(targetTable)).forEach(c -> {
                partitionKeys.add(c.getName());
            });
        } catch (Exception e) {
            LOG.warn("paimon: failed to get partition keys for table={}.{}: {}",
                    targetTable.getDbName(), targetTable.getName(), e.getMessage());
            throw new AnalysisException("Failed to get partition keys for paimon table", e);
        }
        tSink.setPartitionKeys(partitionKeys);

        ArrayList<String> bucketKeys = new ArrayList<>();
        int bucketNum = 0;
        try {
            if (paimonTable instanceof org.apache.paimon.table.FileStoreTable) {
                org.apache.paimon.schema.TableSchema schema =
                        ((org.apache.paimon.table.FileStoreTable) paimonTable).schema();
                bucketNum = schema.numBuckets();
                bucketKeys.addAll(schema.bucketKeys());
                if (bucketNum > 0 && bucketKeys.isEmpty()) {
                    bucketKeys.addAll(schema.fieldNames());
                }
            }
        } catch (Exception e) {
            LOG.error("paimon: failed to get bucket info for table={}.{}: {}",
                    targetTable.getDbName(), targetTable.getName(), e.getMessage());
            throw new AnalysisException("Failed to get bucket info for paimon table", e);
        }
        tSink.setBucketKeys(bucketKeys);
        if (bucketNum > 0) {
            tSink.setBucketNum(bucketNum);
            tSink.setShuffleMode(TPaimonWriteShuffleMode.PAIMON_SHUFFLE_BUCKET);
        } else if (!partitionKeys.isEmpty()) {
            tSink.setShuffleMode(TPaimonWriteShuffleMode.PAIMON_SHUFFLE_PARTITION);
        } else {
            tSink.setShuffleMode(TPaimonWriteShuffleMode.PAIMON_SHUFFLE_RANDOM);
        }

        boolean enableJni = false;
        if (ConnectContext.get() != null) {
            enableJni = ConnectContext.get().getSessionVariable().enablePaimonJniWriter;
        }
        options.put("paimon_use_jni", String.valueOf(enableJni));
        if (ConnectContext.get() != null) {
            boolean enableJniCompact = ConnectContext.get().getSessionVariable().enablePaimonJniCompact;
            options.put("paimon_use_jni_compact", String.valueOf(enableJniCompact));
        } else {
            options.put("paimon_use_jni_compact", "false");
        }
        tSink.setOptions(options);

        // Pass column names to BE because PipelineX may strip them from Block
        ArrayList<String> columnNames = new ArrayList<>();
        for (Column col : cols) {
            columnNames.add(col.getName());
        }
        tSink.setColumnNames(columnNames);

        tDataSink = new TDataSink(TDataSinkType.PAIMON_TABLE_SINK);
        tDataSink.setPaimonTableSink(tSink);
    }

    public static <T> String encodeObjectToString(T t) {
        try {
            byte[] bytes = InstantiationUtil.serializeObject(t);
            return new String(BASE64_ENCODER.encode(bytes), java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String resolveDefaultFsName(String warehouse) {
        if (warehouse == null || warehouse.isEmpty()) {
            return null;
        }
        try {
            URI uri = URI.create(warehouse);
            String scheme = uri.getScheme();
            String authority = uri.getAuthority();
            if (scheme == null || scheme.isEmpty() || authority == null || authority.isEmpty()) {
                return null;
            }
            return scheme + "://" + authority;
        } catch (Exception e) {
            LOG.warn("paimon: invalid warehouse uri {}, skip default fs resolve", warehouse);
            return null;
        }
    }

    private String normalizeTableLocation(String tableLocation) {
        if (tableLocation == null || tableLocation.isEmpty()) {
            return tableLocation;
        }
        try {
            URI uri = URI.create(tableLocation);
            String scheme = uri.getScheme();
            if ("hdfs".equalsIgnoreCase(scheme)) {
                String authority = uri.getAuthority();
                if (authority != null && !authority.isEmpty()) {
                    return tableLocation;
                }
                String path = uri.getPath();
                if (path != null && !path.isEmpty()) {
                    return path;
                }
            }
        } catch (Exception e) {
            if (tableLocation.startsWith("hdfs:/") && !tableLocation.startsWith("hdfs://")) {
                return "/" + tableLocation.substring("hdfs:/".length());
            }
        }
        return tableLocation;
    }
}
