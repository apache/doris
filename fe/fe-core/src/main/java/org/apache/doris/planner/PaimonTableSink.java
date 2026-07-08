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
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.nereids.trees.plans.commands.insert.BaseExternalTableInsertCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TPaimonTableSink;
import org.apache.doris.thrift.TPaimonWriteBackendType;
import org.apache.doris.thrift.TPaimonWriteMode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.utils.InstantiationUtil;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Paimon table sink.
 *
 * Generates TPaimonTableSink payload consumed by BE, including table location,
 * Paimon options, Hadoop config, bucket metadata, and sink column names.
 *
 * v1: always sets backend_type = JNI.
 */
public class PaimonTableSink extends BaseExternalTableDataSink {
    private static final Logger LOG = LogManager.getLogger(PaimonTableSink.class);
    private static final String OUTPUT_COLUMN_NAMES_KEY = "doris.output_column_names";
    private static final String COLUMN_NAME_SEPARATOR = "";
    private static final Base64.Encoder BASE64_ENCODER =
            java.util.Base64.getUrlEncoder().withoutPadding();

    private final PaimonExternalTable targetTable;
    private List<Expr> outputExprs;
    private List<Column> cols;

    private static final HashSet<TFileFormatType> supportedTypes = new HashSet<TFileFormatType>() {{
            add(TFileFormatType.FORMAT_ORC);
            add(TFileFormatType.FORMAT_PARQUET);
        }};

    public PaimonTableSink(PaimonExternalTable targetTable) {
        super();
        this.targetTable = targetTable;
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

        tSink.setDbName(targetTable.getDbName());
        tSink.setTbName(targetTable.getName());

        Map<String, String> hadoopConfig = new HashMap<>(
                targetTable.getCatalog().getCatalogProperty().getHadoopProperties());
        Map<String, String> paimonOptions = new HashMap<>();

        String warehouse = ((PaimonExternalCatalog) targetTable.getCatalog())
                .getPaimonOptionsMap().get(CatalogOptions.WAREHOUSE.key());
        String defaultFs = resolveDefaultFsName(warehouse);
        if (defaultFs != null && !defaultFs.isEmpty()) {
            String current = hadoopConfig.get("fs.defaultFS");
            if (current == null || current.isEmpty() || current.startsWith("file:/")) {
                hadoopConfig.put("fs.defaultFS", defaultFs);
            }
        }

        // Transaction context: commit identifier and user
        if (insertCtx.isPresent() && insertCtx.get() instanceof BaseExternalTableInsertCommandContext) {
            BaseExternalTableInsertCommandContext ctx =
                    (BaseExternalTableInsertCommandContext) insertCtx.get();
            if (ctx.getTxnId() > 0) {
                paimonOptions.put("doris.commit_identifier", String.valueOf(ctx.getTxnId()));
            }
            if (ctx.getCommitUser() != null && !ctx.getCommitUser().isEmpty()) {
                paimonOptions.put("doris.commit_user", ctx.getCommitUser());
            }
        }

        // Column names (BE-side column name preservation)
        paimonOptions.put(OUTPUT_COLUMN_NAMES_KEY, String.join(COLUMN_NAME_SEPARATOR, outputColumnNames()));

        // Hadoop user
        String hadoopUser = hadoopConfig.get("hadoop.username");
        if (hadoopUser == null || hadoopUser.isEmpty()) {
            hadoopUser = hadoopConfig.get("hadoop.user.name");
        }
        if (hadoopUser == null || hadoopUser.isEmpty()) {
            hadoopUser = "hadoop";
        }
        hadoopConfig.put("hadoop.user.name", hadoopUser);

        // Table location
        String tableLocation = null;
        org.apache.paimon.table.Table paimonTable =
                targetTable.getPaimonTable(MvccUtil.getSnapshotFromContext(targetTable));
        if (paimonTable instanceof org.apache.paimon.table.FileStoreTable) {
            tableLocation = ((org.apache.paimon.table.FileStoreTable) paimonTable).location().toString();
        }
        if (tableLocation != null && !tableLocation.isEmpty()) {
            tSink.setTableLocation(tableLocation);
        }

        // Serialize table for BE-side fast loading
        if (paimonTable != null) {
            tSink.setSerializedTable(encodeObjectToString(paimonTable));
        }

        // Bucket info
        if (paimonTable instanceof org.apache.paimon.table.FileStoreTable) {
            int bucketNum = ((org.apache.paimon.table.FileStoreTable) paimonTable).schema().numBuckets();
            if (bucketNum > 0) {
                tSink.setBucketNum(bucketNum);
            }
        }

        // v1: always JNI backend, APPEND mode
        tSink.setBackendType(TPaimonWriteBackendType.JNI);
        tSink.setWriteMode(TPaimonWriteMode.APPEND);

        tSink.setPaimonOptions(paimonOptions);
        tSink.setHadoopConfig(hadoopConfig);

        List<String> columnNames = new ArrayList<>();
        for (Column col : cols) {
            columnNames.add(col.getName());
        }
        tSink.setColumnNames(columnNames);

        tDataSink = new TDataSink(TDataSinkType.PAIMON_TABLE_SINK);
        tDataSink.setPaimonTableSink(tSink);
    }

    private List<String> outputColumnNames() throws AnalysisException {
        List<Column> fullSchema = targetTable.getFullSchema();
        if (fullSchema.size() != outputExprs.size()) {
            throw new AnalysisException("Paimon sink output column size mismatch, schema="
                    + fullSchema.size() + ", exprs=" + outputExprs.size());
        }
        List<String> names = new ArrayList<>(fullSchema.size());
        for (Column col : fullSchema) {
            names.add(col.getName());
        }
        return names;
    }

    private static String encodeObjectToString(Object obj) {
        try {
            byte[] bytes = InstantiationUtil.serializeObject(obj);
            return new String(BASE64_ENCODER.encode(bytes),
                    java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize Paimon table", e);
        }
    }

    private static String resolveDefaultFsName(String warehouse) {
        if (warehouse == null || warehouse.isEmpty()) {
            return null;
        }
        try {
            java.net.URI uri = java.net.URI.create(warehouse);
            String scheme = uri.getScheme();
            String authority = uri.getAuthority();
            if (scheme != null && !scheme.isEmpty() && authority != null && !authority.isEmpty()) {
                return scheme + "://" + authority;
            }
        } catch (Exception e) {
            LOG.warn("paimon: invalid warehouse uri {}", warehouse);
        }
        return null;
    }
}
