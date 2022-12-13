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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Collect statistics.
 *
 * syntax:
 * ANALYZE [[ db_name.tb_name ] [( column_name [, ...] )], ...] [ PROPERTIES(...) ]
 *     db_name.tb_name: collect table and column statistics from tb_name
 *     column_name: collect column statistics from column_name
 *     properties: properties of statistics jobs
 */
public class AnalyzeStmt extends DdlStmt {
    // time to wait for collect  statistics
    public static final String CBO_STATISTICS_TASK_TIMEOUT_SEC = "cbo_statistics_task_timeout_sec";

    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(CBO_STATISTICS_TASK_TIMEOUT_SEC)
            .build();

    private static final Predicate<Long> DESIRED_TASK_TIMEOUT_SEC = (v) -> v > 0L;

    public final boolean wholeTbl;

    private final TableName tableName;

    private TableIf table;

    private final PartitionNames optPartitionNames;
    private List<String> optColumnNames;
    private Map<String, String> optProperties;

    // after analyzed
    private long dbId;

    private final List<String> partitionNames = Lists.newArrayList();

    public AnalyzeStmt(TableName tableName,
            List<String> optColumnNames,
            PartitionNames optPartitionNames,
            Map<String, String> optProperties) {
        this.tableName = tableName;
        this.optColumnNames = optColumnNames;
        this.optPartitionNames = optPartitionNames;
        wholeTbl = CollectionUtils.isEmpty(optColumnNames);
        this.optProperties = optProperties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        tableName.analyze(analyzer);

        String catalogName = tableName.getCtl();
        String dbName = tableName.getDb();
        String tblName = tableName.getTbl();
        CatalogIf catalog = analyzer.getEnv().getCatalogMgr().getCatalog(catalogName);
        DatabaseIf db = catalog.getDbOrAnalysisException(dbName);
        table = db.getTableOrAnalysisException(tblName);

        checkAnalyzePriv(dbName, tblName);

        if (optColumnNames != null && !optColumnNames.isEmpty()) {
            table.readLock();
            try {
                List<String> baseSchema = table.getBaseSchema(false)
                        .stream().map(Column::getName).collect(Collectors.toList());
                Optional<String> optional = optColumnNames.stream()
                        .filter(entity -> !baseSchema.contains(entity)).findFirst();
                if (optional.isPresent()) {
                    String columnName = optional.get();
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_COLUMN_NAME,
                            columnName, FeNameFormat.getColumnNameRegex());
                }
            } finally {
                table.readUnlock();
            }
        } else {
            optColumnNames = table.getBaseSchema(false)
                    .stream().map(Column::getName).collect(Collectors.toList());
        }
        dbId = db.getId();
        // step2: analyze partition
        checkPartitionNames();
        // step3: analyze properties
        checkProperties();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    private void checkAnalyzePriv(String dbName, String tblName) throws AnalysisException {
        PaloAuth auth = Env.getCurrentEnv().getAuth();
        if (!auth.checkTblPriv(ConnectContext.get(), dbName, tblName, PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                    "ANALYZE",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    dbName + ": " + tblName);
        }
    }

    private void checkPartitionNames() throws AnalysisException {
        if (optPartitionNames != null) {
            optPartitionNames.analyze(analyzer);
            if (tableName != null) {
                Database db = analyzer.getEnv().getInternalCatalog().getDbOrAnalysisException(tableName.getDb());
                OlapTable olapTable = (OlapTable) db.getTableOrAnalysisException(tableName.getTbl());
                if (!olapTable.isPartitioned()) {
                    throw new AnalysisException("Not a partitioned table: " + olapTable.getName());
                }
                List<String> names = optPartitionNames.getPartitionNames();
                Set<String> olapPartitionNames = olapTable.getPartitionNames();
                List<String> tempPartitionNames = olapTable.getTempPartitions().stream()
                        .map(Partition::getName).collect(Collectors.toList());
                Optional<String> optional = names.stream()
                        .filter(name -> (tempPartitionNames.contains(name)
                                || !olapPartitionNames.contains(name)))
                        .findFirst();
                if (optional.isPresent()) {
                    throw new AnalysisException("Temporary partition or partition does not exist");
                }
            } else {
                throw new AnalysisException("Specify partition should specify table name as well");
            }
            partitionNames.addAll(optPartitionNames.getPartitionNames());
        }
    }

    private void checkProperties() throws UserException {
        if (optProperties == null) {
            optProperties = Maps.newHashMap();
        } else {
            Optional<String> optional = optProperties.keySet().stream().filter(
                    entity -> !PROPERTIES_SET.contains(entity)).findFirst();
            if (optional.isPresent()) {
                throw new AnalysisException(optional.get() + " is invalid property");
            }
        }
        long taskTimeout = ((Long) Util.getLongPropertyOrDefault(optProperties.get(CBO_STATISTICS_TASK_TIMEOUT_SEC),
                Config.max_cbo_statistics_task_timeout_sec, DESIRED_TASK_TIMEOUT_SEC,
                CBO_STATISTICS_TASK_TIMEOUT_SEC + " should > 0")).intValue();
        optProperties.put(CBO_STATISTICS_TASK_TIMEOUT_SEC, String.valueOf(taskTimeout));
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ANALYZE");

        if (tableName != null) {
            sb.append(" ");
            sb.append(tableName.toSql());
        }

        if (optColumnNames != null) {
            sb.append("(");
            sb.append(StringUtils.join(optColumnNames, ","));
            sb.append(")");
        }

        if (optPartitionNames != null) {
            sb.append(" ");
            sb.append(optPartitionNames.toSql());
        }

        if (optProperties != null) {
            sb.append(" ");
            sb.append("PROPERTIES(");
            sb.append(new PrintableMap<>(optProperties, " = ",
                    true,
                    false));
            sb.append(")");
        }

        return sb.toString();
    }

    public String getCatalogName() {
        return tableName.getCtl();
    }

    public String getDBName() {
        return tableName.getDb();
    }

    public TableName getTblName() {
        return tableName;
    }

    public List<String> getOptColumnNames() {
        return optColumnNames;
    }


    public long getDbId() {
        Preconditions.checkArgument(isAnalyzed(),
                "The dbId must be obtained after the parsing is complete");
        return dbId;
    }

    public Database getDb() throws AnalysisException {
        Preconditions.checkArgument(isAnalyzed(),
                "The db must be obtained after the parsing is complete");
        return analyzer.getEnv().getInternalCatalog().getDbOrAnalysisException(dbId);
    }

    public TableIf getTable() {
        return table;
    }

    public List<String> getPartitionNames() {
        Preconditions.checkArgument(isAnalyzed(),
                "The partitionNames must be obtained after the parsing is complete");
        return partitionNames;
    }

    public Map<String, String> getProperties() {
        return optProperties;
    }

}
