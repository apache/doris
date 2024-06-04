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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.StatsType;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Manually inject statistics for columns.
 * Only OLAP table statistics are supported.
 *
 * Syntax:
 *   ALTER TABLE table_name MODIFY COLUMN columnName
 *   SET STATS ('k1' = 'v1', ...);
 *
 * e.g.
 *   ALTER TABLE stats_test.example_tbl MODIFY COLUMN age
 *   SET STATS ('row_count'='6001215');
 *
 * Note: partition stats injection is mainly convenient for test cost estimation,
 * and can be removed after the related functions are completed.
 */
public class AlterColumnStatsStmt extends DdlStmt {

    private static final ImmutableSet<StatsType> CONFIGURABLE_PROPERTIES_SET = new ImmutableSet.Builder<StatsType>()
            .add(StatsType.ROW_COUNT)
            .add(ColumnStatistic.NDV)
            .add(ColumnStatistic.AVG_SIZE)
            .add(ColumnStatistic.MAX_SIZE)
            .add(ColumnStatistic.NUM_NULLS)
            .add(ColumnStatistic.MIN_VALUE)
            .add(ColumnStatistic.MAX_VALUE)
            .add(StatsType.DATA_SIZE)
            .build();

    private final TableName tableName;
    private final String indexName;
    private final String columnName;
    private final Map<String, String> properties;
    private final PartitionNames optPartitionNames;

    private final List<Long> partitionIds = Lists.newArrayList();
    private final Map<StatsType, String> statsTypeToValue = Maps.newHashMap();

    private long indexId = -1;

    public AlterColumnStatsStmt(TableName tableName, String indexName, String columnName,
            Map<String, String> properties, PartitionNames optPartitionNames) {
        this.tableName = tableName;
        this.indexName = indexName;
        this.columnName = columnName;
        this.properties = properties == null ? Collections.emptyMap() : properties;
        this.optPartitionNames = optPartitionNames;
    }

    public TableName getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public long getIndexId() {
        return indexId;
    }

    public List<Long> getPartitionIds() {
        return partitionIds;
    }

    public Map<StatsType, String> getStatsTypeToValue() {
        return statsTypeToValue;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (!ConnectContext.get().getSessionVariable().enableStats) {
            throw new UserException("Analyze function is forbidden, you should add `enable_stats=true`"
                    + "in your FE conf file");
        }
        super.analyze(analyzer);

        // check table name
        tableName.analyze(analyzer);

        // check partition & column
        checkPartitionAndColumn();

        // check properties
        Optional<StatsType> optional = properties.keySet().stream().map(StatsType::fromString)
                .filter(statsType -> !CONFIGURABLE_PROPERTIES_SET.contains(statsType))
                .findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid statistics");
        }

        // get statsTypeToValue
        properties.forEach((key, value) -> {
            StatsType statsType = StatsType.fromString(key);
            statsTypeToValue.put(statsType, value);
        });
    }

    @Override
    public void checkPriv() throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tableName.getCtl(), tableName.getDb(),
                        tableName.getTbl(), PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "ALTER COLUMN STATS",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    tableName.getDb() + ": " + tableName.getTbl());
        }
    }

    private void checkPartitionAndColumn() throws AnalysisException {
        CatalogIf catalog = analyzer.getEnv().getCatalogMgr().getCatalog(tableName.getCtl());
        DatabaseIf db = catalog.getDbOrAnalysisException(tableName.getDb());
        TableIf table = db.getTableOrAnalysisException(tableName.getTbl());

        if (indexName != null) {
            if (!(table instanceof OlapTable)) {
                throw new AnalysisException("Only OlapTable support alter index stats. "
                    + "Table " + table.getName() + " is not OlapTable.");
            }
            OlapTable olapTable = (OlapTable) table;
            Long idxId = olapTable.getIndexIdByName(indexName);
            if (idxId == null) {
                throw new AnalysisException("Index " + indexName + " not exist in table " + table.getName());
            }
            indexId = idxId;
        }

        if (table.getColumn(columnName) == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_COLUMN_NAME,
                    columnName, FeNameFormat.getColumnNameRegex());
        }

        if (optPartitionNames != null && table instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) table;
            if (olapTable.getPartitionInfo().getType().equals(PartitionType.UNPARTITIONED)) {
                throw new AnalysisException("Not a partitioned table: " + olapTable.getName());
            }

            optPartitionNames.analyze(analyzer);
            List<String> partitionNames = optPartitionNames.getPartitionNames();
            for (String partitionName : partitionNames) {
                Partition partition = olapTable.getPartition(partitionName);
                if (partition == null) {
                    throw new AnalysisException("Partition does not exist: " + partitionName);
                }
                partitionIds.add(partition.getId());
            }
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ");
        sb.append(tableName.toSql());
        if (indexName != null) {
            sb.append(" INDEX ");
            sb.append(indexName);
        }
        sb.append(" MODIFY COLUMN ");
        sb.append(columnName);
        sb.append(" SET STATS ");
        sb.append("(");
        sb.append(new PrintableMap<>(properties,
                " = ", true, false));
        sb.append(")");
        if (optPartitionNames != null) {
            sb.append(" ");
            sb.append(optPartitionNames.toSql());
        }
        return sb.toString();
    }

    public String getValue(StatsType statsType) {
        return statsTypeToValue.get(statsType);
    }
}
