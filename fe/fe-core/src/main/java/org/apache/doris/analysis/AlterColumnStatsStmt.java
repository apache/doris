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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStat;
import org.apache.doris.statistics.StatsType;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Manually inject statistics for columns.
 * For partitioned tables, partitions must be specified, or statistics cannot be updated,
 * and only OLAP table statistics are supported.
 * e.g.
 *   ALTER TABLE table_name MODIFY COLUMN columnName
 *   SET STATS ('k1' = 'v1', ...) [ PARTITIONS(p_name1, p_name2...) ]
 */
public class AlterColumnStatsStmt extends DdlStmt {

    private static final ImmutableSet<StatsType> CONFIGURABLE_PROPERTIES_SET = new ImmutableSet.Builder<StatsType>()
            .add(StatsType.ROW_COUNT)
            .add(ColumnStat.NDV)
            .add(ColumnStat.AVG_SIZE)
            .add(ColumnStat.MAX_SIZE)
            .add(ColumnStat.NUM_NULLS)
            .add(ColumnStat.MIN_VALUE)
            .add(ColumnStat.MAX_VALUE)
            .add(StatsType.DATA_SIZE)
            .build();

    private final TableName tableName;
    private final PartitionNames optPartitionNames;
    private final String columnName;
    private final Map<String, String> properties;

    private final List<String> partitionNames = Lists.newArrayList();
    private final Map<StatsType, String> statsTypeToValue = Maps.newHashMap();

    public AlterColumnStatsStmt(TableName tableName, String columnName,
            Map<String, String> properties, PartitionNames optPartitionNames) {
        this.tableName = tableName;
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

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public Map<StatsType, String> getStatsTypeToValue() {
        return statsTypeToValue;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // check table name
        tableName.analyze(analyzer);

        // disallow external catalog
        Util.prohibitExternalCatalog(tableName.getCtl(), this.getClass().getSimpleName());

        // check partition & column
        checkPartitionAndColumnNames();

        // check properties
        Optional<StatsType> optional = properties.keySet().stream().map(StatsType::fromString)
                .filter(statsType -> !CONFIGURABLE_PROPERTIES_SET.contains(statsType))
                .findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid statistics");
        }

        // check auth
        if (!Env.getCurrentEnv().getAuth()
                .checkTblPriv(ConnectContext.get(), tableName.getDb(), tableName.getTbl(), PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "ALTER COLUMN STATS",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    tableName.getDb() + ": " + tableName.getTbl());
        }

        // get statsTypeToValue
        properties.forEach((key, value) -> {
            StatsType statsType = StatsType.fromString(key);
            statsTypeToValue.put(statsType, value);
        });
    }

    /**
     * TODO(wzt): Support for external tables
     */
    private void checkPartitionAndColumnNames() throws AnalysisException {
        Database db = analyzer.getEnv().getInternalCatalog().getDbOrAnalysisException(tableName.getDb());
        Table table = db.getTableOrAnalysisException(tableName.getTbl());

        if (table.getType() != Table.TableType.OLAP) {
            throw new AnalysisException("Only OLAP table statistics are supported");
        }

        OlapTable olapTable = (OlapTable) table;
        if (olapTable.getColumn(columnName) == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_COLUMN_NAME,
                    columnName, FeNameFormat.getColumnNameRegex());
        }

        if (optPartitionNames != null) {
            if (olapTable.getPartitionInfo().getType().equals(PartitionType.UNPARTITIONED)) {
                throw new AnalysisException("Not a partitioned table: " + olapTable.getName());
            }

            optPartitionNames.analyze(analyzer);
            Set<String> olapPartitionNames = olapTable.getPartitionNames();
            Optional<String> optional = optPartitionNames.getPartitionNames().stream()
                    .filter(name -> !olapPartitionNames.contains(name))
                    .findFirst();
            if (optional.isPresent()) {
                throw new AnalysisException("Partition does not exist: " + optional.get());
            }
            partitionNames.addAll(optPartitionNames.getPartitionNames());
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ");
        sb.append(tableName.toSql());
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
