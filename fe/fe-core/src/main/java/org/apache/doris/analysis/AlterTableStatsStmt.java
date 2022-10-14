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
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.StatsType;
import org.apache.doris.statistics.TableStats;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Manually inject statistics for tables or partitions.
 * Only OLAP table statistics are supported.
 *
 * syntax:
 *   ALTER TABLE table_name
 *   SET STATS ('k1' = 'v1', ...) [ PARTITIONS(p_name1, p_name2...) ]
 */
public class AlterTableStatsStmt extends DdlStmt {

    private static final ImmutableSet<StatsType> CONFIGURABLE_PROPERTIES_SET =
            new ImmutableSet.Builder<StatsType>()
            .add(TableStats.DATA_SIZE)
            .add(TableStats.ROW_COUNT)
            .build();

    private final TableName tableName;
    private final PartitionNames optPartitionNames;
    private final Map<String, String> properties;

    private final List<String> partitionNames = Lists.newArrayList();
    private final Map<StatsType, String> statsTypeToValue = Maps.newHashMap();

    public AlterTableStatsStmt(TableName tableName, Map<String, String> properties,
            PartitionNames optPartitionNames) {
        this.tableName = tableName;
        this.properties = properties == null ? Maps.newHashMap() : properties;
        this.optPartitionNames = optPartitionNames;
    }

    public TableName getTableName() {
        return tableName;
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

        // check partition
        checkPartitionNames();

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
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "ALTER TABLE STATS",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    tableName.getDb() + ": " + tableName.getTbl());
        }

        // get statsTypeToValue
        properties.forEach((key, value) -> {
            StatsType statsType = StatsType.fromString(key);
            statsTypeToValue.put(statsType, value);
        });
    }

    private void checkPartitionNames() throws AnalysisException {
        Database db = analyzer.getEnv().getInternalCatalog().getDbOrAnalysisException(tableName.getDb());
        Table table = db.getTableOrAnalysisException(tableName.getTbl());

        if (table.getType() != Table.TableType.OLAP) {
            throw new AnalysisException("Only OLAP table statistics are supported");
        }

        if (optPartitionNames != null) {
            OlapTable olapTable = (OlapTable) table;

            if (!olapTable.isPartitioned()) {
                throw new AnalysisException("Not a partitioned table: " + olapTable.getName());
            }

            optPartitionNames.analyze(analyzer);
            List<String> names = optPartitionNames.getPartitionNames();
            Set<String> olapPartitionNames = olapTable.getPartitionNames();
            Optional<String> optional = names.stream()
                    .filter(name -> !olapPartitionNames.contains(name))
                    .findFirst();
            if (optional.isPresent()) {
                throw new AnalysisException("Partition does not exist: " + optional.get());
            }
            partitionNames.addAll(names);
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ");
        sb.append(tableName.toSql());
        sb.append(" SET STATS ");
        sb.append("(");
        sb.append(new PrintableMap<>(properties,
                " = ", true, false));
        sb.append(") ");
        if (optPartitionNames != null) {
            sb.append(optPartitionNames.toSql());
        }
        return sb.toString();
    }
}
