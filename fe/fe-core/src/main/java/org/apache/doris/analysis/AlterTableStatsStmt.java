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
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.StatsType;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Manually inject statistics for table.
 * <p>
 * Syntax:
 * ALTER TABLE table_name SET STATS ('k1' = 'v1', ...);
 * <p>
 * e.g.
 * ALTER TABLE stats_test.example_tbl SET STATS ('row_count'='6001215');
 */
public class AlterTableStatsStmt extends DdlStmt {

    private static final ImmutableSet<StatsType> CONFIGURABLE_PROPERTIES_SET =
            new ImmutableSet.Builder<StatsType>()
                    .add(StatsType.ROW_COUNT)
                    .build();

    private final TableName tableName;
    private final Map<String, String> properties;
    private final Map<StatsType, String> statsTypeToValue = Maps.newHashMap();

    // after analyzed
    private long tableId;

    public AlterTableStatsStmt(TableName tableName, Map<String, String> properties) {
        this.tableName = tableName;
        this.properties = properties == null ? Collections.emptyMap() : properties;
    }

    public TableName getTableName() {
        return tableName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (!ConnectContext.get().getSessionVariable().enableStats) {
            throw new UserException("Analyze function is forbidden, you should add `enable_stats=true`"
                    + "in your FE conf file");
        }

        super.analyze(analyzer);
        tableName.analyze(analyzer);

        Optional<StatsType> optional = properties.keySet().stream().map(StatsType::fromString)
                .filter(statsType -> !CONFIGURABLE_PROPERTIES_SET.contains(statsType))
                .findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid statistics");
        }

        properties.forEach((key, value) -> {
            StatsType statsType = StatsType.fromString(key);
            statsTypeToValue.put(statsType, value);
        });

        Database db = analyzer.getEnv().getInternalCatalog().getDbOrAnalysisException(tableName.getDb());
        Table table = db.getTableOrAnalysisException(tableName.getTbl());
        tableId = table.getId();
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

    public long getTableId() {
        return tableId;
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
        sb.append(")");

        return sb.toString();
    }

    public String getValue(StatsType statsType) {
        return statsTypeToValue.get(statsType);
    }
}
