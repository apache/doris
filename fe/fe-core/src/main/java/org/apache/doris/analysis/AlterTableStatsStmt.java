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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.StatsType;
import org.apache.doris.statistics.TableStats;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Optional;

public class AlterTableStatsStmt extends DdlStmt {

    private static final ImmutableSet<StatsType> CONFIGURABLE_PROPERTIES_SET = new ImmutableSet.Builder<StatsType>()
            .add(TableStats.DATA_SIZE)
            .add(TableStats.ROW_COUNT)
            .build();

    private TableName tableName;
    private Map<String, String> properties;
    public final Map<StatsType, String> statsTypeToValue = Maps.newHashMap();

    public AlterTableStatsStmt(TableName tableName, Map<String, String> properties) {
        this.tableName = tableName;
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        // check table name
        tableName.analyze(analyzer);
        // check properties
        Optional<StatsType> optional = properties.keySet().stream().map(StatsType::fromString)
            .filter(statsType -> !CONFIGURABLE_PROPERTIES_SET.contains(statsType)).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid statistic");
        }
        // check auth
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), tableName.getDb(), tableName.getTbl(),
                PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "ALTER TABLE STATS",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    tableName.getDb() + ": " + tableName.getTbl());
        }
        // get statsTypeToValue
        properties.forEach((key, value) -> {
            StatsType statsType = StatsType.fromString(key);
            statsTypeToValue.put(statsType, value);
        });
    }

    public TableName getTableName() {
        return tableName;
    }

    public Map<StatsType, String> getStatsTypeToValue() {
        return statsTypeToValue;
    }
}
