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
import org.apache.doris.statistics.ColumnStats;

import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Optional;

public class AlterColumnStatsStmt extends DdlStmt {

    private static final ImmutableSet<String> CONFIGURABLE_PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(ColumnStats.NDV)
            .add(ColumnStats.AVG_SIZE)
            .add(ColumnStats.MAX_SIZE)
            .add(ColumnStats.NUM_NULLS)
            .add(ColumnStats.MIN_VALUE)
            .add(ColumnStats.MAX_VALUE)
            .build();

    private TableName tableName;
    private String columnName;
    private Map<String, String> properties;

    public AlterColumnStatsStmt(TableName tableName, String columnName, Map<String, String> properties) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        // check table name
        tableName.analyze(analyzer);
        // check properties
        Optional<String> optional = properties.keySet().stream().filter(
                entity -> !CONFIGURABLE_PROPERTIES_SET.contains(entity.toLowerCase())).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid statistic");
        }
        // check auth
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), tableName.getDb(), tableName.getTbl(),
                PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "ALTER COLUMN STATS",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    tableName.getDb() + ": " + tableName.getTbl());
        }
    }

    public TableName getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
