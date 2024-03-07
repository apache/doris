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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;

public class ShowDynamicPartitionStmt extends ShowStmt {
    private String db;
    private static final ShowResultSetMetaData SHOW_DYNAMIC_PARTITION_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("TableName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Enable", ScalarType.createVarchar(20)))
                    .addColumn(new Column("TimeUnit", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Start", ScalarType.createVarchar(20)))
                    .addColumn(new Column("End", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Prefix", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Buckets", ScalarType.createVarchar(20)))
                    .addColumn(new Column("ReplicationNum", ScalarType.createVarchar(20)))
                    .addColumn(new Column("ReplicaAllocation", ScalarType.createVarchar(128)))
                    .addColumn(new Column("StartOf", ScalarType.createVarchar(20)))
                    .addColumn(new Column("LastUpdateTime", ScalarType.createVarchar(20)))
                    .addColumn(new Column("LastSchedulerTime", ScalarType.createVarchar(20)))
                    .addColumn(new Column("State", ScalarType.createVarchar(20)))
                    .addColumn(new Column("LastCreatePartitionMsg", ScalarType.createVarchar(20)))
                    .addColumn(new Column("LastDropPartitionMsg", ScalarType.createVarchar(20)))
                    .addColumn(new Column("ReservedHistoryPeriods", ScalarType.createVarchar(20)))
                    .build();

    ShowDynamicPartitionStmt(String db) {
        this.db = db;
    }

    public String getDb() {
        return db;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(db)) {
            db = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(db)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }

        // we do not check db privs here. because user may not have any db privs,
        // but if it has privs of tbls inside this db,it should be allowed to see this db.
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW DYNAMIC PARTITION TABLES");
        if (!Strings.isNullOrEmpty(db)) {
            sb.append(" FROM ").append(db);
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return SHOW_DYNAMIC_PARTITION_META_DATA;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
