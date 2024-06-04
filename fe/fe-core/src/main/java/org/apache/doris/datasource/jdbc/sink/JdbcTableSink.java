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

package org.apache.doris.datasource.jdbc.sink;

import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TJdbcTable;
import org.apache.doris.thrift.TJdbcTableSink;
import org.apache.doris.thrift.TOdbcTableType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class JdbcTableSink extends DataSink {
    private static final Logger LOG = LogManager.getLogger(JdbcTableSink.class);

    private final String externalTableName;
    private final String dorisTableName;
    private final TOdbcTableType jdbcType;
    private final boolean useTransaction;
    private String insertSql;

    private JdbcTable jdbcTable;

    public JdbcTableSink(JdbcTable jdbcTable, List<String> insertCols) {
        this.jdbcTable = jdbcTable;
        jdbcType = jdbcTable.getJdbcTableType();
        externalTableName = jdbcTable.getProperRemoteFullTableName(jdbcType);
        useTransaction = ConnectContext.get().getSessionVariable().isEnableOdbcTransaction();
        dorisTableName = jdbcTable.getName();
        insertSql = jdbcTable.getInsertSql(insertCols);
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix + "JDBC TABLE SINK:\n");
        strBuilder.append(prefix + "TABLENAME IN DORIS: ").append(dorisTableName).append("\n");
        strBuilder.append(prefix + "TABLE TYPE: ").append(jdbcType.toString()).append("\n");
        strBuilder.append(prefix + "TABLENAME OF EXTERNAL TABLE: ").append(externalTableName).append("\n");
        strBuilder.append(prefix + "EnableTransaction: ").append(useTransaction ? "true" : "false").append("\n");
        strBuilder.append(prefix + "PreparedStatement SQL: ").append(insertSql).append("\n");
        return strBuilder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink tDataSink = new TDataSink(TDataSinkType.JDBC_TABLE_SINK);
        TJdbcTableSink jdbcTableSink = new TJdbcTableSink();
        TJdbcTable jdbcTable = this.jdbcTable.toThrift().getJdbcTable();
        jdbcTableSink.setJdbcTable(jdbcTable);
        jdbcTableSink.setInsertSql(insertSql);
        jdbcTableSink.setUseTransaction(useTransaction);
        jdbcTableSink.setTableType(jdbcType);
        tDataSink.setJdbcTableSink(jdbcTableSink);
        return tDataSink;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return null;
    }
}
