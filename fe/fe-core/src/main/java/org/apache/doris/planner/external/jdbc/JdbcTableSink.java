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

package org.apache.doris.planner.external.jdbc;

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

    private final String resourceName;
    private final String externalTableName;
    private final String dorisTableName;
    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPasswd;
    private final String driverClass;
    private final String driverUrl;
    private final String checkSum;
    private final TOdbcTableType jdbcType;
    private final boolean useTransaction;
    private String insertSql;

    public JdbcTableSink(JdbcTable jdbcTable, List<String> insertCols) {
        resourceName = jdbcTable.getResourceName();
        jdbcType = jdbcTable.getJdbcTableType();
        externalTableName = jdbcTable.getProperRealFullTableName(jdbcType);
        useTransaction = ConnectContext.get().getSessionVariable().isEnableOdbcTransaction();
        jdbcUrl = jdbcTable.getJdbcUrl();
        jdbcUser = jdbcTable.getJdbcUser();
        jdbcPasswd = jdbcTable.getJdbcPasswd();
        driverClass = jdbcTable.getDriverClass();
        driverUrl = jdbcTable.getDriverUrl();
        checkSum = jdbcTable.getCheckSum();
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
        return strBuilder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink tDataSink = new TDataSink(TDataSinkType.JDBC_TABLE_SINK);
        TJdbcTableSink jdbcTableSink = new TJdbcTableSink();
        TJdbcTable jdbcTable = new TJdbcTable();
        jdbcTableSink.setJdbcTable(jdbcTable);
        jdbcTableSink.jdbc_table.setJdbcUrl(jdbcUrl);
        jdbcTableSink.jdbc_table.setJdbcUser(jdbcUser);
        jdbcTableSink.jdbc_table.setJdbcPassword(jdbcPasswd);
        jdbcTableSink.jdbc_table.setJdbcTableName(externalTableName);
        jdbcTableSink.jdbc_table.setJdbcDriverUrl(driverUrl);
        jdbcTableSink.jdbc_table.setJdbcDriverClass(driverClass);
        jdbcTableSink.jdbc_table.setJdbcDriverChecksum(checkSum);
        jdbcTableSink.jdbc_table.setJdbcResourceName(resourceName);
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
