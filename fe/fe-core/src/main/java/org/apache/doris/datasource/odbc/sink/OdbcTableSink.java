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

package org.apache.doris.datasource.odbc.sink;

import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TOdbcTableSink;
import org.apache.doris.thrift.TOdbcTableType;

public class OdbcTableSink extends DataSink {
    private final TOdbcTableType odbcType;
    private final String tblName;
    private final String originTblName;
    private final String connectString;
    private final boolean useTransaction;

    public OdbcTableSink(OdbcTable odbcTable) {
        connectString = odbcTable.getConnectString();
        originTblName = odbcTable.getName();
        odbcType = odbcTable.getOdbcTableType();
        tblName = JdbcTable.databaseProperName(odbcType, odbcTable.getOdbcTableName());
        useTransaction = ConnectContext.get().getSessionVariable().isEnableOdbcTransaction();
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix + "ODBC TABLE SINK:\n");
        strBuilder.append(prefix + "TABLENAME IN DORIS: ").append(originTblName).append("\n");
        strBuilder.append(prefix + "TABLE TYPE: ").append(odbcType.toString()).append("\n");
        strBuilder.append(prefix + "TABLENAME OF EXTERNAL TABLE: ").append(tblName).append("\n");
        strBuilder.append(prefix + "EnableTransaction: ").append(useTransaction ? "true" : "false").append("\n");
        return strBuilder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink tDataSink = new TDataSink(TDataSinkType.ODBC_TABLE_SINK);
        TOdbcTableSink odbcTableSink = new TOdbcTableSink();
        odbcTableSink.setConnectString(connectString);
        odbcTableSink.setTable(tblName);
        odbcTableSink.setUseTransaction(useTransaction);
        tDataSink.setOdbcTableSink(odbcTableSink);
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
