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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.load.sync.DataSyncJobType;

import com.google.common.base.Strings;

import java.util.List;
import java.util.Map;

// create sync job statement, sync mysql data into tables.
//
// syntax:
//      CREATE SYNC doris_db.job_name
//          (channel_desc, ...)
//          binlog_desc
//      [PROPERTIES (key1=value1, )]
//
//      channel_desc:
//          FROM mysql_db.src_tbl INTO des_tbl
//          [PARTITION (p1, p2)]
//          [(col1, ...)]
//          [KEEP ORDER]
//
//      binlog_desc:
//          FROM BINLOG
//          (key1=value1, ...)
public class CreateDataSyncJobStmt extends DdlStmt implements NotFallbackInParser {
    private String jobName;
    private String dbName;
    private DataSyncJobType dataSyncJobType;
    private final List<ChannelDescription> channelDescriptions;
    private final BinlogDesc binlogDesc;
    private final Map<String, String> properties;

    public CreateDataSyncJobStmt(String jobName, String dbName, List<ChannelDescription> channelDescriptions,
                                 BinlogDesc binlogDesc, Map<String, String> properties) {
        this.jobName = jobName;
        this.dbName = dbName;
        this.channelDescriptions = channelDescriptions;
        this.binlogDesc = binlogDesc;
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(dbName)) {
            if (Strings.isNullOrEmpty(analyzer.getDefaultDb())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
            dbName = analyzer.getDefaultDb();
        }

        if (binlogDesc != null) {
            binlogDesc.analyze();
            dataSyncJobType = binlogDesc.getDataSyncJobType();
            if (dataSyncJobType != DataSyncJobType.CANAL) {
                throw new AnalysisException("Data sync job now only support CANAL type");
            }
        }

        if (channelDescriptions == null || channelDescriptions.isEmpty()) {
            throw new AnalysisException("No channel is assign in data sync job statement.");
        }

        for (ChannelDescription channelDescription : channelDescriptions) {
            channelDescription.analyze(dbName);
            String tableName = channelDescription.getTargetTable();
            Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbName);
            OlapTable olapTable = db.getOlapTableOrAnalysisException(tableName);
            if (olapTable.getKeysType() != KeysType.UNIQUE_KEYS) {
                throw new AnalysisException("Table: " + tableName
                        + " is not a unique table, key type: " + olapTable.getKeysType());
            }
            if (!olapTable.hasDeleteSign()) {
                throw new AnalysisException("Table: " + tableName
                        + " don't support batch delete. Please upgrade it to support, see `help alter table`.");
            }
        }
    }

    public String getJobName() {
        return jobName;
    }

    public String getDbName() {
        return dbName;
    }

    public List<ChannelDescription> getChannelDescriptions() {
        return channelDescriptions;
    }

    public BinlogDesc getBinlogDesc() {
        return binlogDesc;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public DataSyncJobType getDataSyncJobType() {
        return dataSyncJobType;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }
}
