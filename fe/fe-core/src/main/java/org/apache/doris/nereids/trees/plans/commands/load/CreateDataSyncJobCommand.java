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

package org.apache.doris.nereids.trees.plans.commands.load;

import org.apache.doris.analysis.BinlogDesc;
import org.apache.doris.analysis.ChannelDescription;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.load.sync.DataSyncJobType;
import org.apache.doris.load.sync.SyncJobManager;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;

import java.util.List;
import java.util.Map;
import java.util.Objects;

// create sync job command, sync mysql data into tables.
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

/**
 * CreateDataSyncJobCommand
 */
public class CreateDataSyncJobCommand extends Command {
    private String dbName;
    private String jobName;
    private DataSyncJobType dataSyncJobType;
    private final List<ChannelDescription> channelDescriptions;
    private final BinlogDesc binlogDesc;
    private final Map<String, String> properties;

    /**
     * CreateDataSyncJobCommand
     */
    public CreateDataSyncJobCommand(String dbName,
                                    String jobName,
                                    List<ChannelDescription> channelDescriptions,
                                    BinlogDesc binlogDesc,
                                    Map<String, String> properties) {
        super(PlanType.CREATE_DATA_SYNC_JOB_COMMAND);
        Objects.requireNonNull(jobName, "jobName is null");
        Objects.requireNonNull(channelDescriptions, "channelDescriptions is null");
        Objects.requireNonNull(binlogDesc, "binlogDesc is null");
        Objects.requireNonNull(properties, "properties is null");

        this.dbName = dbName;
        this.jobName = jobName;
        this.channelDescriptions = channelDescriptions;
        this.binlogDesc = binlogDesc;
        this.properties = properties;
    }

    public String getJobName() {
        return jobName;
    }

    public String getDbName() {
        return dbName;
    }

    public DataSyncJobType getDataSyncJobType() {
        return dataSyncJobType;
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

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        handleCreateDataSyncJobCommand(ctx);
    }

    private void validate(ConnectContext ctx) throws AnalysisException {
        if (Strings.isNullOrEmpty(dbName)) {
            if (Strings.isNullOrEmpty(ctx.getDatabase())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
            dbName = ctx.getDatabase();
        }

        binlogDesc.analyze();
        dataSyncJobType = binlogDesc.getDataSyncJobType();
        if (dataSyncJobType != DataSyncJobType.CANAL) {
            throw new AnalysisException("Data sync job now only support CANAL type");
        }

        if (channelDescriptions.isEmpty()) {
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

    private void handleCreateDataSyncJobCommand(ConnectContext ctx) throws DdlException {
        SyncJobManager syncJobMgr = ctx.getEnv().getSyncJobManager();
        if (!syncJobMgr.isJobNameExist(getDbName(), getJobName())) {
            syncJobMgr.addDataSyncJob(this);
        } else {
            throw new DdlException("The syncJob with jobName '" + getJobName() + "' in database ["
                + getDbName() + "] is already exists.");
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateDataSyncJobCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.SYNC;
    }
}
