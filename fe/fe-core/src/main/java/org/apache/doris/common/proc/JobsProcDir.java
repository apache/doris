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

package org.apache.doris.common.proc;

import org.apache.doris.alter.MaterializedViewHandler;
import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.load.ExportJobState;
import org.apache.doris.load.ExportMgr;
import org.apache.doris.load.loadv2.LoadManager;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/*
 * SHOW PROC '/jobs/dbId/'
 * show job type
 */
public class JobsProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobType").add("Pending").add("Running").add("Finished")
            .add("Cancelled").add("Total")
            .build();

    private static final String LOAD = "load";
    private static final String DELETE = "delete";
    private static final String ROLLUP = "rollup";
    private static final String SCHEMA_CHANGE = "schema_change";
    private static final String EXPORT = "export";
    private static final String BUILD_INDEX = "build_index";

    private Env env;
    private Database db;

    public JobsProcDir(Env env, Database db) {
        this.env = env;
        this.db = db;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String jobTypeName) throws AnalysisException {
        if (Strings.isNullOrEmpty(jobTypeName) || env == null) {
            throw new AnalysisException("Job type name is null");
        }

        if (jobTypeName.equals(LOAD)) {
            return new LoadProcDir(env.getCurrentEnv().getLoadManager(), db);
        } else if (jobTypeName.equals(DELETE)) {
            Long dbId = db == null ? -1 : db.getId();
            return new DeleteInfoProcDir(env.getDeleteHandler(), env.getLoadInstance(), dbId);
        } else if (jobTypeName.equals(ROLLUP)) {
            return new RollupProcDir(env.getMaterializedViewHandler(), db);
        } else if (jobTypeName.equals(SCHEMA_CHANGE)) {
            return new SchemaChangeProcDir(env.getSchemaChangeHandler(), db);
        } else if (jobTypeName.equals(EXPORT)) {
            return new ExportProcNode(env.getExportMgr(), db);
        } else if (jobTypeName.equals(BUILD_INDEX)) {
            return new BuildIndexProcDir(env.getSchemaChangeHandler(), db);
        } else {
            throw new AnalysisException("Invalid job type: " + jobTypeName);
        }
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(env);

        BaseProcResult result = new BaseProcResult();

        result.setNames(TITLE_NAMES);

        // db is null means need total result of all databases
        if (db == null) {
            return fetchResultForAllDbs();
        }

        long dbId = db.getId();

        // load
        LoadManager loadManager = Env.getCurrentEnv().getLoadManager();
        Long pendingNum = new Long(loadManager.getLoadJobNum(org.apache.doris.load.loadv2.JobState.PENDING, dbId));
        Long runningNum = new Long(loadManager.getLoadJobNum(org.apache.doris.load.loadv2.JobState.LOADING, dbId));
        Long finishedNum = new Long(loadManager.getLoadJobNum(org.apache.doris.load.loadv2.JobState.FINISHED, dbId));
        Long cancelledNum = new Long(loadManager.getLoadJobNum(org.apache.doris.load.loadv2.JobState.CANCELLED, dbId));
        Long totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(LOAD, pendingNum.toString(), runningNum.toString(), finishedNum.toString(),
                cancelledNum.toString(), totalNum.toString()));

        // delete
        // TODO: find it from delete handler
        pendingNum = 0L;
        runningNum = 0L;
        finishedNum = 0L;
        cancelledNum = 0L;
        totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(DELETE, pendingNum.toString(), runningNum.toString(), finishedNum.toString(),
                                         cancelledNum.toString(), totalNum.toString()));

        // rollup
        MaterializedViewHandler materializedViewHandler = Env.getCurrentEnv().getMaterializedViewHandler();
        pendingNum = materializedViewHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.PENDING, dbId);
        runningNum = materializedViewHandler.getAlterJobV2Num(
                org.apache.doris.alter.AlterJobV2.JobState.WAITING_TXN, dbId)
                + materializedViewHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.RUNNING, dbId);
        finishedNum = materializedViewHandler.getAlterJobV2Num(
                org.apache.doris.alter.AlterJobV2.JobState.FINISHED, dbId);
        cancelledNum = materializedViewHandler.getAlterJobV2Num(
                org.apache.doris.alter.AlterJobV2.JobState.CANCELLED, dbId);
        totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(ROLLUP, pendingNum.toString(), runningNum.toString(), finishedNum.toString(),
                                         cancelledNum.toString(), totalNum.toString()));

        // schema change
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        pendingNum = schemaChangeHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.PENDING, dbId);
        runningNum = schemaChangeHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.WAITING_TXN, dbId)
                + schemaChangeHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.RUNNING, dbId);
        finishedNum = schemaChangeHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.FINISHED, dbId);
        cancelledNum = schemaChangeHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.CANCELLED, dbId);
        totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(SCHEMA_CHANGE, pendingNum.toString(), runningNum.toString(),
                                         finishedNum.toString(), cancelledNum.toString(), totalNum.toString()));

        // export
        ExportMgr exportMgr = Env.getCurrentEnv().getExportMgr();
        pendingNum = exportMgr.getJobNum(ExportJobState.PENDING, dbId);
        runningNum = exportMgr.getJobNum(ExportJobState.EXPORTING, dbId);
        finishedNum = exportMgr.getJobNum(ExportJobState.FINISHED, dbId);
        cancelledNum = exportMgr.getJobNum(ExportJobState.CANCELLED, dbId);
        totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(EXPORT, pendingNum.toString(), runningNum.toString(), finishedNum.toString(),
                cancelledNum.toString(), totalNum.toString()));

        return result;
    }

    public ProcResult fetchResultForAllDbs() {
        BaseProcResult result = new BaseProcResult();

        result.setNames(TITLE_NAMES);
        // load
        LoadManager loadManager = Env.getCurrentEnv().getLoadManager();
        Long pendingNum = new Long(loadManager.getLoadJobNum(org.apache.doris.load.loadv2.JobState.PENDING));
        Long runningNum = new Long(loadManager.getLoadJobNum(org.apache.doris.load.loadv2.JobState.LOADING));
        Long finishedNum = new Long(loadManager.getLoadJobNum(org.apache.doris.load.loadv2.JobState.FINISHED));
        Long cancelledNum = new Long(loadManager.getLoadJobNum(org.apache.doris.load.loadv2.JobState.CANCELLED));
        Long totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(LOAD, pendingNum.toString(), runningNum.toString(), finishedNum.toString(),
                cancelledNum.toString(), totalNum.toString()));

        // delete
        // TODO: find it from delete handler
        pendingNum = 0L;
        runningNum = 0L;
        finishedNum = 0L;
        cancelledNum = 0L;
        totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(DELETE, pendingNum.toString(), runningNum.toString(), finishedNum.toString(),
                cancelledNum.toString(), totalNum.toString()));

        // rollup
        MaterializedViewHandler materializedViewHandler = Env.getCurrentEnv().getMaterializedViewHandler();
        pendingNum = materializedViewHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.PENDING);
        runningNum = materializedViewHandler.getAlterJobV2Num(
            org.apache.doris.alter.AlterJobV2.JobState.WAITING_TXN)
            + materializedViewHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.RUNNING);
        finishedNum = materializedViewHandler.getAlterJobV2Num(
            org.apache.doris.alter.AlterJobV2.JobState.FINISHED);
        cancelledNum = materializedViewHandler.getAlterJobV2Num(
            org.apache.doris.alter.AlterJobV2.JobState.CANCELLED);
        totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(ROLLUP, pendingNum.toString(), runningNum.toString(), finishedNum.toString(),
                cancelledNum.toString(), totalNum.toString()));

        // schema change
        SchemaChangeHandler schemaChangeHandler = Env.getCurrentEnv().getSchemaChangeHandler();
        pendingNum = schemaChangeHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.PENDING);
        runningNum = schemaChangeHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.WAITING_TXN)
            + schemaChangeHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.RUNNING);
        finishedNum = schemaChangeHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.FINISHED);
        cancelledNum = schemaChangeHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.CANCELLED);
        totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(SCHEMA_CHANGE, pendingNum.toString(), runningNum.toString(),
                finishedNum.toString(), cancelledNum.toString(), totalNum.toString()));

        // export
        ExportMgr exportMgr = Env.getCurrentEnv().getExportMgr();
        pendingNum = exportMgr.getJobNum(ExportJobState.PENDING);
        runningNum = exportMgr.getJobNum(ExportJobState.EXPORTING);
        finishedNum = exportMgr.getJobNum(ExportJobState.FINISHED);
        cancelledNum = exportMgr.getJobNum(ExportJobState.CANCELLED);
        totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(EXPORT, pendingNum.toString(), runningNum.toString(), finishedNum.toString(),
                cancelledNum.toString(), totalNum.toString()));

        return result;
    }
}
