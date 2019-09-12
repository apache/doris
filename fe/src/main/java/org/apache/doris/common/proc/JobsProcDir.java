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

import org.apache.doris.alter.RollupHandler;
import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.load.ExportJob;
import org.apache.doris.load.ExportMgr;
import org.apache.doris.load.Load;
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

    private Catalog catalog;
    private Database db;

    public JobsProcDir(Catalog catalog, Database db) {
        this.catalog = catalog;
        this.db = db;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String jobTypeName) throws AnalysisException {
        if (Strings.isNullOrEmpty(jobTypeName) || catalog == null) {
            throw new AnalysisException("Job type name is null");
        }

        if (jobTypeName.equals(LOAD)) {
            return new LoadProcDir(catalog.getLoadInstance(), db);
        } else if (jobTypeName.equals(DELETE)) {
            return new DeleteInfoProcDir(catalog.getLoadInstance(), db.getId());
        } else if (jobTypeName.equals(ROLLUP)) {
            return new RollupProcDir(catalog.getRollupHandler(), db);
        } else if (jobTypeName.equals(SCHEMA_CHANGE)) {
            return new SchemaChangeProcNode(catalog.getSchemaChangeHandler(), db);
        } else if (jobTypeName.equals(EXPORT)) {
            return new ExportProcNode(catalog.getExportMgr(), db);
        } else {
            throw new AnalysisException("Invalid job type: " + jobTypeName);
        }
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(catalog);

        BaseProcResult result = new BaseProcResult();

        result.setNames(TITLE_NAMES);

        long dbId = db.getId();
        // load
        Load load = Catalog.getInstance().getLoadInstance();
        LoadManager loadManager = Catalog.getCurrentCatalog().getLoadManager();
        Long pendingNum = load.getLoadJobNum(org.apache.doris.load.LoadJob.JobState.PENDING, dbId)
                + loadManager.getLoadJobNum(org.apache.doris.load.loadv2.JobState.PENDING, dbId);
        Long runningNum = load.getLoadJobNum(org.apache.doris.load.LoadJob.JobState.ETL, dbId)
                + load.getLoadJobNum(org.apache.doris.load.LoadJob.JobState.LOADING, dbId)
                + loadManager.getLoadJobNum(org.apache.doris.load.loadv2.JobState.LOADING, dbId);
        Long finishedNum = load.getLoadJobNum(org.apache.doris.load.LoadJob.JobState.QUORUM_FINISHED, dbId)
                + load.getLoadJobNum(org.apache.doris.load.LoadJob.JobState.FINISHED, dbId)
                + loadManager.getLoadJobNum(org.apache.doris.load.loadv2.JobState.FINISHED, dbId);
        Long cancelledNum = load.getLoadJobNum(org.apache.doris.load.LoadJob.JobState.CANCELLED, dbId)
                + loadManager.getLoadJobNum(org.apache.doris.load.loadv2.JobState.CANCELLED, dbId);
        Long totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(LOAD, pendingNum.toString(), runningNum.toString(), finishedNum.toString(),
                                         cancelledNum.toString(), totalNum.toString()));

        // delete
        pendingNum = 0L;
        runningNum = load.getDeleteJobNumByState(dbId, org.apache.doris.load.LoadJob.JobState.LOADING);
        finishedNum = load.getDeleteJobNumByState(dbId, org.apache.doris.load.LoadJob.JobState.FINISHED);
        cancelledNum = load.getDeleteJobNumByState(dbId, org.apache.doris.load.LoadJob.JobState.CANCELLED);
        totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(DELETE, pendingNum.toString(), runningNum.toString(), finishedNum.toString(),
                                         cancelledNum.toString(), totalNum.toString()));

        // rollup
        RollupHandler rollupHandler = Catalog.getInstance().getRollupHandler();
        pendingNum = rollupHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.PENDING, dbId);
        runningNum = rollupHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.WAITING_TXN, dbId)
                + rollupHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.RUNNING, dbId);
        finishedNum = rollupHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.FINISHED, dbId);
        cancelledNum = rollupHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.CANCELLED, dbId);
        totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(ROLLUP, pendingNum.toString(), runningNum.toString(), finishedNum.toString(),
                                         cancelledNum.toString(), totalNum.toString()));

        // schema change
        SchemaChangeHandler schemaChangeHandler = Catalog.getInstance().getSchemaChangeHandler();
        pendingNum = schemaChangeHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.PENDING, dbId);
        runningNum = schemaChangeHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.WAITING_TXN, dbId)
                + schemaChangeHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.RUNNING, dbId);
        finishedNum = schemaChangeHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.FINISHED, dbId);
        cancelledNum = schemaChangeHandler.getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState.CANCELLED, dbId);
        totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(SCHEMA_CHANGE, pendingNum.toString(), runningNum.toString(),
                                         finishedNum.toString(), cancelledNum.toString(), totalNum.toString()));

        // export
        ExportMgr exportMgr = Catalog.getInstance().getExportMgr();
        pendingNum = exportMgr.getJobNum(ExportJob.JobState.PENDING, dbId);
        runningNum = exportMgr.getJobNum(ExportJob.JobState.EXPORTING, dbId);
        finishedNum = exportMgr.getJobNum(ExportJob.JobState.FINISHED, dbId);
        cancelledNum = exportMgr.getJobNum(ExportJob.JobState.CANCELLED, dbId);
        totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(EXPORT, pendingNum.toString(), runningNum.toString(), finishedNum.toString(),
                cancelledNum.toString(), totalNum.toString()));

        return result;
    }
}
