// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.common.proc;

import com.baidu.palo.alter.RollupHandler;
import com.baidu.palo.alter.SchemaChangeHandler;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.clone.CloneJob.JobState;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.load.ExportJob;
import com.baidu.palo.load.ExportMgr;
import com.baidu.palo.load.Load;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/*
 * SHOW PROC '/jobs/dbName/'
 * show job type
 */
public class JobsProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobType").add("Pending").add("Running").add("Finished")
            .add("Cancelled").add("Total")
            .build();

    private static final String CLONE = "clone";
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

        if (jobTypeName.equals(CLONE)) {
            return new CloneProcNode(catalog.getCloneInstance(), db);
        } else if (jobTypeName.equals(LOAD)) {
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
        // clone
        Integer pendingNum = Catalog.getInstance().getCloneInstance().getCloneJobNum(JobState.PENDING, dbId);
        Integer runningNum = Catalog.getInstance().getCloneInstance().getCloneJobNum(JobState.RUNNING, dbId);
        Integer finishedNum = Catalog.getInstance().getCloneInstance().getCloneJobNum(JobState.FINISHED, dbId);
        Integer cancelledNum = Catalog.getInstance().getCloneInstance().getCloneJobNum(JobState.CANCELLED, dbId);
        Integer totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(CLONE, pendingNum.toString(), runningNum.toString(), finishedNum.toString(),
                                         cancelledNum.toString(), totalNum.toString()));

        // load
        Load load = Catalog.getInstance().getLoadInstance();
        pendingNum = load.getLoadJobNum(com.baidu.palo.load.LoadJob.JobState.PENDING, dbId);
        runningNum = load.getLoadJobNum(com.baidu.palo.load.LoadJob.JobState.ETL, dbId)
                + load.getLoadJobNum(com.baidu.palo.load.LoadJob.JobState.LOADING, dbId);
        finishedNum = load.getLoadJobNum(com.baidu.palo.load.LoadJob.JobState.QUORUM_FINISHED, dbId)
                + load.getLoadJobNum(com.baidu.palo.load.LoadJob.JobState.FINISHED, dbId);
        cancelledNum = load.getLoadJobNum(com.baidu.palo.load.LoadJob.JobState.CANCELLED, dbId);
        totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(LOAD, pendingNum.toString(), runningNum.toString(), finishedNum.toString(),
                                         cancelledNum.toString(), totalNum.toString()));

        // delete
        pendingNum = 0;
        runningNum = 0;
        finishedNum = load.getDeleteInfoNum(dbId);
        cancelledNum = 0;
        totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(DELETE, pendingNum.toString(), runningNum.toString(), finishedNum.toString(),
                                         cancelledNum.toString(), totalNum.toString()));

        // rollup
        RollupHandler rollupHandler = Catalog.getInstance().getRollupHandler();
        pendingNum = rollupHandler.getAlterJobNum(com.baidu.palo.alter.AlterJob.JobState.PENDING, dbId);
        runningNum = rollupHandler.getAlterJobNum(com.baidu.palo.alter.AlterJob.JobState.RUNNING, dbId);
        finishedNum = rollupHandler.getAlterJobNum(com.baidu.palo.alter.AlterJob.JobState.FINISHED, dbId);
        cancelledNum = rollupHandler.getAlterJobNum(com.baidu.palo.alter.AlterJob.JobState.CANCELLED, dbId);
        totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(ROLLUP, pendingNum.toString(), runningNum.toString(), finishedNum.toString(),
                                         cancelledNum.toString(), totalNum.toString()));

        // schema change
        SchemaChangeHandler schemaChangeHandler = Catalog.getInstance().getSchemaChangeHandler();
        pendingNum = schemaChangeHandler.getAlterJobNum(com.baidu.palo.alter.AlterJob.JobState.PENDING, dbId);
        runningNum = schemaChangeHandler.getAlterJobNum(com.baidu.palo.alter.AlterJob.JobState.RUNNING, dbId);
        finishedNum = schemaChangeHandler.getAlterJobNum(com.baidu.palo.alter.AlterJob.JobState.FINISHED, dbId);
        cancelledNum = schemaChangeHandler.getAlterJobNum(com.baidu.palo.alter.AlterJob.JobState.CANCELLED, dbId);
        totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(SCHEMA_CHANGE, pendingNum.toString(), runningNum.toString(),
                                         finishedNum.toString(), cancelledNum.toString(), totalNum.toString()));

        /*
        // backup
        BackupHandler backupHandler = Catalog.getInstance().getBackupHandler();
        pendingNum = backupHandler.getBackupJobNum(BackupJobState.PENDING, dbId);
        runningNum = backupHandler.getBackupJobNum(BackupJobState.SNAPSHOT, dbId)
                + backupHandler.getBackupJobNum(BackupJobState.UPLOAD, dbId)
                + backupHandler.getBackupJobNum(BackupJobState.UPLOADING, dbId)
                + backupHandler.getBackupJobNum(BackupJobState.FINISHING, dbId);
        finishedNum = backupHandler.getBackupJobNum(BackupJobState.FINISHED, dbId);
        cancelledNum = backupHandler.getBackupJobNum(BackupJobState.CANCELLED, dbId);
        totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(BACKUP, pendingNum.toString(), runningNum.toString(),
                                         finishedNum.toString(), cancelledNum.toString(), totalNum.toString()));
        
        // restore
        pendingNum = backupHandler.getRestoreJobNum(RestoreJobState.PENDING, dbId);
        runningNum = backupHandler.getRestoreJobNum(RestoreJobState.RESTORE_META, dbId)
                + backupHandler.getRestoreJobNum(RestoreJobState.DOWNLOAD, dbId)
                + backupHandler.getRestoreJobNum(RestoreJobState.DOWNLOADING, dbId);
        finishedNum = backupHandler.getRestoreJobNum(RestoreJobState.FINISHED, dbId);
        cancelledNum = backupHandler.getRestoreJobNum(RestoreJobState.CANCELLED, dbId);
        totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(RESTORE, pendingNum.toString(), runningNum.toString(),
                                         finishedNum.toString(), cancelledNum.toString(), totalNum.toString()));
         */

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
