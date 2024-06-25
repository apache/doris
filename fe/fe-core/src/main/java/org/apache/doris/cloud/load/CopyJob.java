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

package org.apache.doris.cloud.load;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.CopyStmt;
import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.cloud.proto.Cloud.FinishCopyRequest.Action;
import org.apache.doris.cloud.proto.Cloud.StagePB;
import org.apache.doris.cloud.proto.Cloud.StagePB.StageType;
import org.apache.doris.cloud.stage.StageUtil;
import org.apache.doris.cloud.storage.RemoteBase.ObjectInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.loadv2.LoadJobFinalOperation;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TUniqueId;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class CopyJob extends CloudBrokerLoadJob {
    private static final Logger LOG = LogManager.getLogger(CopyJob.class);
    private static final String TABLE_NAME_KEY = "TableName";
    private static final String USER_NAME_KEY = "UserName";

    @Getter
    private String stageId;
    @Getter
    private StagePB.StageType stageType;
    @Getter
    private String stagePrefix;
    @Getter
    private long sizeLimit;
    @Getter
    private String pattern;
    @Getter
    private ObjectInfo objectInfo;

    @SerializedName("cid")
    @Getter
    private String copyId;
    @Getter
    private boolean forceCopy;
    @SerializedName("lfp")
    private String loadFilePaths = "";
    @SerializedName("pty")
    private Map<String, String> properties = new HashMap<>();
    private volatile boolean abortedCopy = false;
    private boolean isReplay = false;
    private List<String> loadFiles = null;

    public CopyJob() {
        super(EtlJobType.COPY);
    }

    public CopyJob(long dbId, String label, TUniqueId queryId, BrokerDesc brokerDesc, OriginStatement originStmt,
            UserIdentity userInfo, String stageId, StagePB.StageType stageType, String stagePrefix, long sizeLimit,
            String pattern, ObjectInfo objectInfo, boolean forceCopy, String user) throws MetaNotFoundException {
        super(EtlJobType.COPY, dbId, label, brokerDesc, originStmt, userInfo);
        this.stageId = stageId;
        this.stageType = stageType;
        this.stagePrefix = stagePrefix;
        this.sizeLimit =  sizeLimit;
        this.pattern = pattern;
        this.objectInfo = objectInfo;
        this.forceCopy = forceCopy;
        this.copyId = DebugUtil.printId(queryId);
        this.properties.put(USER_NAME_KEY, user);
    }

    @Override
    public void checkAndSetDataSourceInfo(Database db, List<DataDescription> dataDescriptions) throws DdlException {
        super.checkAndSetDataSourceInfo(db, dataDescriptions);
        // now, copy into only support one table
        for (DataDescription dataDescription : dataDescriptions) {
            properties.put(TABLE_NAME_KEY, dataDescription.getTableName());
        }
    }

    @Override
    protected LoadTask createPendingTask() {
        return new CopyLoadPendingTask(this, fileGroupAggInfo.getAggKeyToFileGroups(), brokerDesc);
    }

    @Override
    protected void afterCommit() throws DdlException {
        super.afterCommit();
        if (forceCopy) {
            return;
        }
        for (Entry<FileGroupAggKey, List<BrokerFileGroup>> entry : fileGroupAggInfo.getAggKeyToFileGroups()
                .entrySet()) {
            long tableId = entry.getKey().getTableId();
            LOG.debug("Start finish copy for stage={}, table={}, queryId={}", stageId, tableId, getCopyId());
            ((CloudInternalCatalog) Env.getCurrentInternalCatalog())
                    .finishCopy(stageId, stageType, tableId, getCopyId(), 0, Action.COMMIT);
            // delete internal stage files and copy job
            if (Config.cloud_delete_loaded_internal_stage_files && loadFiles != null && stageType == StageType.INTERNAL
                    && !isForceCopy()) {
                CleanCopyJobTask copyJobCleanTask = new CleanCopyJobTask(objectInfo, stageId, stageType, tableId,
                        copyId, loadFiles);
                ((CloudLoadManager) Env.getCurrentEnv().getLoadManager()).createCleanCopyJobTask(copyJobCleanTask);
            }
        }
    }

    @Override
    public void cancelJob(FailMsg failMsg) throws DdlException {
        super.cancelJob(failMsg);
        loadFiles = null;
        if (forceCopy || abortedCopy) {
            return;
        }
        for (Entry<FileGroupAggKey, List<BrokerFileGroup>> entry : fileGroupAggInfo.getAggKeyToFileGroups()
                .entrySet()) {
            long tableId = entry.getKey().getTableId();
            LOG.info("Cancel copy for stage={}, table={}, queryId={}", stageId, tableId, getCopyId());
            ((CloudInternalCatalog) Env.getCurrentInternalCatalog())
                    .finishCopy(stageId, stageType, tableId, getCopyId(), 0, Action.ABORT);
        }
        abortedCopy = true;
    }

    @Override
    public void cancelJobWithoutCheck(FailMsg failMsg, boolean abortTxn, boolean needLog) {
        super.cancelJobWithoutCheck(failMsg, abortTxn, needLog);
        loadFiles = null;
        if (forceCopy || abortedCopy) {
            return;
        }
        abortCopy();
    }

    @Override
    protected void unprotectedExecuteCancel(FailMsg failMsg, boolean abortTxn) {
        super.unprotectedExecuteCancel(failMsg, abortTxn);
        loadFiles = null;
        if (forceCopy || abortedCopy) {
            return;
        }
        abortCopy();
    }

    private void abortCopy() {
        for (Entry<FileGroupAggKey, List<BrokerFileGroup>> entry : fileGroupAggInfo.getAggKeyToFileGroups()
                .entrySet()) {
            long tableId = entry.getKey().getTableId();
            try {
                LOG.info("Cancel copy for stage={}, table={}, queryId={}", stageId, tableId, getCopyId());
                ((CloudInternalCatalog) Env.getCurrentInternalCatalog())
                        .finishCopy(stageId, stageType, tableId, getCopyId(), 0, Action.ABORT);
                abortedCopy = true;
            } catch (DdlException e) {
                // if cancel copy failed, kvs in fdb will be cleaned when expired
                LOG.warn("Failed to cancel copy for stage={}, table={}, queryId={}", stageId, tableId, getCopyId(), e);
            }
        }
    }

    public void setAbortedCopy(boolean abortedCopy) {
        this.abortedCopy = abortedCopy;
    }

    @Override
    protected List<Comparable> getShowInfoUnderLock() throws DdlException {
        List<Comparable> showInfos = new ArrayList<>();
        showInfos.add(getCopyId());
        showInfos.addAll(super.getShowInfoUnderLock());
        // table name
        showInfos.add(getTableName());
        showInfos.add(loadFilePaths);
        return showInfos;
    }

    @Override
    protected void logFinalOperation() {
        Env.getCurrentEnv().getEditLog().logEndLoadJob(getLoadJobFinalOperation());
    }

    @Override
    public void unprotectReadEndOperation(LoadJobFinalOperation loadJobFinalOperation) {
        super.unprotectReadEndOperation(loadJobFinalOperation);
        this.copyId = loadJobFinalOperation.getCopyId();
        this.loadFilePaths = loadJobFinalOperation.getLoadFilePaths();
        this.properties.putAll(loadJobFinalOperation.getProperties());
    }

    @Override
    protected LoadJobFinalOperation getLoadJobFinalOperation() {
        return new LoadJobFinalOperation(id, loadingStatus, progress, loadStartTimestamp,
                finishTimestamp, state, failMsg, copyId, loadFilePaths, properties);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        copyId = Text.readString(in);
        loadFilePaths = Text.readString(in);
        String property = Text.readString(in);
        properties = property.isEmpty() ? new HashMap<>()
                : (new Gson().fromJson(property, new TypeToken<Map<String, String>>() {
                }.getType()));
    }

    @Override
    protected void analyzeStmt(StatementBase stmtBase, Database db) throws UserException {
        CopyStmt stmt = (CopyStmt) stmtBase;
        stmt.analyzeWhenReplay(getUser(), db.getFullName());
        // check if begin copy happened
        checkAndSetDataSourceInfo(db, stmt.getDataDescriptions());
        this.stageId = stmt.getStageId();
        this.stageType = stmt.getStageType();
        this.sizeLimit = stmt.getSizeLimit();
        this.pattern = stmt.getPattern();
        this.objectInfo = stmt.getObjectInfo();
        this.forceCopy = stmt.isForce();
        this.isReplay = true;
    }

    protected void setSelectedFiles(Map<FileGroupAggKey, List<List<TBrokerFileStatus>>> fileStatusMap) {
        this.loadFilePaths = selectedFilesToJson(fileStatusMap);
    }

    private String selectedFilesToJson(Map<FileGroupAggKey, List<List<TBrokerFileStatus>>> selectedFiles) {
        if (selectedFiles == null) {
            return "";
        }
        List<String> paths = new ArrayList<>();
        for (Entry<FileGroupAggKey, List<List<TBrokerFileStatus>>> entry : selectedFiles.entrySet()) {
            for (List<TBrokerFileStatus> fileStatuses : entry.getValue()) {
                paths.addAll(fileStatuses.stream().map(e -> e.path).collect(Collectors.toList()));
            }
        }
        if (stageType == StageType.INTERNAL) {
            loadFiles = StageUtil.parseLoadFiles(paths, objectInfo.getBucket(), stagePrefix);
        }
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(paths);
    }

    public String getTableName() {
        return properties.containsKey(TABLE_NAME_KEY) ? properties.get(TABLE_NAME_KEY) : "";
    }

    public String getFiles() {
        return loadFilePaths == null ? "" : loadFilePaths;
    }

    public String getUser() {
        return properties.get(USER_NAME_KEY);
    }

    protected boolean isReplay() {
        return this.isReplay;
    }

    @Override
    protected void unprotectedExecuteRetry(FailMsg failMsg) {
        LOG.info("CopyJob.unprotectedExecuteRetry(): forceCopy={}, abortedCopy={}", forceCopy, abortedCopy);
        super.unprotectedExecuteRetry(failMsg);
        if (forceCopy || abortedCopy) {
            return;
        }
        abortCopy();
    }
}
