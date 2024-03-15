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

package org.apache.doris.insertoverwrite;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.insertoverwrite.InsertOverwriteLog.InsertOverwriteOpType;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class InsertOverwriteManager extends MasterDaemon implements Writable {
    private static final Logger LOG = LogManager.getLogger(InsertOverwriteManager.class);

    private static final long CLEAN_INTERVAL_SECOND = 10;

    @SerializedName(value = "tasks")
    private Map<Long, InsertOverwriteTask> tasks = Maps.newConcurrentMap();

    public InsertOverwriteManager() {
        super("InsertOverwriteDropDirtyPartitions", CLEAN_INTERVAL_SECOND * 1000);
    }

    /**
     * register insert overwrite task
     *
     * @param dbId
     * @param tableId
     * @param tempPartitionNames
     * @return taskId
     */
    public long registerTask(long dbId, long tableId, List<String> tempPartitionNames) {
        long taskId = Env.getCurrentEnv().getNextId();
        InsertOverwriteTask task = new InsertOverwriteTask(dbId, tableId,
                tempPartitionNames);
        tasks.put(taskId, task);
        Env.getCurrentEnv().getEditLog()
                .logInsertOverwrite(new InsertOverwriteLog(taskId, task, InsertOverwriteOpType.ADD));
        return taskId;
    }

    /**
     * when insert overwrite fail, try drop temp partition
     *
     * @param taskId
     */
    public void taskFail(long taskId) {
        boolean rollback = rollback(taskId);
        if (rollback) {
            removeTask(taskId);
        } else {
            cancelTask(taskId);
        }
    }

    /**
     * when insert overwrite success, drop task
     *
     * @param taskId
     */
    public void taskSuccess(long taskId) {
        removeTask(taskId);
    }

    /**
     * for transferToMaster, try drop all temp partitions
     */
    public void allTaskFail() {
        LOG.info("try drop all temp partitions when transferToMaster");
        HashMap<Long, InsertOverwriteTask> copyTasks = Maps.newHashMap(tasks);
        for (Entry<Long, InsertOverwriteTask> entry : copyTasks.entrySet()) {
            taskFail(entry.getKey());
        }
    }

    private void cancelTask(long taskId) {
        if (tasks.containsKey(taskId)) {
            LOG.info("cancel insert overwrite task: {}", tasks.get(taskId));
            tasks.get(taskId).setCancel(true);
            Env.getCurrentEnv().getEditLog()
                    .logInsertOverwrite(new InsertOverwriteLog(taskId, null, InsertOverwriteOpType.CANCEL));
        }
    }

    private void removeTask(long taskId) {
        if (tasks.containsKey(taskId)) {
            LOG.info("remove insert overwrite task: {}", tasks.get(taskId));
            tasks.remove(taskId);
            Env.getCurrentEnv().getEditLog()
                    .logInsertOverwrite(new InsertOverwriteLog(taskId, null, InsertOverwriteOpType.DROP));
        }
    }

    /**
     * drop temp partitions
     *
     * @param taskId
     * @return if success
     */
    private boolean rollback(long taskId) {
        InsertOverwriteTask task = tasks.get(taskId);
        OlapTable olapTable;
        try {
            olapTable = task.getTable();
        } catch (DdlException e) {
            LOG.warn("can not get table, task: {}", task);
            return true;
        }
        return InsertOverwriteUtil.dropPartitions(olapTable, task.getTempPartitionNames());
    }

    /**
     * replay logs
     *
     * @param insertOverwriteLog
     */
    public void replayInsertOverwriteLog(InsertOverwriteLog insertOverwriteLog) {
        switch (insertOverwriteLog.getOpType()) {
            case ADD:
                tasks.put(insertOverwriteLog.getTaskId(), insertOverwriteLog.getTask());
                break;
            case DROP:
                tasks.remove(insertOverwriteLog.getTaskId());
                break;
            case CANCEL:
                InsertOverwriteTask task = tasks.get(insertOverwriteLog.getTaskId());
                if (task != null) {
                    task.setCancel(true);
                }
                break;
            default:
                LOG.warn("error insertOverwriteLog: {}", insertOverwriteLog.toString());
        }
    }

    /**
     * Regularly drop partitions that have failed dropped
     */
    @Override
    protected void runAfterCatalogReady() {
        LOG.info("start clean insert overwrite temp partitions");
        HashMap<Long, InsertOverwriteTask> copyTasks = Maps.newHashMap(tasks);
        for (Entry<Long, InsertOverwriteTask> entry : copyTasks.entrySet()) {
            if (entry.getValue().isCancel()) {
                boolean rollback = rollback(entry.getKey());
                if (rollback) {
                    removeTask(entry.getKey());
                }
            }
        }
    }


    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static InsertOverwriteManager read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, InsertOverwriteManager.class);
    }
}
