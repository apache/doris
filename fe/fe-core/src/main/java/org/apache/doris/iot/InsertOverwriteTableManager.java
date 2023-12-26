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

package org.apache.doris.iot;

import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.DropPartitionClause;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.DdlExecutor;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class InsertOverwriteTableManager implements Writable {
    private static final Logger LOG = LogManager.getLogger(InsertOverwriteTableManager.class);

    @SerializedName(value = "tasks")
    private Map<Long, InsertOverwriteTableTask> tasks = Maps.newHashMap();


    public long registerTask(long dbId, long tableId, List<String> tempPartitionNames) {
        long taskId = Env.getCurrentEnv().getNextId();
        InsertOverwriteTableTask task = new InsertOverwriteTableTask(dbId, tableId,
                tempPartitionNames);
        tasks.put(taskId, task);
        return taskId;
    }

    public void replayRegisterTask() {

    }

    public void taskFail(long taskId) {
        boolean rollback = rollback(taskId);
        if (rollback) {
            removeTask(taskId);
        } else {
            setTaskAbnormal(taskId);
        }
    }

    private void setTaskAbnormal(long taskId) {
        tasks.get(taskId).setNormal(false);
        //Env.getCurrentEnv().getEditLog().logUpdateIotTask();
    }

    public void taskSuccess(long taskId) {
        removeTask(taskId);
    }

    public void removeTask(long taskId) {
        tasks.remove(taskId);
        //Env.getCurrentEnv().getEditLog().logDropIotTask();
    }

    public void onTransferToMaster() {
        for (Entry<Long, InsertOverwriteTableTask> entry : tasks.entrySet()) {
            taskFail(entry.getKey());
        }
    }

    public boolean rollback(long taskId) {
        InsertOverwriteTableTask task = tasks.get(taskId);
        try {
            for (String partitionName : tempPartitionNames) {
                List<AlterClause> ops = new ArrayList<>();
                ops.add(new DropPartitionClause(true, partitionName, true, true));
                AlterTableStmt dropTablePartitionStmt = new AlterTableStmt(targetTableName, ops);
                Analyzer tempAnalyzer = new Analyzer(Env.getCurrentEnv(), ctx);
                dropTablePartitionStmt.analyze(tempAnalyzer);
                DdlExecutor.execute(ctx.getEnv(), dropTablePartitionStmt);
            }
        } catch (Exception e) {
            LOG.warn("IOT drop partitions error", e);
        }
        return true;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static InsertOverwriteTableManager read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, InsertOverwriteTableManager.class);
    }
}
