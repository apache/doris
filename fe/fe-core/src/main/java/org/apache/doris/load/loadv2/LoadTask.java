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

package org.apache.doris.load.loadv2;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.FailMsg;
import org.apache.doris.task.MasterTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class LoadTask extends MasterTask {

    public enum MergeType {
        MERGE,
        APPEND,
        DELETE
    }

    public enum TaskType {
        PENDING,
        LOADING
    }

    private static final Logger LOG = LogManager.getLogger(LoadTask.class);

    protected TaskType taskType;
    protected LoadTaskCallback callback;
    protected TaskAttachment attachment;
    protected FailMsg failMsg = new FailMsg();
    protected int retryTime = 1;

    public LoadTask(LoadTaskCallback callback, TaskType taskType) {
        this.taskType = taskType;
        this.signature = Catalog.getCurrentCatalog().getNextId();
        this.callback = callback;
    }

    @Override
    protected void exec() {
        boolean isFinished = false;
        try {
            // execute pending task
            executeTask();
            // callback on pending task finished
            callback.onTaskFinished(attachment);
            isFinished = true;
        } catch (UserException e) {
            failMsg.setMsg(e.getMessage() == null ? "" : e.getMessage());
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId())
                    .add("error_msg", "Failed to execute load task").build(), e);
        } catch (Exception e) {
            failMsg.setMsg(e.getMessage() == null ? "" : e.getMessage());
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId())
                    .add("error_msg", "Unexpected failed to execute load task").build(), e);
        } finally {
            if (!isFinished) {
                // callback on pending task failed
                callback.onTaskFailed(signature, failMsg);
            }
        }
    }

    /**
     * init load task
     * @throws LoadException
     */
    public void init() throws LoadException {
    }

    /**
     * execute load task
     *
     * @throws UserException task is failed
     */
    abstract void executeTask() throws Exception;

    public int getRetryTime() {
        return retryTime;
    }

    // Derived class may need to override this.
    public void updateRetryInfo() {
        this.retryTime--;
        this.signature = Catalog.getCurrentCatalog().getNextId();
    }

    public TaskType getTaskType() {
        return taskType;
    }
}
