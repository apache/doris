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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.FailMsg;
import org.apache.doris.task.MasterTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;


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

    public enum Priority {
        HIGH(0),
        NORMAL(1),
        LOW(2);

        Priority(int value) {
            this.value = value;
        }

        private final int value;

        public int getValue() {
            return value;
        }
    }

    private static final Logger LOG = LogManager.getLogger(LoadTask.class);
    public static final Comparator<LoadTask> COMPARATOR = Comparator.comparing(LoadTask::getPriorityValue)
                .thenComparingLong(LoadTask::getSignature);

    protected TaskType taskType;
    protected LoadTaskCallback callback;
    protected TaskAttachment attachment;
    protected FailMsg failMsg = new FailMsg();
    protected int retryTime = 1;
    private volatile boolean done = false;
    protected long startTimeMs = 0;
    protected final Priority priority;

    public LoadTask(LoadTaskCallback callback, TaskType taskType, Priority priority) {
        this.taskType = taskType;
        this.signature = Env.getCurrentEnv().getNextId();
        this.callback = callback;
        this.priority = priority;
    }

    @Override
    protected void exec() {
        boolean isFinished = false;
        try {
            if (Config.isCloudMode()) {
                while (startTimeMs > System.currentTimeMillis()) {
                    try {
                        Thread.sleep(1000);
                        LOG.info("LoadTask:{} backoff startTimeMs:{} now:{}",
                                signature, startTimeMs, System.currentTimeMillis());
                    } catch (InterruptedException e) {
                        LOG.info("ignore InterruptedException: ", e);
                    }
                }
            }
            // execute pending task
            executeTask();
            // callback on pending task finished
            callback.onTaskFinished(attachment);
            isFinished = true;
        } catch (UserException e) {
            failMsg.setMsg(e.getMessage() == null ? "" : e.getMessage());
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId())
                    .add("error_msg", "Failed to execute load task").build(), e);
        } catch (Throwable t) {
            failMsg.setMsg(t.getMessage() == null ? "" : t.getMessage());
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId())
                    .add("error_msg", "Unexpected failed to execute load task").build(), t);
        } finally {
            if (!isFinished) {
                // callback on pending task failed
                callback.onTaskFailed(signature, failMsg);
            }
            done = true;
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
        this.signature = Env.getCurrentEnv().getNextId();
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public boolean isDone() {
        return done;
    }

    public void setStartTimeMs(long startTimeMs) {
        this.startTimeMs = startTimeMs;
    }

    public int getPriorityValue() {
        return this.priority.value;
    }

}
