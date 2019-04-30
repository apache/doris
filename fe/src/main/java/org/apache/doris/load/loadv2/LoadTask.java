/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.doris.load.loadv2;

import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.task.MasterTask;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public abstract class LoadTask extends MasterTask {

    private static final Logger LOG = LogManager.getLogger(LoadTask.class);

    protected LoadTaskCallback callback;
    protected TaskAttachment attachment;
    protected boolean isFinished = false;

    public LoadTask(LoadTaskCallback callback) {
        this.callback = callback;
    }

    @Override
    protected void exec() {
        Exception exception = null;
        try {
            // execute pending task
            executeTask();
            isFinished = true;
            // callback on pending task finished
            callback.onTaskFinished(attachment);
        } catch (Exception e) {
            exception = e;
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId())
                             .add("error_msg", "Failed to execute load task").build(), e);
        } finally {
            if (!isFinished) {
                // callback on pending task failed
                callback.onTaskFailed(exception.getMessage());
            }
        }
    }

    public boolean isFinished() {
        return isFinished;
    }

    /**
     * execute load task
     *
     * @throws UserException task is failed
     */
    abstract void executeTask() throws UserException;
}
