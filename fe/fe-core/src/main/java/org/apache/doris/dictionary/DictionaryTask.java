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

package org.apache.doris.dictionary;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;

import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Task for dictionary operations
 */
public class DictionaryTask extends AbstractTask {
    private static final Logger LOG = LogManager.getLogger(DictionaryTask.class);

    @SerializedName(value = "di")
    private long dictionaryId;

    @SerializedName(value = "tc")
    private DictionaryTaskContext taskContext;

    public DictionaryTask() {
    }

    public DictionaryTask(long dictionaryId, DictionaryTaskContext taskContext) {
        this.dictionaryId = dictionaryId;
        this.taskContext = taskContext;
    }

    @Override
    public void run() throws JobException {
        LOG.info("begin execute dictionary task, taskId: {}, dictionaryId: {}, context: {}", 
                getTaskId(), dictionaryId, taskContext);

        Dictionary dictionary = Env.getCurrentEnv().getDictionaryManager().getDictionary(dictionaryId);
        if (dictionary == null) {
            String msg = "Dictionary not found: " + dictionaryId;
            LOG.warn(msg);
            return;
        }

        DictionaryJob job = (DictionaryJob) getJobOrJobException();
        job.writeLock();
        try {
            if (taskContext.isLoad()) {
                Env.getCurrentEnv().getDictionaryManager().dataLoad(null, dictionary);
            } else {
                Env.getCurrentEnv().getDictionaryManager().dataUnload(dictionary);
            }
            LOG.info("finish execute dictionary task, taskId: {}, dictionaryId: {}", getTaskId(), dictionaryId);
        } catch (Exception e) {
            LOG.warn("execute dictionary task failed, taskId: {}, dictionaryId: {}", getTaskId(), dictionaryId, e);
        } finally {
            job.writeUnlock();
        }
    }

    @Override
    public String toString() {
        return "DictionaryTask{" +
                "taskId=" + getTaskId() +
                ", dictionaryId=" + dictionaryId +
                ", taskContext=" + taskContext +
                '}';
    }

    @Override
    public TRow getTvfInfo(String jobName) {
        TRow trow = new TRow();
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getTaskId())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getJobId())));
        trow.addToColumnValue(new TCell().setStringVal(jobName));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(dictionaryId)));
        trow.addToColumnValue(new TCell().setStringVal(
                getStatus() == null ? FeConstants.null_string : getStatus().toString()));
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(getCreateTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(getStartTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(getFinishTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(
                (getFinishTimeMs() == null || getFinishTimeMs() == 0) ? FeConstants.null_string
                        : String.valueOf(getFinishTimeMs() - getStartTimeMs())));
        return trow;
    }

    @Override
    protected void closeOrReleaseResources() {
        // Nothing to clean up
    }

    @Override
    protected void executeCancelLogic(boolean needWaitCancelComplete) throws Exception {
        LOG.info("Nothing to cancel for dictionary task {}", getTaskId());
    }
}
