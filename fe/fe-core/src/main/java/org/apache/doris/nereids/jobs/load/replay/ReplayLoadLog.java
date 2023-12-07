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

package org.apache.doris.nereids.jobs.load.replay;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.extensions.insert.InsertJob;
import org.apache.doris.load.FailMsg;
import org.apache.doris.persist.gson.GsonUtils;

import java.io.DataOutput;
import java.io.IOException;

/**
 * for load replay
 */
public abstract class ReplayLoadLog implements Writable {

    private long jobId;

    private ReplayLoadLog(long jobId) {
        this.jobId = jobId;
    }

    public static ReplayCreateLoadLog logCreateLoadOperation(InsertJob loadJob) {
        return new ReplayCreateLoadLog(loadJob);
    }

    public static ReplayEndLoadLog logEndLoadOperation(InsertJob loadJob) {
        return new ReplayEndLoadLog(loadJob);
    }

    public Long getId() {
        return jobId;
    }

    public abstract void write(DataOutput out) throws IOException;

    /**
     * replay create load log
     */
    public static class ReplayCreateLoadLog extends ReplayLoadLog {
        private final long dbId;

        public ReplayCreateLoadLog(InsertJob loadJob) {
            super(loadJob.getJobId());
            dbId = loadJob.getDbId();
        }

        public long getDbId() {
            return dbId;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, GsonUtils.GSON.toJson(this));
        }
    }

    /**
     * replay end load log
     */
    public static class ReplayEndLoadLog extends ReplayLoadLog {
        private final JobStatus status;
        // 0: the job status is pending
        // n/100: n is the number of task which has been finished
        // 99: all tasks have been finished
        // 100: txn status is visible and load has been finished
        private final int progress;
        private final long startTimestamp;
        private final long finishTimestamp;
        private final FailMsg failMsg;

        /**
         * serialize end load log
         * @param loadJob serialize some field from loadJob
         */
        public ReplayEndLoadLog(InsertJob loadJob) {
            super(loadJob.getJobId());
            status = loadJob.getJobStatus();
            progress = loadJob.getProgress();
            startTimestamp = loadJob.getStartTimestamp();
            finishTimestamp = loadJob.getFinishTimestamp();
            failMsg = loadJob.getFailMsg();
        }

        public JobStatus getLoadingStatus() {
            return status;
        }

        public int getProgress() {
            return progress;
        }

        public long getStartTimestamp() {
            return startTimestamp;
        }

        public long getFinishTimestamp() {
            return finishTimestamp;
        }

        public FailMsg getFailMsg() {
            return failMsg;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, GsonUtils.GSON.toJson(this));
        }
    }
}
