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

package org.apache.doris.job.extensions.mtmv;

import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.qe.ShowResultSetMetaData;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class MTMVJob extends AbstractJob<MTMVTask> {
    @Override
    public void write(DataOutput out) throws IOException {
        
    }

    @Override
    protected void checkJobParamsInternal() {

    }

    @Override
    public List<MTMVTask> createTasks(TaskType taskType) {
        return null;
    }

    @Override
    public boolean isReadyForScheduling() {
        return false;
    }

    @Override
    public ShowResultSetMetaData getJobMetaData() {
        return null;
    }

    @Override
    public ShowResultSetMetaData getTaskMetaData() {
        return null;
    }

    @Override
    public JobType getJobType() {
        return null;
    }

    @Override
    public List<MTMVTask> queryTasks() {
        return null;
    }

    @Override
    public void onTaskCancel(MTMVTask task) {

    }

    @Override
    public List<String> getShowInfo() {
        return null;
    }
}
