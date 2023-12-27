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

package org.apache.doris.mtmv;

import org.apache.doris.common.Config;
import org.apache.doris.job.extensions.mtmv.MTMVTask;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.util.LinkedList;

/**
 * MTMVJobInfo
 */
public class MTMVJobInfo {
    @SerializedName("jobName")
    private String jobName;
    @SerializedName("ht")
    private LinkedList<MTMVTask> historyTasks;

    public MTMVJobInfo(String jobName) {
        this.jobName = jobName;
        historyTasks = Lists.newLinkedList();
    }

    public String getJobName() {
        return jobName;
    }

    public void addHistoryTask(MTMVTask task) {
        if (Config.max_persistence_task_count < 1) {
            return;
        }
        historyTasks.add(task);
        if (historyTasks.size() > Config.max_persistence_task_count) {
            historyTasks.removeFirst();
        }
    }

    public LinkedList<MTMVTask> getHistoryTasks() {
        return historyTasks;
    }

    @Override
    public String toString() {
        return "MTMVJobInfo{"
                + "jobName='" + jobName + '\''
                + ", historyTasks=" + historyTasks
                + '}';
    }
}
