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

package org.apache.doris.scheduler.job;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;

import lombok.extern.slf4j.Slf4j;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class JobTaskManager implements Writable {

    private ConcurrentHashMap<Long, LinkedList<JobTask>> jobTaskMap = new ConcurrentHashMap<>(16);

    public void addJobTask(JobTask jobTask) {
        LinkedList<JobTask> jobTasks = jobTaskMap.computeIfAbsent(jobTask.getJobId(), k -> new LinkedList<>());
        jobTasks.add(jobTask);
        Env.getCurrentEnv().getEditLog().logCreateJobTask(jobTask);
    }

    public LinkedList<JobTask> getJobTasks(Long jobId) {
        if (jobTaskMap.containsKey(jobId)) {
            LinkedList<JobTask> jobTasks = jobTaskMap.get(jobId);
            Collections.reverse(jobTasks);
            return jobTasks;
        }
        return new LinkedList<>();
    }

    public void replayCreateTask(JobTask task) {

        LinkedList<JobTask> jobTasks = jobTaskMap.computeIfAbsent(task.getJobId(), k -> new LinkedList<>());
        jobTasks.add(task);
        log.info(new LogBuilder(LogKey.SCHEDULER_TASK, task.getTaskId())
                .add("msg", "replay create scheduler task").build());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(jobTaskMap.size());
        for (Map.Entry<Long, LinkedList<JobTask>> entry : jobTaskMap.entrySet()) {
            out.writeLong(entry.getKey());
            out.writeInt(entry.getValue().size());
            for (JobTask jobTask : entry.getValue()) {
                jobTask.write(out);
            }
        }

    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            Long jobId = in.readLong();
            int taskSize = in.readInt();
            LinkedList<JobTask> jobTasks = new LinkedList<>();
            for (int j = 0; j < taskSize; j++) {
                JobTask jobTask = JobTask.readFields(in);
                jobTasks.add(jobTask);
            }
            jobTaskMap.put(jobId, jobTasks);
        }
    }
}
