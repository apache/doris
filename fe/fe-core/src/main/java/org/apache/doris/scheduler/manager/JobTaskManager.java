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

package org.apache.doris.scheduler.manager;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.scheduler.job.JobTask;

import lombok.extern.slf4j.Slf4j;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
public class JobTaskManager implements Writable {

    private static final Integer TASK_MAX_NUM = Config.scheduler_job_task_max_num;

    private ConcurrentHashMap<Long, ConcurrentLinkedQueue<JobTask>> jobTaskMap = new ConcurrentHashMap<>(16);

    public void addJobTask(JobTask jobTask) {
        ConcurrentLinkedQueue<JobTask> jobTasks = jobTaskMap
                .computeIfAbsent(jobTask.getJobId(), k -> new ConcurrentLinkedQueue<>());
        jobTasks.add(jobTask);
        if (jobTasks.size() > TASK_MAX_NUM) {
            JobTask oldTask = jobTasks.poll();
            Env.getCurrentEnv().getEditLog().logDeleteJobTask(oldTask);
        }
        Env.getCurrentEnv().getEditLog().logCreateJobTask(jobTask);
    }

    public List<JobTask> getJobTasks(Long jobId) {
        if (jobTaskMap.containsKey(jobId)) {
            ConcurrentLinkedQueue<JobTask> jobTasks = jobTaskMap.get(jobId);
            List<JobTask> jobTaskList = new LinkedList<>(jobTasks);
            Collections.reverse(jobTaskList);
            return jobTaskList;
        }
        return new ArrayList<>();
    }

    public void replayCreateTask(JobTask task) {
        ConcurrentLinkedQueue<JobTask> jobTasks = jobTaskMap
                .computeIfAbsent(task.getJobId(), k -> new ConcurrentLinkedQueue<>());
        jobTasks.add(task);
        log.info(new LogBuilder(LogKey.SCHEDULER_TASK, task.getTaskId())
                .add("msg", "replay create scheduler task").build());
    }

    public void replayDeleteTask(JobTask task) {
        ConcurrentLinkedQueue<JobTask> jobTasks = jobTaskMap.get(task.getJobId());
        if (jobTasks != null) {
            jobTasks.remove(task);
        }
        log.info(new LogBuilder(LogKey.SCHEDULER_TASK, task.getTaskId())
                .add("msg", "replay delete scheduler task").build());
    }

    public void deleteJobTasks(Long jobId) {
        ConcurrentLinkedQueue<JobTask> jobTasks = jobTaskMap.get(jobId);
        if (jobTasks != null) {
            JobTask jobTask = jobTasks.poll();
            log.info(new LogBuilder(LogKey.SCHEDULER_TASK, jobTask.getTaskId())
                    .add("msg", "replay delete scheduler task").build());
        }
        jobTaskMap.remove(jobId);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(jobTaskMap.size());
        for (Map.Entry<Long, ConcurrentLinkedQueue<JobTask>> entry : jobTaskMap.entrySet()) {
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
            ConcurrentLinkedQueue<JobTask> jobTasks = new ConcurrentLinkedQueue<>();
            for (int j = 0; j < taskSize; j++) {
                JobTask jobTask = JobTask.readFields(in);
                jobTasks.add(jobTask);
            }
            jobTaskMap.put(jobId, jobTasks);
        }
    }
}
