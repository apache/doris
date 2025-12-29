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

package org.apache.doris.job.manager;

import org.apache.doris.job.extensions.insert.streaming.AbstractStreamingTask;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertTask;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

public class StreamingTaskManager {
    @Getter
    private final LinkedBlockingDeque<AbstractStreamingTask> needScheduleTasksQueue = new LinkedBlockingDeque<>();
    @Getter
    private List<AbstractStreamingTask> runningTasks = Collections.synchronizedList(new ArrayList<>());

    public void registerTask(AbstractStreamingTask task) {
        needScheduleTasksQueue.add(task);
    }

    public StreamingInsertTask getStreamingInsertTaskById(long taskId) {
        synchronized (runningTasks) {
            return (StreamingInsertTask) runningTasks.stream()
                    .filter(task -> task.getTaskId() == taskId)
                    .filter(task -> task instanceof StreamingInsertTask)
                    .findFirst()
                    .orElse(null);
        }
    }

    public void addRunningTask(AbstractStreamingTask task) {
        runningTasks.add(task);
    }

    public void removeRunningTask(AbstractStreamingTask task) {
        runningTasks.remove(task);
    }
}
