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

package org.apache.doris.task;

import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

public class AgentTaskExecutor {

    private static final ExecutorService EXECUTOR = ThreadPoolManager.newDaemonCacheThreadPoolThrowException(
            Config.max_agent_task_threads_num, "agent-task-pool", true);

    public AgentTaskExecutor() {
    }

    public static void submit(AgentBatchTask task) {
        if (task == null) {
            return;
        }
        try {
            EXECUTOR.submit(task);
        } catch (RejectedExecutionException e) {
            String msg = "Task is rejected, because the agent-task-pool is full, "
                    + "consider increasing the max_agent_task_threads_num config";
            for (AgentTask t : task.getAllTasks()) {
                // Skip the task if it is a resend type and already exists in the queue, because it will be
                // re-submit to the executor later.
                if (t.isNeedResendType() && AgentTaskQueue.contains(t)) {
                    continue;
                }
                t.failedWithMsg(msg);
            }
        }
    }

}
