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

package org.apache.doris.manager.agent.task;

import org.apache.doris.manager.agent.common.AgentConstants;

public class TaskContext {
    private static final LRU<String, Task> TASK_LRU = new LRU<>(AgentConstants.COMMAND_HISTORY_SAVE_MAX_COUNT);

    public static synchronized void register(Task task) {
        TASK_LRU.put(task.getTaskId(), task);
    }

    public static synchronized void unregister(Task task) {
        TASK_LRU.remove(task.getTaskId());
    }

    public static synchronized Task getTaskByTaskId(String taskId) {
        return TASK_LRU.get(taskId);
    }
}
