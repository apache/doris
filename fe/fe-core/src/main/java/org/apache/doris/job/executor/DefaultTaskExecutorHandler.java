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

package org.apache.doris.job.executor;

import org.apache.doris.job.disruptor.ExecuteTaskEvent;
import org.apache.doris.job.task.AbstractTask;

import com.lmax.disruptor.WorkHandler;

/** Executes one task and releases its reusable Disruptor event. */
public class DefaultTaskExecutorHandler<T extends AbstractTask> implements WorkHandler<ExecuteTaskEvent<T>> {

    @Override
    public void onEvent(ExecuteTaskEvent<T> executeTaskEvent) {
        try {
            TaskProcessor.runTask(executeTaskEvent.getTask());
        } finally {
            executeTaskEvent.clear();
        }
    }
}
