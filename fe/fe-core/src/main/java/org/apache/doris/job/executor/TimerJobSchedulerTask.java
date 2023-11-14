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

import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.disruptor.TaskDisruptor;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import jline.internal.Log;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TimerJobSchedulerTask<T extends AbstractJob<?>> implements TimerTask {

    private TaskDisruptor dispatchDisruptor;

    private final T job;

    public TimerJobSchedulerTask(TaskDisruptor dispatchDisruptor, T job) {
        this.dispatchDisruptor = dispatchDisruptor;
        this.job = job;
    }

    @Override
    public void run(Timeout timeout) {
        try {
            dispatchDisruptor.publishEvent(this.job);
        } catch (Exception e) {
            Log.warn("dispatch timer job error, task id is {}", this.job.getJobId(), e);
        }
    }
}
