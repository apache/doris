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

package org.apache.doris.event;

import org.apache.doris.event.executor.EventJobExecutor;
import org.apache.doris.event.job.EventJob;
import org.apache.doris.event.job.EventJobManager;
import org.apache.doris.event.registry.EventJobSchedulerRegister;

import lombok.extern.slf4j.Slf4j;

/**
 * This class registers timed scheduling events using the Netty time wheel algorithm to trigger events in a timely
 * manner.
 * After the event is triggered, it is produced by the Disruptor producer and consumed by the consumer, which is an
 * asynchronous
 * consumption model that does not guarantee strict timing accuracy.
 */
@Slf4j
public class AsyncEventJobSchedulerRegister implements EventJobSchedulerRegister {

    private final EventJobManager eventJobManager;

    public AsyncEventJobSchedulerRegister() {
        this.eventJobManager = new EventJobManager();
    }

    @Override
    public Long registerEventJob(String name, Long intervalMs, EventJobExecutor executor) {
        return this.registerEventJob(name, intervalMs, null, null, executor);
    }

    @Override
    public Long registerEventJob(String name, Long intervalMs, Long startTimeStamp, EventJobExecutor executor) {
        return this.registerEventJob(name, intervalMs, startTimeStamp, null, executor);
    }

    @Override
    public Long registerEventJob(String name, Long intervalMs, Long startTimeStamp, Long endTimeStamp,
                                 EventJobExecutor executor) {

        EventJob eventJob = new EventJob(name, intervalMs, startTimeStamp, endTimeStamp, executor);
        return eventJobManager.registerEventJob(eventJob);
    }

    @Override
    public Boolean pauseEventJob(Long eventId) {
        return eventJobManager.pauseEventJob(eventId);
    }

    @Override
    public Boolean stopEventJob(Long eventId) {
        return eventJobManager.stopEventJob(eventId);
    }

    @Override
    public Boolean resumeEventJob(Long eventId) {
        return eventJobManager.resumeEventJob(eventId);
    }
}
