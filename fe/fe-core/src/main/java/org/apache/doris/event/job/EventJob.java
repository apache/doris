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

package org.apache.doris.event.job;

import org.apache.doris.event.constants.EventStatus;
import org.apache.doris.event.executor.EventJobExecutor;

import lombok.Data;

import java.util.UUID;

/**
 * EventJob is the core of the event module, which is used to store the event information of the event module.
 * We can use the eventJobId to uniquely identify an eventJob.
 * The eventJobName is used to identify the eventJob, which is not unique.
 * The eventStatus is used to identify the status of the eventJob, which is used to control the execution of the
 * eventJob.
 */
@Data
public class EventJob {

    public EventJob(String eventJobName, Long intervalMilliSeconds, Long startTimestamp, Long endTimestamp,
                    EventJobExecutor executor) {
        this.eventJobName = eventJobName;
        this.executor = executor;
        this.intervalMilliSeconds = intervalMilliSeconds;
        this.startTimestamp = null == startTimestamp ? 0L : startTimestamp;
        this.endTimestamp = null == endTimestamp ? 0L : endTimestamp;
    }

    private Long eventJobId = UUID.randomUUID().getMostSignificantBits();

    private String eventJobName;

    /**
     * The status of the eventJob, which is used to control the execution of the eventJob.
     *
     * @see EventStatus
     */
    private EventStatus eventStatus = EventStatus.RUNNING;

    /**
     * The executor of the eventJob, which is used to execute the eventJob.
     *
     * @see EventJobExecutor
     */
    private EventJobExecutor executor;

    private String user;

    private String errMsg;

    private Long intervalMilliSeconds;

    private Long updateTime;

    private Long nextExecuteTimestamp;


    private Long startTimestamp = 0L;

    private Long endTimestamp = 0L;

    private Long firstExecuteTimestamp = 0L;

    private Long latestStartExecuteTimestamp = 0L;

    public boolean isRunning() {
        return eventStatus == EventStatus.RUNNING;
    }

    public boolean isStopped() {
        return eventStatus == EventStatus.STOPPED;
    }

    public boolean isExpired(long nextExecuteTimestamp) {
        if (endTimestamp == 0L) {
            return false;
        }
        return nextExecuteTimestamp > endTimestamp;
    }

    public Long getExecuteTimestampAndGeneratorNext() {
        long executeTimestamp = nextExecuteTimestamp;
        this.latestStartExecuteTimestamp = executeTimestamp;
        this.nextExecuteTimestamp = System.currentTimeMillis() + intervalMilliSeconds;
        return executeTimestamp;
    }

    public void pause() {
        this.eventStatus = EventStatus.PAUSED;
    }

    public void pause(String errMsg) {
        this.eventStatus = EventStatus.PAUSED;
        this.errMsg = errMsg;
    }

    public void resume() {
        this.eventStatus = EventStatus.RUNNING;
    }

    public void stop() {
        this.eventStatus = EventStatus.STOPPED;
    }

    public boolean checkJobParam() {
        if (startTimestamp != 0L && startTimestamp < System.currentTimeMillis()) {
            return false;
        }
        if (endTimestamp != 0L && endTimestamp < System.currentTimeMillis()) {
            return false;
        }
        if (intervalMilliSeconds == null || intervalMilliSeconds <= 0L) {
            return false;
        }
        return null != executor;
    }
}
