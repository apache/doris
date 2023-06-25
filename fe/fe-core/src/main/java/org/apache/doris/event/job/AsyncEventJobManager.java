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

import org.apache.doris.event.disruptor.EventTaskDisruptor;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class AsyncEventJobManager implements Closeable {

    private final ConcurrentHashMap<Long, EventJob> eventJobMap = new ConcurrentHashMap<>(128);

    private long lastBatchSchedulerTimestamp;

    /**
     * batch scheduler interval time
     */
    private static final long BATCH_SCHEDULER_INTERVAL_MILLI_SECONDS = 10 * 60 * 1000L;

    private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;


    private boolean isClosed = false;

    /**
     * key: eventId
     * value: timeout list  for one event
     * it's used to cancel task, if task has started, it can't be canceled
     */
    private final ConcurrentHashMap<Long, Map<Long, Timeout>> eventTimeoutMap =
            new ConcurrentHashMap<>(128);

    /**
     * scheduler tasks, it's used to scheduler event job
     */
    private final HashedWheelTimer dorisTimer = new HashedWheelTimer(1, TimeUnit.SECONDS,
            660);

    /**
     * Producer and Consumer model
     * disruptor is used to handle event task
     * disruptor will start a thread pool to handle event task
     */
    private final EventTaskDisruptor disruptor;

    public AsyncEventJobManager() {
        dorisTimer.start();
        this.disruptor = new EventTaskDisruptor(this);
        batchSchedulerTasks();
        cycleSystemSchedulerTasks();
    }

    public Long registerEventJob(EventJob eventJob) {
        if (!eventJob.checkJobParam()) {
            log.warn("registerEventJob failed, eventJob: {} param is invalid", eventJob);
            return null;
        }
        if (eventJob.getStartTimestamp() != 0L) {
            eventJob.setNextExecuteTimestamp(eventJob.getStartTimestamp() + eventJob.getIntervalMilliSeconds());
        } else {
            eventJob.setNextExecuteTimestamp(System.currentTimeMillis() + eventJob.getIntervalMilliSeconds());
        }

        if (eventJob.getNextExecuteTimestamp() < BATCH_SCHEDULER_INTERVAL_MILLI_SECONDS + lastBatchSchedulerTimestamp) {
            List<Long> executeTimestamp = findEventsBetweenTime(System.currentTimeMillis(),
                    BATCH_SCHEDULER_INTERVAL_MILLI_SECONDS + lastBatchSchedulerTimestamp,
                    eventJob.getNextExecuteTimestamp(), eventJob.getIntervalMilliSeconds());
            if (!executeTimestamp.isEmpty()) {
                for (Long timestamp : executeTimestamp) {
                    putOneTask(eventJob.getEventJobId(), timestamp);
                    eventJob.setLatestStartExecuteTimestamp(timestamp);
                }
            }
        }

        eventJobMap.putIfAbsent(eventJob.getEventJobId(), eventJob);
        return eventJob.getEventJobId();
    }

    public void unregisterEventJob(Long eventJobId) {
        eventJobMap.remove(eventJobId);
    }

    public boolean pauseEventJob(Long eventJobId) {
        if (eventJobMap.get(eventJobId) == null) {
            log.warn("pauseEventJob failed, eventJobId: {} not exist", eventJobId);
            return false;
        }
        cancelAllEventTask(eventJobId);
        eventJobMap.get(eventJobId).pause();
        return true;
    }

    public boolean resumeEventJob(Long eventJobId) {
        if (eventJobMap.get(eventJobId) == null) {
            log.warn("resumeEventJob failed, eventJobId: {} not exist", eventJobId);
            return false;
        }
        eventJobMap.get(eventJobId).resume();
        return true;
    }

    public boolean stopEventJob(Long eventJobId) {
        if (eventJobMap.get(eventJobId) == null) {
            log.warn("stopEventJob failed, eventJobId: {} not exist", eventJobId);
            return false;
        }
        cancelAllEventTask(eventJobId);
        eventJobMap.get(eventJobId).stop();
        return true;
    }

    public EventJob getEventJob(Long eventJobId) {
        return eventJobMap.get(eventJobId);
    }

    public Map<Long, EventJob> getAllEventJob() {
        return eventJobMap;
    }

    public boolean batchSchedulerTasks() {
        executeJobIdsWithinLastTenMinutesWindow();
        return true;
    }

    public List<Long> findEventsBetweenTime(Long startTime, Long endTime, Long latestExecuteTime,
                                            Long intervalMilliSeconds) {
        List<Long> jobExecuteTimes = new ArrayList<>();
        while (latestExecuteTime < endTime) {
            if (latestExecuteTime >= startTime) {
                jobExecuteTimes.add(latestExecuteTime);
            }
            latestExecuteTime += intervalMilliSeconds;
        }
        return jobExecuteTimes;
    }

    /**
     * We will get the task in the next time window, and then hand it over to the time wheel for timing trigger
     */
    private void executeJobIdsWithinLastTenMinutesWindow() {
        this.lastBatchSchedulerTimestamp = System.currentTimeMillis();
        if (eventJobMap.isEmpty()) {
            return;
        }
        eventJobMap.forEach((k, v) -> {
            if (v.isRunning() && (v.getLatestStartExecuteTimestamp() + v.getIntervalMilliSeconds()
                    < lastBatchSchedulerTimestamp + BATCH_SCHEDULER_INTERVAL_MILLI_SECONDS)) {
                List<Long> executeTimes = findEventsBetweenTime(lastBatchSchedulerTimestamp,
                        lastBatchSchedulerTimestamp + BATCH_SCHEDULER_INTERVAL_MILLI_SECONDS,
                        v.getLatestStartExecuteTimestamp(), v.getIntervalMilliSeconds());
                if (!executeTimes.isEmpty()) {
                    for (Long executeTime : executeTimes) {
                        putOneTask(v.getEventJobId(), executeTime);
                        v.setLatestStartExecuteTimestamp(executeTime);
                    }
                }
            }
        });
    }

    /**
     * We will cycle system scheduler tasks every 10 minutes.
     * Events will be re-registered after the task is completed
     */
    private void cycleSystemSchedulerTasks() {
        dorisTimer.newTimeout(timeout -> {
            batchSchedulerTasks();
            cycleSystemSchedulerTasks();
        }, BATCH_SCHEDULER_INTERVAL_MILLI_SECONDS, TimeUnit.MILLISECONDS);
    }

    public void putOneTask(Long eventJobId, Long startExecuteTime) {
        DorisTimerTask task = new DorisTimerTask(eventJobId, startExecuteTime, disruptor);
        if (isClosed) {
            log.info("putOneTask failed, scheduler is closed, eventId: {}", task.getEventId());
            return;
        }
        long delay = getDelay(task.getStartTimestamp());
        Timeout timeout = dorisTimer.newTimeout(task, delay, TIME_UNIT);
        if (timeout == null) {
            log.error("putOneTask failed, eventId: {}", task.getEventId());
            return;
        }
        if (eventTimeoutMap.containsKey(task.getEventId())) {
            eventTimeoutMap.get(task.getEventId()).put(task.getTaskId(), timeout);
            return;
        }
        Map<Long, Timeout> timeoutMap = new ConcurrentHashMap<>();
        timeoutMap.put(task.getTaskId(), timeout);
        eventTimeoutMap.put(task.getEventId(), timeoutMap);
    }

    // cancel all task for one event
    // if task has started, it can't be canceled
    public void cancelAllEventTask(Long eventId) {
        if (!eventTimeoutMap.containsKey(eventId)) {
            return;
        }

        eventTimeoutMap.get(eventId).values().forEach(timeout -> {
            if (!timeout.isExpired() || timeout.isCancelled()) {
                timeout.cancel();
            }
        });
    }

    public void stopEventTask(Long eventId, Long taskId) {
        if (!eventTimeoutMap.containsKey(eventId)) {
            return;
        }
        eventTimeoutMap.get(eventId).remove(taskId);
    }

    // get delay time, if startTimestamp is less than now, return 0
    private long getDelay(long startTimestamp) {
        long delay = 0;
        long now = System.currentTimeMillis();
        if (startTimestamp > now) {
            delay = startTimestamp - now;
        } else {
            log.warn("startTimestamp is less than now, startTimestamp: {}, now: {}", startTimestamp, now);
        }
        return delay;
    }

    @Override
    public void close() throws IOException {
        isClosed = true;
        dorisTimer.stop();
        disruptor.close();
    }
}
