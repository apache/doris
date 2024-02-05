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

import lombok.experimental.UtilityClass;
import lombok.extern.log4j.Log4j2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * TaskTokenManager is responsible for managing semaphore tokens for different jobs.
 * It provides a method to acquire a semaphore token for a specific job ID with the given maximum concurrency.
 * If a semaphore doesn't exist for the job ID, it creates a new one and adds it to the map.
 */
@Log4j2
@UtilityClass
public class TaskTokenManager {

    private static final Map<Long, Semaphore> taskTokenMap = new ConcurrentHashMap<>(16);

    /**
     * Tries to acquire a semaphore token for the specified job ID with the given maximum concurrency.
     * If a semaphore doesn't exist for the job ID, it creates a new one and adds it to the map.
     *
     * @param jobId         the ID of the job
     * @param maxConcurrent the maximum concurrency for the job
     * @return the acquired semaphore
     */
    public static Semaphore tryAcquire(long jobId, long maxConcurrent) {
        Semaphore semaphore = taskTokenMap.computeIfAbsent(jobId, id -> new Semaphore((int) maxConcurrent));
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            log.warn("Interrupted while acquiring semaphore for job id: {} ", jobId, e);
            Thread.currentThread().interrupt();
        }
        return semaphore;
    }
}
