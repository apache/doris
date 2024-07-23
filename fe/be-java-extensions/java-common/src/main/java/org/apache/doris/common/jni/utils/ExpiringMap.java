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

package org.apache.doris.common.jni.utils;

import org.apache.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ExpiringMap<K, V> {
    private final ConcurrentHashMap<K, V> map = new ConcurrentHashMap<>(); // key --> value
    private final ConcurrentHashMap<K, Long> ttlMap = new ConcurrentHashMap<>(); // key --> ttl interval
    // key --> expirationTime(ttl interval + currentTimeMillis)
    private final ConcurrentHashMap<K, Long> expirationMap = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static final long DEFAULT_INTERVAL_TIME = 10 * 60 * 1000L; // 10 minutes
    public static final Logger LOG = Logger.getLogger(ExpiringMap.class);

    public ExpiringMap() {
        startExpirationTask();
    }

    public void put(K key, V value, long expirationTimeMs) {
        long expirationTime = System.currentTimeMillis() + expirationTimeMs;
        map.put(key, value);
        expirationMap.put(key, expirationTime);
        ttlMap.put(key, expirationTimeMs);
    }

    public V get(K key) {
        Long expirationTime = expirationMap.get(key);
        if (expirationTime == null || System.currentTimeMillis() > expirationTime) {
            map.remove(key);
            expirationMap.remove(key);
            ttlMap.remove(key);
            return null;
        }
        // reset time again
        long ttl = ttlMap.get(key);
        long newExpirationTime = System.currentTimeMillis() + ttl;
        expirationMap.put(key, newExpirationTime);
        return map.get(key);
    }

    private void startExpirationTask() {
        scheduler.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            for (K key : expirationMap.keySet()) {
                if (expirationMap.get(key) <= now) {
                    map.remove(key);
                    expirationMap.remove(key);
                    ttlMap.remove(key);
                }
            }
        }, DEFAULT_INTERVAL_TIME, DEFAULT_INTERVAL_TIME, TimeUnit.MINUTES);
    }

    public void remove(K key) {
        map.remove(key);
        expirationMap.remove(key);
        ttlMap.remove(key);
    }

    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
    }
}
