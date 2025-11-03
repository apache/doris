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

package org.apache.doris.jdbc;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class JdbcDataSource {
    private static final Logger LOG = Logger.getLogger(JdbcDataSource.class);
    private static final JdbcDataSource jdbcDataSource = new JdbcDataSource();
    private final Map<String, HikariDataSource> sourcesMap = new ConcurrentHashMap<>();
    private final Map<String, Long> lastAccessTimeMap = new ConcurrentHashMap<>();
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private long cleanupInterval = 8 * 60 * 60 * 1000; // 8 hours
    private ScheduledFuture<?> cleanupTask = null;

    private JdbcDataSource() {
        startCleanupTask();
    }

    public static JdbcDataSource getDataSource() {
        return jdbcDataSource;
    }

    public HikariDataSource getSource(String cacheKey) {
        lastAccessTimeMap.put(cacheKey, System.currentTimeMillis());
        return sourcesMap.get(cacheKey);
    }

    public void putSource(String cacheKey, HikariDataSource ds) {
        sourcesMap.put(cacheKey, ds);
        lastAccessTimeMap.put(cacheKey, System.currentTimeMillis());
    }

    public Map<String, HikariDataSource> getSourcesMap() {
        return sourcesMap;
    }

    public void setCleanupInterval(long interval) {
        if (this.cleanupInterval != interval * 1000L) {
            this.cleanupInterval = interval * 1000L;
            restartCleanupTask();
        }
    }

    private synchronized void restartCleanupTask() {
        if (cleanupTask != null && !cleanupTask.isCancelled()) {
            cleanupTask.cancel(false);
        }
        cleanupTask = executor.scheduleAtFixedRate(() -> {
            try {
                long now = System.currentTimeMillis();
                lastAccessTimeMap.forEach((key, lastAccessTime) -> {
                    if (now - lastAccessTime > cleanupInterval) {
                        HikariDataSource ds = sourcesMap.remove(key);
                        if (ds != null) {
                            ds.close();
                        }
                        lastAccessTimeMap.remove(key);
                        LOG.info("remove jdbc data source: " + key.split("jdbc")[0]);
                    }
                });
            } catch (Exception e) {
                LOG.error("failed to cleanup jdbc data source", e);
            }
        }, cleanupInterval, cleanupInterval, TimeUnit.MILLISECONDS);
    }

    private void startCleanupTask() {
        if (cleanupTask == null || cleanupTask.isCancelled()) {
            restartCleanupTask();
        }
    }
}
