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

package org.apache.doris.cdcclient.common;

import org.apache.doris.cdcclient.source.factory.DataSource;
import org.apache.doris.cdcclient.source.factory.SourceReaderFactory;
import org.apache.doris.cdcclient.source.reader.SourceReader;
import org.apache.doris.job.cdc.request.JobBaseConfig;
import org.apache.doris.job.cdc.request.WriteRecordRequest;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Env {
    private static final Logger LOG = LoggerFactory.getLogger(Env.class);
    private static volatile Env INSTANCE;
    private final Map<String, JobContext> jobContexts;
    private final Map<String, Lock> jobLocks;
    private final ScheduledExecutorService idleReaderScheduler;
    @Setter private int backendHttpPort;
    @Setter @Getter private String clusterToken;
    @Setter @Getter private volatile String feMasterAddress;

    private Env() {
        this.jobContexts = new ConcurrentHashMap<>();
        this.jobLocks = new ConcurrentHashMap<>();
        this.idleReaderScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "cdc-idle-reader-cleaner");
                            t.setDaemon(true);
                            return t;
                        });
        this.idleReaderScheduler.scheduleWithFixedDelay(
                this::releaseIdleReaders,
                Constants.IDLE_READER_SCAN_INTERVAL_MS,
                Constants.IDLE_READER_SCAN_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
    }

    public String getBackendHostPort() {
        return "127.0.0.1:" + backendHttpPort;
    }

    public static Env getCurrentEnv() {
        if (INSTANCE == null) {
            synchronized (Env.class) {
                if (INSTANCE == null) {
                    INSTANCE = new Env();
                }
            }
        }
        return INSTANCE;
    }

    public SourceReader getReader(JobBaseConfig jobConfig) {
        if (jobConfig.getFrontendAddress() != null && !jobConfig.getFrontendAddress().isEmpty()) {
            this.feMasterAddress = jobConfig.getFrontendAddress();
        }
        DataSource ds = resolveDataSource(jobConfig.getDataSource());
        Env manager = Env.getCurrentEnv();
        return manager.getOrCreateReader(jobConfig.getJobId(), ds, jobConfig.getConfig());
    }

    /** Return the reader only if already created, else null (never creates one). */
    public SourceReader getReaderIfPresent(String jobId) {
        JobContext context = jobContexts.get(jobId);
        return context == null ? null : context.reader;
    }

    /**
     * Reader for stateless metadata ops (end offset / compare): reuse the live one if present, else
     * a throwaway instance. Never create/cache/initialize a heavy reader, so a metadata RPC for an
     * idle/absent job can't trigger pub/slot/schema (re)initialization or leak an unreaped context.
     */
    public SourceReader getMetaReader(JobBaseConfig jobConfig) {
        SourceReader existing = getReaderIfPresent(jobConfig.getJobId());
        if (existing != null) {
            return existing;
        }
        return SourceReaderFactory.createSourceReader(resolveDataSource(jobConfig.getDataSource()));
    }

    /**
     * Get-or-create this job's reader and claim ownership for {@code taskId} atomically under the
     * per-job lock, so a concurrent stale release cannot stop a reader this task is about to use.
     */
    public SourceReader getReaderAndClaim(JobBaseConfig jobConfig, String taskId) {
        if (jobConfig.getFrontendAddress() != null && !jobConfig.getFrontendAddress().isEmpty()) {
            this.feMasterAddress = jobConfig.getFrontendAddress();
        }
        DataSource ds = resolveDataSource(jobConfig.getDataSource());
        String jobId = jobConfig.getJobId();
        Lock lock = jobLocks.computeIfAbsent(jobId, k -> new ReentrantLock());
        SourceReader staleReader = null;
        JobBaseConfig staleConfig = null;
        SourceReader reader;
        lock.lock();
        try {
            JobContext context = jobContexts.get(jobId);
            if (context != null
                    && jobConfig instanceof WriteRecordRequest
                    && ((WriteRecordRequest) jobConfig).isRebuildReader()) {
                // FE declared the previous task abnormal: swap in a fresh reader instance so the
                // old task's thread can never reach the new fetcher.
                LOG.info(
                        "Rebuild reader for job {} on FE request, discard current instance", jobId);
                jobContexts.remove(jobId);
                staleReader = context.reader;
                staleConfig = context.jobConfig != null ? context.jobConfig : jobConfig;
                context = null;
            }
            if (context == null) {
                LOG.info("Creating new reader for job {}, dataSource {}", jobId, ds);
                context = new JobContext(jobId, ds, jobConfig.getConfig());
                context.initializeReader();
                jobContexts.put(jobId, context);
            }
            context.ownerTaskId = taskId;
            context.jobConfig = jobConfig;
            if (jobConfig instanceof WriteRecordRequest) {
                context.maxIntervalMs = ((WriteRecordRequest) jobConfig).getMaxInterval() * 1000;
            }
            context.lastAliveTime = System.currentTimeMillis();
            reader = context.getReader(ds);
        } finally {
            lock.unlock();
        }
        if (staleReader != null) {
            // free the engine/slot connection before the caller submits the new fetcher
            try {
                staleReader.release(staleConfig);
            } catch (Exception ex) {
                LOG.warn("Failed to release stale reader for job {}", jobId, ex);
            }
        }
        return reader;
    }

    /** Whether {@code taskId} is still the current claimer of this job's reader. */
    public boolean isOwner(String jobId, String taskId) {
        JobContext context = jobContexts.get(jobId);
        return context != null && Objects.equals(context.ownerTaskId, taskId);
    }

    /**
     * If {@code taskId} still owns the reader, remove the context under the per-job lock and return
     * the reader to release; else null (stale release -> no-op). Removing under the lock guarantees
     * a racing {@link #getReaderAndClaim} either sees the new owner (no-op) or rebuilds a fresh
     * one.
     */
    public SourceReader detachReaderIfOwner(String jobId, String taskId) {
        Lock lock = jobLocks.get(jobId);
        if (lock == null) {
            return null;
        }
        lock.lock();
        try {
            JobContext context = jobContexts.get(jobId);
            if (context == null || !Objects.equals(context.ownerTaskId, taskId)) {
                if (context != null) {
                    LOG.info(
                            "Stale release for job {} task {} (owner {}), skip",
                            jobId,
                            taskId,
                            context.ownerTaskId);
                }
                return null;
            }
            jobContexts.remove(jobId);
            return context.reader;
        } finally {
            lock.unlock();
        }
    }

    private DataSource resolveDataSource(String source) {
        if (source == null || source.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing dataSource");
        }
        try {
            return DataSource.valueOf(source.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Unsupported dataSource: " + source, ex);
        }
    }

    private SourceReader getOrCreateReader(
            String jobId, DataSource dataSource, Map<String, String> config) {
        Objects.requireNonNull(jobId, "jobId is null");
        Objects.requireNonNull(dataSource, "dataSource is null");
        JobContext context = jobContexts.get(jobId);
        if (context != null) {
            return context.getReader(dataSource);
        }

        Lock lock = jobLocks.computeIfAbsent(jobId, k -> new ReentrantLock());
        lock.lock();
        try {
            // double check
            context = jobContexts.get(jobId);
            if (context != null) {
                return context.getReader(dataSource);
            }

            LOG.info("Creating new reader for job {}, dataSource {}", jobId, dataSource);
            context = new JobContext(jobId, dataSource, config);
            SourceReader reader = context.initializeReader();
            jobContexts.put(jobId, context);
            return reader;
        } finally {
            lock.unlock();
        }
    }

    public void close(String jobId) {
        Lock lock = jobLocks.get(jobId);
        if (lock != null) {
            lock.lock();
            try {
                jobContexts.remove(jobId);
                jobLocks.remove(jobId);
            } finally {
                lock.unlock();
            }
        } else {
            // should not happen
            jobContexts.remove(jobId);
        }
    }

    /** Liveness evidence (FE heartbeat or active poll): keep this job's reader alive. */
    public void keepAlive(String jobId) {
        JobContext context = jobContexts.get(jobId);
        if (context != null) {
            context.lastAliveTime = System.currentTimeMillis();
        }
    }

    // Release (keep slot) readers FE no longer drives; maxIntervalMs<=0 = untracked (e.g. TVF),
    // skip.
    private void releaseIdleReaders() {
        long now = System.currentTimeMillis();
        for (String jobId : jobContexts.keySet()) {
            Lock lock = jobLocks.get(jobId);
            if (lock == null || !lock.tryLock()) {
                continue;
            }
            SourceReader toRelease = null;
            JobBaseConfig releaseConfig = null;
            try {
                JobContext context = jobContexts.get(jobId);
                if (context == null || context.lastAliveTime <= 0 || context.maxIntervalMs <= 0) {
                    continue;
                }
                long timeout =
                        Math.max(
                                (long) Constants.IDLE_READER_TIMEOUT_MULTIPLIER
                                        * context.maxIntervalMs,
                                Constants.IDLE_READER_MIN_TIMEOUT_MS);
                if (now - context.lastAliveTime <= timeout) {
                    continue;
                }
                LOG.info(
                        "Releasing idle reader for job {}, idle {} ms, keep slot",
                        jobId,
                        now - context.lastAliveTime);
                jobContexts.remove(jobId);
                toRelease = context.reader;
                releaseConfig = context.jobConfig;
            } finally {
                lock.unlock();
            }
            // Release outside the lock so blocking IO never stalls getReaderAndClaim/detach.
            if (toRelease != null && releaseConfig != null) {
                try {
                    toRelease.release(releaseConfig);
                } catch (Exception ex) {
                    LOG.warn("Failed to release idle reader for job {}", jobId, ex);
                }
            }
        }
    }

    private static final class JobContext {
        private final String jobId;
        private volatile SourceReader reader;
        private volatile String ownerTaskId;
        private volatile Map<String, String> config;
        private volatile DataSource dataSource;
        private volatile JobBaseConfig jobConfig;
        private volatile long maxIntervalMs;
        private volatile long lastAliveTime;

        private JobContext(String jobId, DataSource dataSource, Map<String, String> config) {
            this.jobId = jobId;
            this.dataSource = dataSource;
            this.config = config;
        }

        private SourceReader initializeReader() {
            SourceReader newReader = SourceReaderFactory.createSourceReader(dataSource);
            newReader.initialize(jobId, dataSource, config);
            this.reader = newReader;
            return reader;
        }

        private SourceReader getReader(DataSource source) {
            if (this.dataSource != source) {
                throw new IllegalStateException(
                        String.format(
                                "Job %s already bound to datasource %s, cannot switch to %s",
                                jobId, this.dataSource, source));
            }
            Preconditions.checkState(reader != null, "Job %s reader not initialized yet", jobId);
            return reader;
        }
    }
}
