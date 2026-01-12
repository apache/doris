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

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Preconditions;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Env {
    private static final Logger LOG = LoggerFactory.getLogger(Env.class);
    private static volatile Env INSTANCE;
    private final Map<Long, JobContext> jobContexts;
    private final Map<Long, Lock> jobLocks;
    @Setter private int backendHttpPort;

    private Env() {
        this.jobContexts = new ConcurrentHashMap<>();
        this.jobLocks = new ConcurrentHashMap<>();
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
        DataSource ds = resolveDataSource(jobConfig.getDataSource());
        Env manager = Env.getCurrentEnv();
        return manager.getOrCreateReader(jobConfig.getJobId(), ds, jobConfig.getConfig());
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
            Long jobId, DataSource dataSource, Map<String, String> config) {
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(dataSource, "dataSource");
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

    public void close(Long jobId) {
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

    private static final class JobContext {
        private final long jobId;
        private volatile SourceReader reader;
        private volatile Map<String, String> config;
        private volatile DataSource dataSource;

        private JobContext(long jobId, DataSource dataSource, Map<String, String> config) {
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
                                "Job %d already bound to datasource %s, cannot switch to %s",
                                jobId, this.dataSource, source));
            }
            Preconditions.checkState(reader != null, "Job %d reader not initialized yet", jobId);
            return reader;
        }
    }
}
