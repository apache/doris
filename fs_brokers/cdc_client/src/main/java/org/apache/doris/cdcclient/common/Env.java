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

import org.apache.doris.cdcclient.model.JobConfig;
import org.apache.doris.cdcclient.source.factory.DataSource;
import org.apache.doris.cdcclient.source.factory.SourceReaderFactory;
import org.apache.doris.cdcclient.source.reader.SourceReader;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import lombok.Getter;
import lombok.Setter;

public class Env {
    private static volatile Env INSTANCE;
    private final Map<Long, JobContext> jobContexts;
    @Getter @Setter private String backendHostPort;

    private Env() {
        this.jobContexts = new ConcurrentHashMap<>();
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

    public SourceReader<?, ?> getReader(JobConfig jobConfig) {
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

    private SourceReader<?, ?> getOrCreateReader(
            Long jobId, DataSource dataSource, Map<String, String> config) {
        JobContext context = getOrCreateContext(jobId, dataSource, config);
        return context.getOrCreateReader(dataSource);
    }

    public void close(Long jobId) {
        JobContext context = jobContexts.remove(jobId);
        if (context != null) {
            context.close();
        }
    }

    private JobContext getOrCreateContext(
            Long jobId, DataSource dataSource, Map<String, String> config) {
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(dataSource, "dataSource");
        return jobContexts.computeIfAbsent(jobId, id -> new JobContext(id, dataSource, config));
    }

    private static final class JobContext {
        private final long jobId;
        private volatile SourceReader<?, ?> reader;
        private volatile Map<String, String> config;
        private volatile DataSource dataSource;

        private JobContext(long jobId, DataSource dataSource, Map<String, String> config) {
            this.jobId = jobId;
            this.dataSource = dataSource;
            this.config = config;
        }

        private synchronized SourceReader<?, ?> getOrCreateReader(DataSource source) {
            if (reader == null) {
                reader = SourceReaderFactory.createSourceReader(source);
                reader.initialize();
                dataSource = source;
            } else if (dataSource != source) {
                throw new IllegalStateException(
                        String.format(
                                "Job %d already bound to datasource %s, cannot switch to %s",
                                jobId, dataSource, source));
            }
            return reader;
        }

        private void close() {
            if (reader != null) {
                reader.close(jobId);
                reader = null;
            }
        }
    }
}
