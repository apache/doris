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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 负责管理以 jobId 为粒度的 SourceReader 及其配置，避免 Controller 手动维护状态。
 */
public class JobManager {
    private static volatile JobManager INSTANCE;
    private final Map<Long, JobContext> jobContexts;

    private JobManager() {
        this.jobContexts = new ConcurrentHashMap<>();
    }

    public static JobManager getInstance() {
        if (INSTANCE == null) {
            synchronized (JobManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new JobManager();
                }
            }
        }
        return INSTANCE;
    }


    public SourceReader<?, ?> getOrCreateReader(Long jobId, DataSource dataSource) {
        JobContext context = getOrCreateContext(jobId, dataSource);
        return context.getOrCreateReader(dataSource);
    }

    public SourceReader<?, ?> getReader(Long jobId) {
        JobContext context = jobContexts.get(jobId);
        return context == null ? null : context.reader;
    }

    public void attachOptions(Long jobId, DataSource dataSource, Map<String, String> options) {
        if (jobId == null || options == null) {
            return;
        }
        JobContext context = getOrCreateContext(jobId, dataSource);
        context.setOptions(options);
    }

    public Map<String, String> getOptions(Long jobId) {
        JobContext context = jobContexts.get(jobId);
        return context == null ? null : context.options;
    }

    public void close(Long jobId) {
        JobContext context = jobContexts.remove(jobId);
        if (context != null) {
            context.close();
        }
    }

    public void clearAll() {
        jobContexts.keySet().forEach(this::close);
    }

    private JobContext getOrCreateContext(Long jobId, DataSource dataSource) {
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(dataSource, "dataSource");
        return jobContexts.computeIfAbsent(jobId, id -> new JobContext(id, dataSource));
    }

    private static final class JobContext {
        private final long jobId;
        private volatile SourceReader<?, ?> reader;
        private volatile Map<String, String> options;
        private volatile DataSource dataSource;

        private JobContext(long jobId, DataSource dataSource) {
            this.jobId = jobId;
            this.dataSource = dataSource;
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

        private void setOptions(Map<String, String> options) {
            this.options = options;
        }

        private void close() {
            if (reader != null) {
                reader.close(jobId);
                reader = null;
            }
        }
    }
}
