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

package org.apache.doris.job;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;

import java.util.Collections;
import java.util.Map;

public abstract class AsyncJob {

    public enum JobStatus {
        INIT, PENDING, RUNNING, FINISHED, CANCELED
    }

    public static final String RESOURCE_QUEUE = "resource_queue";

    // jobId
    protected final long id;
    protected final String label;
    // job user
    protected final UserIdentity userIdentity;
    protected final Map<String, String> properties;
    protected JobStatus jobStatus = JobStatus.INIT;

    public AsyncJob(String label, UserIdentity userIdentity, Map<String, String> properties) {
        id = Env.getCurrentEnv().getNextId();
        this.label = label;
        this.userIdentity = userIdentity;
        this.properties = properties == null ? Collections.emptyMap() : properties;
    }

    public abstract void run();

    public abstract void cancel();

    public long jobId() {
        return id;
    }

    public UserIdentity getUser() {
        return userIdentity;
    }

    public String getProperty(String key, String defaultVal) {
        return properties.getOrDefault(key, defaultVal);
    }

    public String getResourceQueue() {
        return properties.get(RESOURCE_QUEUE);
    }

    public String getLabel() {
        return label;
    }

    public JobStatus getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(JobStatus jobStatus) {
        this.jobStatus = jobStatus;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }
        if (other instanceof AsyncJob) {
            return id == ((AsyncJob) other).id;
        }
        return false;
    }
}
