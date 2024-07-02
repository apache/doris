package org.apache.doris.cdcloader.mysql.rest.model;

import java.util.Map;

public class JobConfig {

    private Long jobId;
    private Map<String, String> config;

    public JobConfig() {
    }

    public JobConfig(Long jobId, Map<String, String> config) {
        this.jobId = jobId;
        this.config = config;
    }

    public Long getJobId() {
        return jobId;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }
}
