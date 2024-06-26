package org.apache.doris.cdcloader.mysql.rest.model;

import java.util.Map;

public class FinishSplitsReq {

    private String datasource;

    private Map<String, Map<String, String>> finishedOffsets;

    public FinishSplitsReq() {
    }

    public FinishSplitsReq(String datasource, Map<String, Map<String, String>> finishedOffsets) {
        this.datasource = datasource;
        this.finishedOffsets = finishedOffsets;
    }

    public String getDatasource() {
        return datasource;
    }

    public void setDatasource(String datasource) {
        this.datasource = datasource;
    }

    public Map<String, Map<String, String>> getFinishedOffsets() {
        return finishedOffsets;
    }

    public void setFinishedOffsets(Map<String, Map<String, String>> finishedOffsets) {
        this.finishedOffsets = finishedOffsets;
    }
}
