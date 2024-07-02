package org.apache.doris.cdcloader.mysql.reader;

import java.util.List;
import java.util.Map;

public class RecordWithMeta {

    private Map<String, String> meta;

    private List<String> records;

    public RecordWithMeta(Map<String, String> meta, List<String> records) {
        this.meta = meta;
        this.records = records;
    }

    public Map<String, String> getMeta() {
        return meta;
    }

    public void setMeta(Map<String, String> meta) {
        this.meta = meta;
    }

    public List<String> getRecords() {
        return records;
    }

    public void setRecords(List<String> records) {
        this.records = records;
    }
}
