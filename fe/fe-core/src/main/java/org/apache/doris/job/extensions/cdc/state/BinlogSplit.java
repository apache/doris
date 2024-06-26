package org.apache.doris.job.extensions.cdc.state;

import com.google.gson.annotations.SerializedName;

import java.util.Map;

public class BinlogSplit extends AbstractSourceSplit {
    @SerializedName("offset")
    private Map<String, String> offset;

    public BinlogSplit() {
    }

    public BinlogSplit(String splitId, Map<String, String> offset) {
        super(splitId);
        this.offset = offset;
    }

    public Map<String, String> getOffset() {
        return offset;
    }

    public void setOffset(Map<String, String> offset) {
        this.offset = offset;
    }
}
