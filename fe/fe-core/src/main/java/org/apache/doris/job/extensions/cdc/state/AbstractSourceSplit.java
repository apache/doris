package org.apache.doris.job.extensions.cdc.state;

import com.google.gson.annotations.SerializedName;

public class AbstractSourceSplit implements SourceSplit {

    @SerializedName("sid")
    protected String splitId;

    public AbstractSourceSplit() {
    }

    public AbstractSourceSplit(String splitId) {
        this.splitId = splitId;
    }

    public String getSplitId() {
        return splitId;
    }

    public void setSplitId(String splitId) {
        this.splitId = splitId;
    }
}
