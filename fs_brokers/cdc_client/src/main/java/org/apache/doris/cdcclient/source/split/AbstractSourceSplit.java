package org.apache.doris.cdcclient.source.split;

public abstract class AbstractSourceSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;
    protected String splitId;

    public AbstractSourceSplit() {}

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
