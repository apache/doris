package org.apache.doris.cdcclient.source.split;

import java.util.Map;

public class BinlogSplit extends AbstractSourceSplit {
    private static final long serialVersionUID = 1L;
    private Map<String, String> offset;

    public BinlogSplit() {}

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

    @Override
    public String toString() {
        return "BinlogSplit{" + "offset=" + offset + ", splitId='" + splitId + '\'' + '}';
    }
}
