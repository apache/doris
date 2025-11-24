package org.apache.doris.cdcclient.source.split;

import java.util.Map;

public class SnapshotSplit extends AbstractSourceSplit {
    private static final long serialVersionUID = 1L;
    private String tableId;
    private String splitKey;
    private String splitStart;
    private String splitEnd;
    private Map<String, String> highWatermark;

    public SnapshotSplit() {
        super();
    }

    public SnapshotSplit(
            String splitId,
            String tableId,
            String splitKey,
            String splitStart,
            String splitEnd,
            Map<String, String> highWatermark) {
        super(splitId);
        this.tableId = tableId;
        this.splitKey = splitKey;
        this.splitStart = splitStart;
        this.splitEnd = splitEnd;
        this.highWatermark = highWatermark;
    }

    public String getTableId() {
        return tableId;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public String getSplitKey() {
        return splitKey;
    }

    public void setSplitKey(String splitKey) {
        this.splitKey = splitKey;
    }

    public String getSplitStart() {
        return splitStart;
    }

    public void setSplitStart(String splitStart) {
        this.splitStart = splitStart;
    }

    public String getSplitEnd() {
        return splitEnd;
    }

    public void setSplitEnd(String splitEnd) {
        this.splitEnd = splitEnd;
    }

    public Map<String, String> getHighWatermark() {
        return highWatermark;
    }

    public void setHighWatermark(Map<String, String> highWatermark) {
        this.highWatermark = highWatermark;
    }

    @Override
    public String toString() {
        return "SnapshotSplit{"
                + "tableId='"
                + tableId
                + '\''
                + ", splitKey='"
                + splitKey
                + '\''
                + ", splitStart='"
                + splitStart
                + '\''
                + ", splitEnd='"
                + splitEnd
                + '\''
                + ", highWatermark="
                + highWatermark
                + ", splitId='"
                + splitId
                + '\''
                + '}';
    }
}
