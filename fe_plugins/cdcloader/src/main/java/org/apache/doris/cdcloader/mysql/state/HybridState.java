package org.apache.doris.cdcloader.mysql.state;

import org.apache.flink.cdc.connectors.mysql.source.assigners.state.SnapshotPendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplitState;

public class HybridState {

    private SnapshotPendingSplitsState splitsState;

    private MySqlBinlogSplitState binlogSplitState;

    public HybridState() {
    }

    public HybridState(SnapshotPendingSplitsState splitsState, MySqlBinlogSplitState binlogSplitState) {
        this.splitsState = splitsState;
        this.binlogSplitState = binlogSplitState;
    }

    public SnapshotPendingSplitsState getSplitsState() {
        return splitsState;
    }

    public MySqlBinlogSplitState getBinlogSplitState() {
        return binlogSplitState;
    }

    public void setSplitsState(SnapshotPendingSplitsState splitsState) {
        this.splitsState = splitsState;
    }

    public void setBinlogSplitState(MySqlBinlogSplitState binlogSplitState) {
        this.binlogSplitState = binlogSplitState;
    }
}
