package org.apache.doris.cdcloader.mysql.service;

import org.apache.doris.cdcloader.mysql.loader.LoadContext;
import org.apache.doris.cdcloader.mysql.state.HybridState;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.SnapshotPendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplitState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner.BINLOG_SPLIT_ID;

public class JobService {

    private LoadContext context;

    public JobService() {
        this.context = LoadContext.getInstance();
    }


    public void reportChunkSplitState(SnapshotPendingSplitsState splitsState) {
        System.out.println("report snapshot state to fe:" + splitsState);
        //Serialize into the format required by fe
    }

    public void reportBinlogOffsetState(MySqlBinlogSplitState splitsState) {
        System.out.println("report binlog state to fe:" + splitsState);
        //Serialize into the format required by fe
    }

    public HybridState getLastConsumedState(){
        //Deserialize into the format required by jar
//        HybridPendingSplitsState state = new HybridPendingSplitsState(null, true);
//        MySqlBinlogSplitState binlogSplitState = new MySqlBinlogSplitState(null);
//        HybridState hybridState = new HybridState(state, binlogSplitState);

//        MySqlBinlogSplitState mySqlBinlogSplitState = mockBinlogSplitState();
//        HybridState hybridState = new HybridState(null, mySqlBinlogSplitState);
//        SnapshotPendingSplitsState state = mockSplitState();
//        HybridState hybridState = new HybridState(state, null);
//        return hybridState;
        return null;
    }

    private static MySqlBinlogSplitState mockBinlogSplitState(){
        Map<String, String> startOffset = new HashMap<>();
        startOffset.put("file", "binlog.000040");
        startOffset.put("pos", "4821");
        startOffset.put("kind", "SPECIFIC");
        startOffset.put("ts_sec", "0");

        MySqlBinlogSplit binlogSplit = new MySqlBinlogSplit(BINLOG_SPLIT_ID, new BinlogOffset(startOffset), BinlogOffset.ofNonStopping(), new ArrayList<>(), new HashMap<>(), 0);
        return new MySqlBinlogSplitState(binlogSplit);
    }

    private static SnapshotPendingSplitsState mockSplitState(){
//        MySQLCdcLoader2 cdcLoader = new MySQLCdcLoader2(TestCdc.initConfig().createConfig(0), false);
//        List<MySqlSnapshotSplit> mySqlSplits = cdcLoader.getMySqlSplits();
//
//        List<MySqlSchemalessSnapshotSplit> remainingSplits = new ArrayList<>();
//        remainingSplits.addAll(mySqlSplits.subList(2, mySqlSplits.size()).stream().map(m->m.toSchemalessSnapshotSplit()).collect(Collectors.toList()));
//        System.out.println("remaining=====" + remainingSplits);
//        Map<String, MySqlSchemalessSnapshotSplit> assignedSplits = mySqlSplits.subList(0, 2).stream().map(m -> m.toSchemalessSnapshotSplit()).collect(Collectors.toMap(m -> m.splitId(), m -> m));
//
//        Map<TableId, TableChanges.TableChange> tableSchemas = cdcLoader.getTableSchemas();
//        Map<String, BinlogOffset> splitFinishedOffsets = new HashMap<>();
//        Map<String, String> binlogOffsetMap = new HashMap<>();
//        binlogOffsetMap.put("file", "mysql-bin.000001");
//        binlogOffsetMap.put("pos", "2651");
//        binlogOffsetMap.put("kind", "SPECIFIC");
//
//        assignedSplits.forEach((k, v) -> splitFinishedOffsets.put(k, new BinlogOffset(binlogOffsetMap)));
//
//        AssignerStatus assignerStatus = AssignerStatus.INITIAL_ASSIGNING;
//        ChunkSplitterState chunkSplitterState = new ChunkSplitterState(new TableId("test","","test_flink"), new ChunkSplitterState.ChunkBound(ChunkSplitterState.ChunkBoundType.START, (Object)null), 1);
//        SnapshotPendingSplitsState state = new SnapshotPendingSplitsState(
//            new ArrayList<>(),
//            remainingSplits,
//            assignedSplits,
//            tableSchemas,
//            splitFinishedOffsets,
//            assignerStatus,
//            new ArrayList<>(),
//            false, false, chunkSplitterState);
//        return state;
        return  null;
    }


}
