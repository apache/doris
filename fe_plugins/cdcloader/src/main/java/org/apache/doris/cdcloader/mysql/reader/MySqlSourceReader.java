package org.apache.doris.cdcloader.mysql.reader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.commons.collections.CollectionUtils;
import org.apache.doris.cdcloader.mysql.rest.model.FetchRecordReq;
import org.apache.doris.cdcloader.mysql.rest.model.JobConfig;
import org.apache.doris.cdcloader.mysql.serialize.JsonSerializer;
import org.apache.doris.cdcloader.mysql.serialize.DorisRecordSerializer;
import org.apache.doris.cdcloader.mysql.utils.ConfigUtil;
import org.apache.doris.job.extensions.cdc.state.AbstractSourceSplit;
import org.apache.doris.job.extensions.cdc.state.BinlogSplit;
import org.apache.doris.job.extensions.cdc.state.SnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.BinlogSplitReader;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.DebeziumReader;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.SnapshotSplitReader;
import org.apache.flink.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlSnapshotSplitAssigner;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.cdc.connectors.mysql.source.utils.ChunkUtils;
import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
import org.apache.flink.cdc.connectors.mysql.source.utils.TableDiscoveryUtils;
import org.apache.flink.cdc.connectors.mysql.table.StartupMode;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner.BINLOG_SPLIT_ID;

public class MySqlSourceReader implements SourceReader<RecordWithMeta, FetchRecordReq> {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceReader.class);
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static final String SPLIT_ID = "splitId";
    private static final String FINISH_SPLITS = "finishSplits";
    private static final String ASSIGNED_SPLITS = "assignedSplits";
    private Map<Long, MySqlSourceConfig> sourceConfigMap;
    private Map<Long, SnapshotSplitReader> reusedSnapshotReaderMap;
    private Map<Long, BinlogSplitReader> reusedBinlogReaderMap;
    private Map<Long, DebeziumReader<SourceRecords, MySqlSplit>> currentReaderMap;
    private DorisRecordSerializer<SourceRecord, List<String>> serializer;
    private Map<Long, Map<TableId, TableChanges.TableChange>> tableSchemaMaps;
    private Map<Long, SplitRecords> currentSplitRecordsMap;

    public MySqlSourceReader() {
        this.sourceConfigMap = new ConcurrentHashMap<>();
        this.reusedSnapshotReaderMap = new ConcurrentHashMap<>();
        this.reusedBinlogReaderMap = new ConcurrentHashMap<>();
        this.currentReaderMap = new ConcurrentHashMap<>();
        this.tableSchemaMaps = new ConcurrentHashMap<>();
        this.serializer = new JsonSerializer();
        this.currentSplitRecordsMap = new ConcurrentHashMap<>();
    }

    @Override
    public void initialize() {
    }

    @Override
    public List<AbstractSourceSplit> getSourceSplits(JobConfig config) throws JsonProcessingException {
        MySqlSourceConfig sourceConfig = getSourceConfig(config);
        StartupMode startupMode = sourceConfig.getStartupOptions().startupMode;
        List<MySqlSnapshotSplit> remainingSnapshotSplits = new ArrayList<>();
        MySqlBinlogSplit remainingBinlogSplit = null;
        if(startupMode.equals(StartupMode.INITIAL)){
            remainingSnapshotSplits = startSplitChunks(sourceConfig);
        }else{
            remainingBinlogSplit = new MySqlBinlogSplit(
                BINLOG_SPLIT_ID,
                sourceConfig.getStartupOptions().binlogOffset,
                BinlogOffset.ofNonStopping(),
                new ArrayList<>(),
                new HashMap<>(),
                0);
        }
        List<AbstractSourceSplit> splits = new ArrayList<>();
        if(!remainingSnapshotSplits.isEmpty()){
            for(MySqlSnapshotSplit snapshotSplit: remainingSnapshotSplits){
                String splitId = snapshotSplit.splitId();
                String tableId = snapshotSplit.getTableId().identifier();
                String splitStart = snapshotSplit.getSplitStart() == null ? null : objectMapper.writeValueAsString(snapshotSplit.getSplitStart());
                String splitEnd = snapshotSplit.getSplitEnd() == null ? null : objectMapper.writeValueAsString(snapshotSplit.getSplitEnd());
                String splitKey = snapshotSplit.getSplitKeyType().getFieldNames().get(0);
                SnapshotSplit split = new SnapshotSplit(splitId, tableId, splitKey, splitStart, splitEnd, null);
                splits.add(split);
            }
        }else{
            BinlogOffset startingOffset = remainingBinlogSplit.getStartingOffset();
            BinlogSplit binlogSplit = new BinlogSplit(remainingBinlogSplit.splitId(), startingOffset.getOffset());
            splits.add(binlogSplit);
        }
        return splits;
    }

    @Override
    public RecordWithMeta read(FetchRecordReq fetchRecord) throws Exception {
        JobConfig jobConfig = new JobConfig(fetchRecord.getJobId(), fetchRecord.getConfig());
        int count=0;
        Map<String, String> lastMeta = new HashMap<>();
        RecordWithMeta recordResponse = new RecordWithMeta(lastMeta, new ArrayList<>());
        int fetchSize = fetchRecord.getFetchSize();
        boolean schedule = fetchRecord.isSchedule();
        MySqlSplit split = null;

        if(schedule){
            Map<String, String> offset = fetchRecord.getMeta();
            if(offset.isEmpty()){
                throw new RuntimeException("miss meta offset");
            }
            String splitId = offset.get(SPLIT_ID);
            if(!BINLOG_SPLIT_ID.equals(splitId)){
                SnapshotSplit snapshotSplit = objectMapper.convertValue(offset, SnapshotSplit.class);
                TableId tableId = TableId.parse(snapshotSplit.getTableId());
                Object[] splitStart = snapshotSplit.getSplitStart() == null ? null : objectMapper.readValue(snapshotSplit.getSplitStart(), Object[].class);
                Object[] splitEnd = snapshotSplit.getSplitEnd() == null ? null : objectMapper.readValue(snapshotSplit.getSplitEnd(), Object[].class);
                String splitKey = snapshotSplit.getSplitKey();
                Map<TableId, TableChanges.TableChange> tableSchemas = getTableSchemas(jobConfig);
                TableChanges.TableChange tableChange = tableSchemas.get(tableId);
                Column splitColumn = tableChange.getTable().columnWithName(splitKey);
                RowType splitType = ChunkUtils.getChunkKeyColumnType(splitColumn);
                split = new MySqlSnapshotSplit(tableId,splitId, splitType, splitStart, splitEnd, null, tableSchemas);

            }else{
                split = createBinlogSplit(offset,jobConfig);
            }
            //reset current reader
            closeBinlogReader(jobConfig.getJobId());
            SplitRecords currentSplitRecords = pollSplitRecords(split, jobConfig);
            currentSplitRecordsMap.put(jobConfig.getJobId(), currentSplitRecords);
        }

        if(split == null){
            throw new RuntimeException("Can not generate split");
        }

        SplitRecords currentSplitRecords = currentSplitRecordsMap.get(jobConfig.getJobId());
        MySqlSplitState currentSplitState = null;
        if (currentSplitRecords != null && !currentSplitRecords.isEmpty()) {
            Iterator<SourceRecord> recordIt = currentSplitRecords.getIterator();

            if(split.isSnapshotSplit()){
                currentSplitState = new MySqlSnapshotSplitState(split.asSnapshotSplit());
            }

            while (recordIt.hasNext()) {
                SourceRecord element = recordIt.next();

                if (RecordUtils.isWatermarkEvent(element)) {
                    BinlogOffset watermark = RecordUtils.getWatermark(element);
                    if (RecordUtils.isHighWatermarkEvent(element) && currentSplitState instanceof MySqlSnapshotSplitState) {
                        currentSplitState.asSnapshotSplitState().setHighWatermark(watermark);
                    }
                }else if (RecordUtils.isHeartbeatEvent(element)) {
                    //updateStartingOffsetForSplit(currentSplitState, element);
                } else if (RecordUtils.isDataChangeRecord(element)) {
                    List<String> serialize = serializer.serialize(element);
                    if(CollectionUtils.isEmpty(serialize)){
                        continue;
                    }
                    count += serialize.size();
                    lastMeta = RecordUtils.getBinlogPosition(element).getOffset();
                    if(split.isBinlogSplit()){
                        lastMeta.put(SPLIT_ID, BINLOG_SPLIT_ID);
                        recordResponse.setMeta(lastMeta);
                    }
                    recordResponse.getRecords().addAll(serialize);
                    if(count >= fetchSize){
                        return recordResponse;
                    }
                } else {
                    // unknown element
                    System.out.println("Meet unknown element {}, just skip." + element);
                }
            }
        }
        if(split.isSnapshotSplit()){
            BinlogOffset highWatermark = currentSplitState.asSnapshotSplitState().getHighWatermark();
            Map<String, String> offset = highWatermark.getOffset();
            offset.put(SPLIT_ID, split.splitId());
            recordResponse.setMeta(offset);
        }
        if(CollectionUtils.isEmpty(recordResponse.getRecords())){
            if(split.isBinlogSplit()){
                Map<String, String> offset = split.asBinlogSplit().getStartingOffset().getOffset();
                offset.put(SPLIT_ID, BINLOG_SPLIT_ID);
                recordResponse.setMeta(offset);
            }else{
                recordResponse.setMeta(fetchRecord.getMeta());
            }
        }
        return recordResponse;
    }

    private MySqlBinlogSplit createBinlogSplit(Map<String, String> meta, JobConfig config) throws JsonProcessingException {
        MySqlSourceConfig sourceConfig = getSourceConfig(config);
        BinlogOffset offsetConfig = null;
        if(sourceConfig.getStartupOptions() != null){
            offsetConfig = sourceConfig.getStartupOptions().binlogOffset;
        }

        List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();
        BinlogOffset minOffsetFinishSplits = null;
        if(meta.containsKey(FINISH_SPLITS) && meta.containsKey(ASSIGNED_SPLITS) ){
            //Construct binlogsplit based on the finished split and assigned split.
            String finishSplitsOffset = meta.remove(FINISH_SPLITS);
            String assignedSplits = meta.remove(ASSIGNED_SPLITS);
            Map<String, Map<String, String>> splitFinishedOffsets = objectMapper.readValue(finishSplitsOffset, new TypeReference<Map<String, Map<String, String>>>() {});
            Map<String, SnapshotSplit> assignedSplitsMap = objectMapper.readValue(assignedSplits, new TypeReference<Map<String, SnapshotSplit>>() {});
            List<SnapshotSplit> assignedSplitLists = assignedSplitsMap.values().stream()
                .sorted(Comparator.comparing(AbstractSourceSplit::getSplitId))
                .collect(Collectors.toList());

            for (SnapshotSplit split : assignedSplitLists) {
                // find the min binlog offset
                Map<String, String> offsetMap = splitFinishedOffsets.get(split.getSplitId());
                BinlogOffset binlogOffset = new BinlogOffset(offsetMap);
                if (minOffsetFinishSplits == null || binlogOffset.isBefore(minOffsetFinishSplits)) {
                    minOffsetFinishSplits = binlogOffset;
                }
                Object[] splitStart = split.getSplitStart() == null ? null : objectMapper.readValue(split.getSplitStart(), Object[].class);
                Object[] splitEnd = split.getSplitEnd() == null ? null : objectMapper.readValue(split.getSplitEnd(), Object[].class);

                finishedSnapshotSplitInfos.add(
                    new FinishedSnapshotSplitInfo(
                        TableId.parse(split.getTableId()),
                        split.getSplitId(),
                        splitStart,
                        splitEnd,
                        binlogOffset));
            }
        }

        BinlogOffset startOffset;
        BinlogOffset lastOffset = new BinlogOffset(meta);
        if (minOffsetFinishSplits != null && lastOffset.getOffsetKind() == null) {
            startOffset = minOffsetFinishSplits;
        }else if(lastOffset.getOffsetKind() != null && lastOffset.getFilename() != null){
            startOffset = lastOffset;
        }else if(offsetConfig != null){
            startOffset = offsetConfig;
        }else{
            startOffset = BinlogOffset.ofEarliest();
        }

        MySqlBinlogSplit split = new MySqlBinlogSplit(BINLOG_SPLIT_ID, startOffset, BinlogOffset.ofNonStopping(), finishedSnapshotSplitInfos, new HashMap<>(), 0);
        //filterTableSchema
        MySqlBinlogSplit binlogSplit = MySqlBinlogSplit.fillTableSchemas(split.asBinlogSplit(), getTableSchemas(config));
        return binlogSplit;
    }

    private List<MySqlSnapshotSplit> startSplitChunks(MySqlSourceConfig sourceConfig){
        List<MySqlSnapshotSplit> remainingSplits = new ArrayList<>();
        MySqlSnapshotSplitAssigner splitAssigner =
            new MySqlSnapshotSplitAssigner(sourceConfig, 1, new ArrayList<>(), false);
        //增加remainSplits，alreadyProcessTables
        //new MySqlSnapshotSplitAssigner(sourceConfig, 1, new ArrayList<>(), false);
        splitAssigner.open();
        while (true) {
            Optional<MySqlSplit> mySqlSplit = splitAssigner.getNext();
            if (mySqlSplit.isPresent()) {
                MySqlSnapshotSplit snapshotSplit = mySqlSplit.get().asSnapshotSplit();
                remainingSplits.add(snapshotSplit);
            } else {
                break;
            }
        }
        return remainingSplits;
    }

    private SplitRecords pollSplitRecords(MySqlSplit split, JobConfig jobConfig) throws Exception {
        Iterator<SourceRecords> dataIt = null;
        String currentSplitId = null;
        DebeziumReader<SourceRecords, MySqlSplit> currentReader = currentReaderMap.get(jobConfig.getJobId());
        if (currentReader == null) {
            LOG.info("Get a split: {}", split.splitId());
            if (split instanceof MySqlSnapshotSplit) {
                currentReader = getSnapshotSplitReader(jobConfig);
            } else if (split instanceof MySqlBinlogSplit) {
                currentReader = getBinlogSplitReader(jobConfig);
            }
            currentReaderMap.put(jobConfig.getJobId(), currentReader);
            currentReader.submitSplit(split);
            currentSplitId = split.splitId();
            //make split record available
            Thread.sleep(100);
            dataIt = currentReader.pollSplitRecords();
            if(currentReader instanceof SnapshotSplitReader){
                closeSnapshotReader(jobConfig.getJobId());
            }
            return dataIt == null ? null : new SplitRecords(currentSplitId, dataIt.next());
        }

//        else if (currentReader instanceof SnapshotSplitReader) {
//            dataIt = currentReader.pollSplitRecords();
//            if (dataIt != null) {
//                if (split != null) {
//                    currentReader.submitSplit(split);
//                    currentSplitId = split.splitId();
//                } else {
//                    closeSnapshotReader(jobConfig.getJobId());
//                }
//                return new SplitRecords(currentSplitId, dataIt.next());
//            }
//            return null;
//        } else if (currentReader instanceof BinlogSplitReader) {
//            dataIt = currentReader.pollSplitRecords();
//            if (dataIt != null) {
//                return new SplitRecords(BINLOG_SPLIT_ID, dataIt.next());
//            } else {
//                // null will be returned after receiving suspend binlog event
//                // finish current binlog split reading
//                closeBinlogReader(jobConfig.getJobId());
//                return null;
//            }
//        }

        else {
            throw new IllegalStateException("Unsupported reader type.");
        }
    }

    private SnapshotSplitReader getSnapshotSplitReader(JobConfig config) {
        MySqlSourceConfig sourceConfig = getSourceConfig(config);
        SnapshotSplitReader reusedSnapshotReader = reusedSnapshotReaderMap.computeIfAbsent(config.getJobId(), v->{
            final MySqlConnection jdbcConnection =
                DebeziumUtils.createMySqlConnection(sourceConfig);
            final BinaryLogClient binaryLogClient =
                DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration());
            final StatefulTaskContext statefulTaskContext =
                new StatefulTaskContext(sourceConfig, binaryLogClient, jdbcConnection);
            return new SnapshotSplitReader(statefulTaskContext, 0);
        });
        return reusedSnapshotReader;
    }

    private BinlogSplitReader getBinlogSplitReader(JobConfig config) {
        MySqlSourceConfig sourceConfig = getSourceConfig(config);
        BinlogSplitReader reusedBinlogReader = reusedBinlogReaderMap.computeIfAbsent(config.getJobId(), v->{
            //todo: reuse binlog reader
            final MySqlConnection jdbcConnection =
                DebeziumUtils.createMySqlConnection(sourceConfig);
            final BinaryLogClient binaryLogClient =
                DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration());
            final StatefulTaskContext statefulTaskContext =
                new StatefulTaskContext(sourceConfig, binaryLogClient, jdbcConnection);
            return new BinlogSplitReader(statefulTaskContext, 0);
        });
        return reusedBinlogReader;
    }

    private void closeSnapshotReader(Long jobId) {
        SnapshotSplitReader reusedSnapshotReader = reusedSnapshotReaderMap.remove(jobId);
        if (reusedSnapshotReader != null) {
            LOG.debug(
                "Close snapshot reader {}", reusedSnapshotReader.getClass().getCanonicalName());
            reusedSnapshotReader.close();
            DebeziumReader<SourceRecords, MySqlSplit> currentReader = currentReaderMap.get(jobId);
            if (reusedSnapshotReader == currentReader) {
                currentReader = null;
                currentReaderMap.remove(jobId);
            }
            reusedSnapshotReader = null;
        }
    }

    private void stopBinlogReadTask(Long jobId) {
        BinlogSplitReader reusedBinlogReader = reusedBinlogReaderMap.get(jobId);
        if (reusedBinlogReader != null) {
            LOG.debug("Stop binlog read task {}", reusedBinlogReader.getClass().getCanonicalName());
            reusedBinlogReader.stopBinlogReadTask();
        }
    }

    private void closeBinlogReader(Long jobId) {
        BinlogSplitReader reusedBinlogReader = reusedBinlogReaderMap.remove(jobId);
        if (reusedBinlogReader != null) {
            LOG.debug("Close binlog reader {}", reusedBinlogReader.getClass().getCanonicalName());
            reusedBinlogReader.close();
            DebeziumReader<SourceRecords, MySqlSplit> currentReader = currentReaderMap.get(jobId);
            if (reusedBinlogReader == currentReader) {
                currentReader = null;
                currentReaderMap.remove(jobId);
            }
            reusedBinlogReader = null;
        }
    }

    private MySqlSourceConfig getSourceConfig(JobConfig config){
        return sourceConfigMap.computeIfAbsent(config.getJobId(), v -> ConfigUtil.generateMySqlConfig(config.getConfig()));
    }

    @Override
    public void close(Long jobId) {
        closeSnapshotReader(jobId);
        closeBinlogReader(jobId);
        sourceConfigMap.remove(jobId);
        tableSchemaMaps.remove(jobId).clear();
        currentSplitRecordsMap.remove(jobId);
    }

    private Map<TableId, TableChanges.TableChange> getTableSchemas(JobConfig config){
        return tableSchemaMaps.computeIfAbsent(config.getJobId(), v -> discoverTableSchemas(config));
    }

    private Map<TableId, TableChanges.TableChange> discoverTableSchemas(JobConfig config){
        MySqlSourceConfig sourceConfig = getSourceConfig(config);
        try (MySqlConnection jdbc = DebeziumUtils.createMySqlConnection(sourceConfig)) {
            MySqlPartition partition =
                new MySqlPartition(sourceConfig.getMySqlConnectorConfig().getLogicalName());
            return TableDiscoveryUtils.discoverSchemaForCapturedTables(
                partition, sourceConfig, jdbc);
        }catch (SQLException ex){
            throw new RuntimeException(ex);
        }
    }

}
