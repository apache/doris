// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.cdcclient.source.reader.mysql;

import org.apache.doris.cdcclient.constants.LoadConstants;
import org.apache.doris.cdcclient.model.JobConfig;
import org.apache.doris.cdcclient.model.request.CompareOffsetReq;
import org.apache.doris.cdcclient.model.request.FetchRecordReq;
import org.apache.doris.cdcclient.model.request.FetchTableSplitsReq;
import org.apache.doris.cdcclient.model.request.JobBaseRecordReq;
import org.apache.doris.cdcclient.model.response.RecordWithMeta;
import org.apache.doris.cdcclient.source.deserialize.DebeziumJsonDeserializer;
import org.apache.doris.cdcclient.source.deserialize.SourceRecordDeserializer;
import org.apache.doris.cdcclient.source.reader.SourceReader;
import org.apache.doris.cdcclient.source.reader.SplitReadResult;
import org.apache.doris.cdcclient.source.reader.SplitRecords;
import org.apache.doris.cdcclient.source.split.AbstractSourceSplit;
import org.apache.doris.cdcclient.source.split.BinlogSplit;
import org.apache.doris.cdcclient.source.split.SnapshotSplit;
import org.apache.doris.cdcclient.utils.ConfigUtil;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.utils.Preconditions;
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
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.cdc.connectors.mysql.source.utils.ChunkUtils;
import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
import org.apache.flink.cdc.connectors.mysql.source.utils.TableDiscoveryUtils;
import org.apache.flink.cdc.connectors.mysql.table.StartupMode;
import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner.BINLOG_SPLIT_ID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.document.Array;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlSourceReader implements SourceReader<MySqlSplit, MySqlSplitState> {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceReader.class);
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static final String SPLIT_ID = "splitId";
    private static final String PURE_BINLOG_PHASE = "pureBinlogPhase";
    private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new FlinkJsonTableChangeSerializer();
    private SourceRecordDeserializer<SourceRecord, List<String>> serializer;
    private JobRuntimeContext jobRuntimeContext;

    public MySqlSourceReader() {
        this.serializer = new DebeziumJsonDeserializer();
        this.jobRuntimeContext = new JobRuntimeContext();
    }

    @Override
    public void initialize() {}

    @Override
    public List<AbstractSourceSplit> getSourceSplits(FetchTableSplitsReq ftsReq) {
        MySqlSourceConfig sourceConfig = getSourceConfig(ftsReq);
        StartupMode startupMode = sourceConfig.getStartupOptions().startupMode;
        List<MySqlSnapshotSplit> remainingSnapshotSplits = new ArrayList<>();
        MySqlBinlogSplit remainingBinlogSplit = null;
        if (startupMode.equals(StartupMode.INITIAL)) {
            remainingSnapshotSplits =
                    startSplitChunks(sourceConfig, ftsReq.getSnapshotTable(), ftsReq.getConfig());
        } else {
            remainingBinlogSplit =
                    new MySqlBinlogSplit(
                            BINLOG_SPLIT_ID,
                            sourceConfig.getStartupOptions().binlogOffset,
                            BinlogOffset.ofNonStopping(),
                            new ArrayList<>(),
                            new HashMap<>(),
                            0);
        }
        List<AbstractSourceSplit> splits = new ArrayList<>();
        if (!remainingSnapshotSplits.isEmpty()) {
            for (MySqlSnapshotSplit snapshotSplit : remainingSnapshotSplits) {
                String splitId = snapshotSplit.splitId();
                String tableId = snapshotSplit.getTableId().identifier();
                Object[] splitStart = snapshotSplit.getSplitStart();
                Object[] splitEnd = snapshotSplit.getSplitEnd();
                List<String> splitKey = snapshotSplit.getSplitKeyType().getFieldNames();
                SnapshotSplit split =
                        new SnapshotSplit(splitId, tableId, splitKey, splitStart, splitEnd, null);
                splits.add(split);
            }
        } else {
            BinlogOffset startingOffset = remainingBinlogSplit.getStartingOffset();
            BinlogSplit binlogSplit = new BinlogSplit();
            binlogSplit.setSplitId(remainingBinlogSplit.splitId());
            binlogSplit.setStartingOffset(startingOffset.getOffset());
            splits.add(binlogSplit);
        }
        return splits;
    }

    /**
     * 1. If the SplitRecords iterator has it, read the iterator directly. 2. If there is a
     * binlogreader, poll it. 3. If there is none, resubmit split. 4. If reload is true, need to
     * reset binlogSplitReader and submit split.
     */
    @Override
    public RecordWithMeta read(FetchRecordReq fetchRecord) throws Exception {
        SplitReadResult<MySqlSplit, MySqlSplitState> readResult = readSplitRecords(fetchRecord);
        return buildRecordResponse(fetchRecord, readResult);
    }

    /** read split records. */
    @Override
    public SplitReadResult<MySqlSplit, MySqlSplitState> readSplitRecords(JobBaseRecordReq baseReq)
            throws Exception {
        Map<String, Object> offsetMeta = baseReq.getMeta();
        if (offsetMeta == null || offsetMeta.isEmpty()) {
            throw new RuntimeException("miss meta offset");
        }
        LOG.info("Job {} read split records with offset: {}", baseReq.getJobId(), offsetMeta);

        //  If there is an active split being consumed, reuse it directly;
        //  Otherwise, create a new snapshot/binlog split based on offset and start the reader.
        MySqlSplit split = null;
        SplitRecords currentSplitRecords = jobRuntimeContext.getCurrentSplitRecords();
        if (currentSplitRecords == null) {
            DebeziumReader<SourceRecords, MySqlSplit> currentReader =
                    jobRuntimeContext.getCurrentReader();
            if (currentReader == null || baseReq.isReload()) {
                LOG.info(
                        "No current reader or reload {}, create new split reader",
                        baseReq.isReload());
                // build split
                Tuple2<MySqlSplit, Boolean> splitFlag = createMySqlSplit(offsetMeta, baseReq);
                split = splitFlag.f0;
                currentSplitRecords = pollSplitRecordsWithSplit(split, baseReq);
                jobRuntimeContext.setCurrentSplitRecords(currentSplitRecords);
                jobRuntimeContext.setCurrentSplit(split);
            } else if (currentReader instanceof BinlogSplitReader) {
                LOG.info("Continue poll records with current binlog reader");
                // only for binlog reader
                currentSplitRecords = pollSplitRecordsWithCurrentReader(currentReader);
                split = jobRuntimeContext.getCurrentSplit();
            } else {
                throw new RuntimeException("Should not happen");
            }
        } else {
            LOG.info(
                    "Continue read records with current split records, splitId: {}",
                    currentSplitRecords.getSplitId());
        }

        // build response with iterator
        SplitReadResult<MySqlSplit, MySqlSplitState> result = new SplitReadResult<>();
        MySqlSplitState currentSplitState = null;
        MySqlSplit currentSplit = jobRuntimeContext.getCurrentSplit();
        if (currentSplit.isSnapshotSplit()) {
            currentSplitState = new MySqlSnapshotSplitState(currentSplit.asSnapshotSplit());
        } else {
            currentSplitState = new MySqlBinlogSplitState(currentSplit.asBinlogSplit());
        }

        Iterator<SourceRecord> filteredIterator =
                new FilteredRecordIterator(currentSplitRecords, currentSplitState);

        result.setRecordIterator(filteredIterator);
        result.setSplitState(currentSplitState);
        result.setSplit(split);
        return result;
    }

    /** build RecordWithMeta */
    private RecordWithMeta buildRecordResponse(
            FetchRecordReq fetchRecord, SplitReadResult<MySqlSplit, MySqlSplitState> readResult)
            throws Exception {
        RecordWithMeta recordResponse = new RecordWithMeta();
        MySqlSplit split = readResult.getSplit();
        MySqlSplitState currentSplitState = readResult.getSplitState();
        int count = 0;
        // Serialize records and add them to the response (collect from iterator)
        Iterator<SourceRecord> iterator = readResult.getRecordIterator();
        while (iterator != null && iterator.hasNext()) {
            SourceRecord element = iterator.next();
            List<String> serializedRecords =
                    serializer.deserialize(fetchRecord.getConfig(), element);
            if (!CollectionUtils.isEmpty(serializedRecords)) {
                recordResponse.getRecords().addAll(serializedRecords);
                count += serializedRecords.size();
                // update meta
                Map<String, String> lastMeta = RecordUtils.getBinlogPosition(element).getOffset();
                if (split.isBinlogSplit()) {
                    lastMeta.put(SPLIT_ID, BINLOG_SPLIT_ID);
                    recordResponse.setMeta(lastMeta);
                }
                if (count >= fetchRecord.getFetchSize()) {
                    return recordResponse;
                }
            }
        }

        finishSplitRecords();
        // Set meta information
        if (split.isSnapshotSplit() && currentSplitState != null) {
            BinlogOffset highWatermark =
                    currentSplitState.asSnapshotSplitState().getHighWatermark();
            Map<String, String> offsetRes = highWatermark.getOffset();
            offsetRes.put(SPLIT_ID, split.splitId());
            recordResponse.setMeta(offsetRes);
            return recordResponse;
        }
        if (CollectionUtils.isEmpty(recordResponse.getRecords())) {
            if (split.isBinlogSplit()) {
                Map<String, String> offsetRes = extractBinlogOffset(readResult.getSplit());
                recordResponse.setMeta(offsetRes);
            } else {
                SnapshotSplit snapshotSplit =
                        objectMapper.convertValue(fetchRecord.getMeta(), SnapshotSplit.class);
                Map<String, String> meta = new HashMap<>();
                meta.put(SPLIT_ID, snapshotSplit.getSplitId());
                // chunk no data
                recordResponse.setMeta(meta);
            }
        }
        return recordResponse;
    }

    /**
     * refresh table changes after schema change
     *
     * @param element
     * @param jobId
     * @throws IOException
     */
    private void refreshTableChanges(SourceRecord element, Long jobId) throws IOException {
        HistoryRecord historyRecord = RecordUtils.getHistoryRecord(element);
        Array tableChanges = historyRecord.document().getArray(HistoryRecord.Fields.TABLE_CHANGES);
        TableChanges changes = TABLE_CHANGE_SERIALIZER.deserialize(tableChanges, true);
        Map<TableId, TableChanges.TableChange> tableChangeMap = jobRuntimeContext.getTableSchemas();
        if (tableChangeMap == null) {
            tableChangeMap = new ConcurrentHashMap<>();
            jobRuntimeContext.setTableSchemas(tableChangeMap);
        }
        for (TableChanges.TableChange tblChange : changes) {
            tableChangeMap.put(tblChange.getTable().id(), tblChange);
        }
    }

    private Tuple2<MySqlSplit, Boolean> createMySqlSplit(
            Map<String, Object> offsetMeta, JobConfig jobConfig) throws JsonProcessingException {
        Tuple2<MySqlSplit, Boolean> splitRes = null;
        String splitId = String.valueOf(offsetMeta.get(SPLIT_ID));
        if (!BINLOG_SPLIT_ID.equals(splitId)) {
            MySqlSnapshotSplit split = createSnapshotSplit(offsetMeta, jobConfig);
            splitRes = Tuple2.of(split, false);
        } else {
            splitRes = createBinlogSplit(offsetMeta, jobConfig);
        }
        return splitRes;
    }

    private MySqlSnapshotSplit createSnapshotSplit(Map<String, Object> offset, JobConfig jobConfig)
            throws JsonProcessingException {
        SnapshotSplit snapshotSplit = objectMapper.convertValue(offset, SnapshotSplit.class);
        TableId tableId = TableId.parse(snapshotSplit.getTableId());
        Object[] splitStart = snapshotSplit.getSplitStart();
        Object[] splitEnd = snapshotSplit.getSplitEnd();
        List<String> splitKeys = snapshotSplit.getSplitKey();
        Map<TableId, TableChanges.TableChange> tableSchemas = getTableSchemas(jobConfig);
        TableChanges.TableChange tableChange = tableSchemas.get(tableId);
        Preconditions.checkNotNull(
                tableChange, "Can not find table " + tableId + " in job " + jobConfig.getJobId());
        // only support one split key
        String splitKey = splitKeys.get(0);
        Column splitColumn = tableChange.getTable().columnWithName(splitKey);
        RowType splitType = ChunkUtils.getChunkKeyColumnType(splitColumn, false);
        MySqlSnapshotSplit split =
                new MySqlSnapshotSplit(
                        tableId,
                        snapshotSplit.getSplitId(),
                        splitType,
                        splitStart,
                        splitEnd,
                        null,
                        tableSchemas);
        return split;
    }

    private Tuple2<MySqlSplit, Boolean> createBinlogSplit(
            Map<String, Object> meta, JobConfig config) {
        MySqlSourceConfig sourceConfig = getSourceConfig(config);
        BinlogOffset offsetConfig = null;
        if (sourceConfig.getStartupOptions() != null) {
            offsetConfig = sourceConfig.getStartupOptions().binlogOffset;
        }
        BinlogSplit binlogSplit = objectMapper.convertValue(meta, BinlogSplit.class);
        List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();
        BinlogOffset minOffsetFinishSplits = null;
        BinlogOffset maxOffsetFinishSplits = null;
        if (CollectionUtils.isNotEmpty(binlogSplit.getFinishedSplits())) {
            List<SnapshotSplit> splitWithHW = binlogSplit.getFinishedSplits();
            List<SnapshotSplit> assignedSplitLists =
                    splitWithHW.stream()
                            .sorted(Comparator.comparing(AbstractSourceSplit::getSplitId))
                            .toList();

            for (SnapshotSplit split : assignedSplitLists) {
                // find the min binlog offset
                Map<String, String> offsetMap = split.getHighWatermark();
                BinlogOffset binlogOffset = new BinlogOffset(offsetMap);
                if (minOffsetFinishSplits == null || binlogOffset.isBefore(minOffsetFinishSplits)) {
                    minOffsetFinishSplits = binlogOffset;
                }
                if (maxOffsetFinishSplits == null || binlogOffset.isAfter(maxOffsetFinishSplits)) {
                    maxOffsetFinishSplits = binlogOffset;
                }
                finishedSnapshotSplitInfos.add(
                        new FinishedSnapshotSplitInfo(
                                TableId.parse(split.getTableId()),
                                split.getSplitId(),
                                split.getSplitStart(),
                                split.getSplitEnd(),
                                binlogOffset));
            }
        }

        BinlogOffset startOffset;
        BinlogOffset lastOffset =
                new BinlogOffset(
                        binlogSplit.getStartingOffset() == null
                                ? new HashMap<>()
                                : binlogSplit.getStartingOffset());
        if (minOffsetFinishSplits != null && lastOffset.getOffsetKind() == null) {
            startOffset = minOffsetFinishSplits;
        } else if (lastOffset.getOffsetKind() != null && lastOffset.getFilename() != null) {
            startOffset = lastOffset;
        } else if (offsetConfig != null) {
            startOffset = offsetConfig;
        } else {
            startOffset = BinlogOffset.ofEarliest();
        }

        boolean pureBinlogPhase = false;
        if (maxOffsetFinishSplits == null) {
            pureBinlogPhase = true;
        } else if (startOffset.isAtOrAfter(maxOffsetFinishSplits)) {
            // All the offsets of the current split are smaller than the offset of the binlog,
            // indicating that the binlog phase has been fully entered.
            pureBinlogPhase = true;
            LOG.info(
                    "The binlog phase has been fully entered, the current split is: {}",
                    startOffset);
        }

        MySqlBinlogSplit split =
                new MySqlBinlogSplit(
                        BINLOG_SPLIT_ID,
                        startOffset,
                        BinlogOffset.ofNonStopping(),
                        finishedSnapshotSplitInfos,
                        new HashMap<>(),
                        0);
        // filterTableSchema
        MySqlBinlogSplit binlogSplitFinal =
                MySqlBinlogSplit.fillTableSchemas(split.asBinlogSplit(), getTableSchemas(config));
        return Tuple2.of(binlogSplitFinal, pureBinlogPhase);
    }

    private List<MySqlSnapshotSplit> startSplitChunks(
            MySqlSourceConfig sourceConfig, String snapshotTable, Map<String, String> config) {
        List<TableId> remainingTables = new ArrayList<>();
        if (snapshotTable != null) {
            // need add database name
            String database = config.get(LoadConstants.DATABASE);
            remainingTables.add(TableId.parse(database + "." + snapshotTable));
        }
        List<MySqlSnapshotSplit> remainingSplits = new ArrayList<>();
        MySqlSnapshotSplitAssigner splitAssigner =
                new MySqlSnapshotSplitAssigner(
                        sourceConfig, 1, remainingTables, false, new MockSplitEnumeratorContext(1));
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
        splitAssigner.close();
        return remainingSplits;
    }

    private SplitRecords pollSplitRecordsWithSplit(MySqlSplit split, JobConfig jobConfig)
            throws Exception {
        Preconditions.checkState(split != null, "split is null");
        Iterator<SourceRecords> dataIt = null;
        String currentSplitId = null;
        DebeziumReader<SourceRecords, MySqlSplit> currentReader = null;
        LOG.info("Get a split: {}", split.splitId());
        if (split instanceof MySqlSnapshotSplit) {
            currentReader = getSnapshotSplitReader(jobConfig);
        } else if (split instanceof MySqlBinlogSplit) {
            currentReader = getBinlogSplitReader(jobConfig);
        }
        jobRuntimeContext.setCurrentReader(currentReader);
        currentReader.submitSplit(split);
        currentSplitId = split.splitId();
        // make split record available
        // todo: Until debezium_heartbeat is consumed
        Thread.sleep(1000);
        dataIt = currentReader.pollSplitRecords();
        if (currentReader instanceof SnapshotSplitReader) {
            closeSnapshotReader();
        }
        return dataIt == null ? null : new SplitRecords(currentSplitId, dataIt.next());
    }

    private SplitRecords pollSplitRecordsWithCurrentReader(
            DebeziumReader<SourceRecords, MySqlSplit> currentReader) throws Exception {
        Iterator<SourceRecords> dataIt = null;
        if (currentReader instanceof BinlogSplitReader) {
            dataIt = currentReader.pollSplitRecords();
            return dataIt == null ? null : new SplitRecords(BINLOG_SPLIT_ID, dataIt.next());
        } else {
            throw new IllegalStateException("Unsupported reader type.");
        }
    }

    private SnapshotSplitReader getSnapshotSplitReader(JobConfig config) {
        MySqlSourceConfig sourceConfig = getSourceConfig(config);
        SnapshotSplitReader snapshotReader = jobRuntimeContext.getSnapshotReader();
        if (snapshotReader == null) {
            final MySqlConnection jdbcConnection =
                    DebeziumUtils.createMySqlConnection(sourceConfig);
            final BinaryLogClient binaryLogClient =
                    DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration());
            final StatefulTaskContext statefulTaskContext =
                    new StatefulTaskContext(sourceConfig, binaryLogClient, jdbcConnection);
            snapshotReader = new SnapshotSplitReader(statefulTaskContext, 0);
            jobRuntimeContext.setSnapshotReader(snapshotReader);
        }
        return snapshotReader;
    }

    private BinlogSplitReader getBinlogSplitReader(JobConfig config) {
        MySqlSourceConfig sourceConfig = getSourceConfig(config);
        BinlogSplitReader binlogReader = jobRuntimeContext.getBinlogReader();
        if (binlogReader == null) {
            final MySqlConnection jdbcConnection =
                    DebeziumUtils.createMySqlConnection(sourceConfig);
            final BinaryLogClient binaryLogClient =
                    DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration());
            final StatefulTaskContext statefulTaskContext =
                    new StatefulTaskContext(sourceConfig, binaryLogClient, jdbcConnection);
            binlogReader = new BinlogSplitReader(statefulTaskContext, 0);
            jobRuntimeContext.setBinlogReader(binlogReader);
        }
        return binlogReader;
    }

    private void closeSnapshotReader() {
        SnapshotSplitReader reusedSnapshotReader = jobRuntimeContext.getSnapshotReader();
        if (reusedSnapshotReader != null) {
            LOG.info(
                    "Close snapshot reader {}", reusedSnapshotReader.getClass().getCanonicalName());
            reusedSnapshotReader.close();
            DebeziumReader<SourceRecords, MySqlSplit> currentReader =
                    jobRuntimeContext.getCurrentReader();
            if (reusedSnapshotReader == currentReader) {
                jobRuntimeContext.setCurrentReader(null);
            }
            jobRuntimeContext.setSnapshotReader(null);
        }
    }

    private void closeBinlogReader() {
        BinlogSplitReader reusedBinlogReader = jobRuntimeContext.getBinlogReader();
        if (reusedBinlogReader != null) {
            LOG.info("Close binlog reader {}", reusedBinlogReader.getClass().getCanonicalName());
            reusedBinlogReader.close();
            DebeziumReader<SourceRecords, MySqlSplit> currentReader =
                    jobRuntimeContext.getCurrentReader();
            if (reusedBinlogReader == currentReader) {
                jobRuntimeContext.setCurrentReader(null);
            }
            jobRuntimeContext.setBinlogReader(null);
        }
    }

    private MySqlSourceConfig getSourceConfig(JobConfig config) {
        return ConfigUtil.generateMySqlConfig(config);
    }

    @Override
    public Map<String, String> extractSnapshotOffset(Object splitState, Object split) {
        if (splitState == null) {
            return null;
        }
        MySqlSplitState mysqlSplitState = (MySqlSplitState) splitState;
        MySqlSplit mysqlSplit = (MySqlSplit) split;
        BinlogOffset highWatermark = mysqlSplitState.asSnapshotSplitState().getHighWatermark();
        Map<String, String> offsetRes = new HashMap<>(highWatermark.getOffset());
        if (mysqlSplit != null) {
            offsetRes.put(SPLIT_ID, mysqlSplit.splitId());
        }
        return offsetRes;
    }

    @Override
    public Map<String, String> extractBinlogOffset(Object split) {
        if (split == null) {
            return null;
        }
        MySqlSplit mysqlSplit = (MySqlSplit) split;
        Map<String, String> offsetRes = mysqlSplit.asBinlogSplit().getStartingOffset().getOffset();
        offsetRes.put(SPLIT_ID, BINLOG_SPLIT_ID);
        // offsetRes.put(PURE_BINLOG_PHASE, String.valueOf(pureBinlogPhase));
        return offsetRes;
    }

    @Override
    public String getSplitId(Object split) {
        if (split == null) {
            return null;
        }
        MySqlSplit mysqlSplit = (MySqlSplit) split;
        return mysqlSplit.splitId();
    }

    @Override
    public boolean isBinlogSplit(Object split) {
        if (split == null) {
            return false;
        }
        MySqlSplit mysqlSplit = (MySqlSplit) split;
        return mysqlSplit.isBinlogSplit();
    }

    @Override
    public void close(Long jobId) {
        jobRuntimeContext.close();
        LOG.info("Close source reader for job {}", jobId);
    }

    @Override
    public void finishSplitRecords() {
        jobRuntimeContext.setCurrentSplitRecords(null);
    }

    @Override
    public Map<String, String> getEndOffset(JobConfig jobConfig) {
        MySqlSourceConfig sourceConfig = getSourceConfig(jobConfig);
        try (MySqlConnection jdbc = DebeziumUtils.createMySqlConnection(sourceConfig)) {
            BinlogOffset binlogOffset = DebeziumUtils.currentBinlogOffset(jdbc);
            return binlogOffset.getOffset();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public int compareOffset(CompareOffsetReq compareOffsetReq) {
        Map<String, String> offsetFirst = compareOffsetReq.getOffsetFirst();
        Map<String, String> offsetSecond = compareOffsetReq.getOffsetSecond();
        // make server id is equals
        String serverId1 = offsetFirst.get("server_id");
        String serverId2 = offsetSecond.get("server_id");
        if (serverId1 == null && serverId2 != null) {
            offsetFirst.put("server_id", serverId2);
        }
        if (serverId2 == null && serverId1 != null) {
            offsetSecond.put("server_id", serverId1);
        }

        BinlogOffset binlogOffset1 = new BinlogOffset(offsetFirst);
        BinlogOffset binlogOffset2 = new BinlogOffset(offsetSecond);
        return binlogOffset1.compareTo(binlogOffset2);
    }

    private Map<TableId, TableChanges.TableChange> getTableSchemas(JobConfig config) {
        Map<TableId, TableChanges.TableChange> schemas = jobRuntimeContext.getTableSchemas();
        if (schemas == null) {
            schemas = discoverTableSchemas(config);
            jobRuntimeContext.setTableSchemas(schemas);
        }
        return schemas;
    }

    private Map<TableId, TableChanges.TableChange> discoverTableSchemas(JobConfig config) {
        MySqlSourceConfig sourceConfig = getSourceConfig(config);
        try (MySqlConnection jdbc = DebeziumUtils.createMySqlConnection(sourceConfig)) {
            MySqlPartition partition =
                    new MySqlPartition(sourceConfig.getMySqlConnectorConfig().getLogicalName());
            return TableDiscoveryUtils.discoverSchemaForCapturedTables(
                    partition, sourceConfig, jdbc);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Getter
    @Setter
    private static final class JobRuntimeContext {
        private SnapshotSplitReader snapshotReader;
        private BinlogSplitReader binlogReader;
        private DebeziumReader<SourceRecords, MySqlSplit> currentReader;
        private Map<TableId, TableChanges.TableChange> tableSchemas;
        private SplitRecords currentSplitRecords;
        private MySqlSplit currentSplit;

        private void close() {
            if (snapshotReader != null) {
                snapshotReader.close();
                snapshotReader = null;
            }
            if (binlogReader != null) {
                binlogReader.close();
                binlogReader = null;
            }
            currentReader = null;
            currentSplitRecords = null;
            tableSchemas = null;
        }
    }

    /**
     * Filtered record iterator that only returns data change records, filtering out watermark,
     * heartbeat and other events. This is a private static inner class that encapsulates record
     * filtering logic, making the main method cleaner.
     */
    private static class FilteredRecordIterator implements Iterator<SourceRecord> {
        private final Iterator<SourceRecord> sourceIterator;
        private final MySqlSplitState splitState;
        private SourceRecord nextRecord;

        FilteredRecordIterator(SplitRecords currentSplitRecords, MySqlSplitState splitState) {
            this.sourceIterator =
                    currentSplitRecords != null && !currentSplitRecords.isEmpty()
                            ? currentSplitRecords.getIterator()
                            : null;
            this.splitState = splitState;
        }

        @Override
        public boolean hasNext() {
            if (sourceIterator == null) {
                return false;
            }
            if (nextRecord != null) {
                return true;
            }

            while (sourceIterator.hasNext()) {
                SourceRecord element = sourceIterator.next();
                if (RecordUtils.isWatermarkEvent(element)) {
                    BinlogOffset watermark = RecordUtils.getWatermark(element);
                    if (RecordUtils.isHighWatermarkEvent(element)
                            && splitState.isSnapshotSplitState()) {
                        splitState.asSnapshotSplitState().setHighWatermark(watermark);
                    }
                } else if (RecordUtils.isHeartbeatEvent(element)) {
                    LOG.debug("Receive heartbeat event: {}", element);
                    if (splitState.isBinlogSplitState()) {
                        BinlogOffset position = RecordUtils.getBinlogPosition(element);
                        splitState.asBinlogSplitState().setStartingOffset(position);
                    }
                } else if (RecordUtils.isDataChangeRecord(element)) {
                    nextRecord = element;
                    return true;
                } else {
                    LOG.debug("Ignore event: {}", element);
                }
            }
            return false;
        }

        @Override
        public SourceRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            SourceRecord record = nextRecord;
            nextRecord = null;
            return record;
        }
    }
}
