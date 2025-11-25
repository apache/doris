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
import org.apache.doris.cdcclient.model.req.BaseRecordReq;
import org.apache.doris.cdcclient.model.req.FetchRecordReq;
import org.apache.doris.cdcclient.model.resp.RecordWithMeta;
import org.apache.doris.cdcclient.source.deserialize.DebeziumJsonDeserializer;
import org.apache.doris.cdcclient.source.deserialize.SourceRecordDeserializer;
import org.apache.doris.cdcclient.source.reader.SourceReader;
import org.apache.doris.cdcclient.source.reader.SplitReadResult;
import org.apache.doris.cdcclient.source.reader.SplitRecords;
import org.apache.doris.cdcclient.source.split.AbstractSourceSplit;
import org.apache.doris.cdcclient.source.split.BinlogSplit;
import org.apache.doris.cdcclient.source.split.SnapshotSplit;
import org.apache.doris.cdcclient.utils.ConfigUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.document.Array;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.BinlogSplitReader;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.DebeziumReader;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.SnapshotSplitReader;
import org.apache.flink.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import static org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner.BINLOG_SPLIT_ID;
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
import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MySqlSourceReader implements SourceReader<MySqlSplit, MySqlSplitState> {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceReader.class);
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static final String SPLIT_ID = "splitId";
    private static final String FINISH_SPLITS = "finishSplits";
    private static final String ASSIGNED_SPLITS = "assignedSplits";
    private static final String SNAPSHOT_TABLE = "snapshotTable";
    private static final String PURE_BINLOG_PHASE = "pureBinlogPhase";
    private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new FlinkJsonTableChangeSerializer();
    private SourceRecordDeserializer<SourceRecord, List<String>> serializer;
    private Map<Long, JobRuntimeContext> jobContexts;

    public MySqlSourceReader() {
        this.serializer = new DebeziumJsonDeserializer();
        this.jobContexts = new ConcurrentHashMap<>();
    }

    @Override
    public void initialize() {}

    @Override
    public List<AbstractSourceSplit> getSourceSplits(JobConfig config)
            throws JsonProcessingException {
        MySqlSourceConfig sourceConfig = getSourceConfig(config);
        StartupMode startupMode = sourceConfig.getStartupOptions().startupMode;
        List<MySqlSnapshotSplit> remainingSnapshotSplits = new ArrayList<>();
        MySqlBinlogSplit remainingBinlogSplit = null;
        if (startupMode.equals(StartupMode.INITIAL)) {
            String snapshotTable = config.getConfig().get(SNAPSHOT_TABLE);
            remainingSnapshotSplits =
                    startSplitChunks(sourceConfig, snapshotTable, config.getConfig());
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
                String splitStart =
                        snapshotSplit.getSplitStart() == null
                                ? null
                                : objectMapper.writeValueAsString(snapshotSplit.getSplitStart());
                String splitEnd =
                        snapshotSplit.getSplitEnd() == null
                                ? null
                                : objectMapper.writeValueAsString(snapshotSplit.getSplitEnd());
                String splitKey = snapshotSplit.getSplitKeyType().getFieldNames().get(0);
                SnapshotSplit split =
                        new SnapshotSplit(splitId, tableId, splitKey, splitStart, splitEnd, null);
                splits.add(split);
            }
        } else {
            BinlogOffset startingOffset = remainingBinlogSplit.getStartingOffset();
            BinlogSplit binlogSplit =
                    new BinlogSplit(remainingBinlogSplit.splitId(), startingOffset.getOffset());
            splits.add(binlogSplit);
        }
        return splits;
    }

    /**
     * 1. If the SplitRecords iterator has it, read the iterator directly.
     * 2. If there is a binlogreader, poll it.
     * 3. If there is none, resubmit split. 4. If reload is true, need to
     *    reset binlogSplitReader and submit split.
     */
    @Override
    public RecordWithMeta read(FetchRecordReq fetchRecord) throws Exception {
        SplitReadResult<MySqlSplit, MySqlSplitState> readResult = readSplitRecords(fetchRecord);

        JobConfig jobConfig =
                new JobConfig(
                        fetchRecord.getJobId(),
                        fetchRecord.getDataSource(),
                        fetchRecord.getConfig());
        Map<String, String> offsetMeta = fetchRecord.getMeta();
        RecordWithMeta recordResponse = new RecordWithMeta();

        return buildRecordResponse(fetchRecord, jobConfig, offsetMeta, recordResponse, readResult);
    }

    /**
     * Prepare split information based on current context:
     * - If there is an active split being consumed, reuse it directly;
     * - Otherwise, create a new snapshot/binlog split based on offset and start the reader.
     */
    private SplitProcessingContext prepareSplitProcessingContext(
            JobRuntimeContext jobRuntimeContext,
            JobConfig jobConfig,
            Map<String, String> offsetMeta)
            throws Exception {
        SplitProcessingContext context = new SplitProcessingContext();
        SplitRecords currentSplitRecords = jobRuntimeContext.getCurrentSplitRecords();
        if (currentSplitRecords == null) {
            DebeziumReader<SourceRecords, MySqlSplit> currentReader =
                    jobRuntimeContext.getCurrentReader();
            if (currentReader instanceof BinlogSplitReader) {
                currentSplitRecords = pollSplitRecordsWithCurrentReader(currentReader);
            } else if (currentReader == null) {
                Tuple2<MySqlSplit, Boolean> splitFlag = createMySqlSplit(offsetMeta, jobConfig);
                context.setSplit(splitFlag.f0);
                context.setPureBinlogPhase(splitFlag.f1);
                context.setReadBinlog(!context.getSplit().isSnapshotSplit());
                LOG.info(
                        "Job {} submitted new split {}, readBinlog={}",
                        jobConfig.getJobId(),
                        context.getSplit().splitId(),
                        context.isReadBinlog());
                currentSplitRecords = pollSplitRecordsWithSplit(context.getSplit(), jobConfig);
            } else {
                throw new IllegalStateException(
                        String.format(
                                "Unsupported reader type %s for job %d",
                                currentReader.getClass().getName(), jobConfig.getJobId()));
            }
            jobRuntimeContext.setCurrentSplitRecords(currentSplitRecords);
        }
        context.setSplitRecords(currentSplitRecords);
        return context;
    }

    /**
     * When a client requests a reload, the split is reconstructed and the binlog reader is reset.
     */
    private SplitProcessingContext reloadSplitProcessingContext(
            JobRuntimeContext jobRuntimeContext,
            JobConfig jobConfig,
            Map<String, String> offsetMeta)
            throws Exception {
        SplitProcessingContext context = new SplitProcessingContext();
        Tuple2<MySqlSplit, Boolean> splitFlag = createMySqlSplit(offsetMeta, jobConfig);
        context.setSplit(splitFlag.f0);
        context.setPureBinlogPhase(splitFlag.f1);
        context.setReadBinlog(!context.getSplit().isSnapshotSplit());
        LOG.info(
                "Job {} reload split {}, readBinlog={}",
                jobConfig.getJobId(),
                context.getSplit() == null ? "null" : context.getSplit().splitId(),
                context.isReadBinlog());
        closeBinlogReader(jobConfig.getJobId());
        SplitRecords currentSplitRecords = pollSplitRecordsWithSplit(context.getSplit(), jobConfig);
        jobRuntimeContext.setCurrentSplitRecords(currentSplitRecords);
        context.setSplitRecords(currentSplitRecords);
        return context;
    }

    /** read split records. */
    @Override
    public SplitReadResult<MySqlSplit, MySqlSplitState> readSplitRecords(BaseRecordReq baseReq)
            throws Exception {
        JobConfig jobConfig =
                new JobConfig(baseReq.getJobId(), baseReq.getDataSource(), baseReq.getConfig());
        Map<String, String> offsetMeta = baseReq.getMeta();
        if (offsetMeta == null || offsetMeta.isEmpty()) {
            throw new RuntimeException("miss meta offset");
        }

        JobRuntimeContext jobRuntimeContext = getJobRuntimeContext(jobConfig.getJobId());
        SplitProcessingContext processingContext =
                prepareSplitProcessingContext(jobRuntimeContext, jobConfig, offsetMeta);

        if (baseReq.isReload() && processingContext.getSplit() == null) {
            processingContext =
                    reloadSplitProcessingContext(jobRuntimeContext, jobConfig, offsetMeta);
        }

        return readSplitRecords(baseReq, jobRuntimeContext, processingContext);
    }

    private SplitReadResult<MySqlSplit, MySqlSplitState> readSplitRecords(
            BaseRecordReq baseReq,
            JobRuntimeContext jobRuntimeContext,
            SplitProcessingContext processingContext) {
        int fetchSize = baseReq.getFetchSize();
        SplitRecords currentSplitRecords = processingContext.getSplitRecords();
        boolean readBinlog = processingContext.isReadBinlog();
        boolean pureBinlogPhase = processingContext.isPureBinlogPhase();
        MySqlSplit split = processingContext.getSplit();

        SplitReadResult<MySqlSplit, MySqlSplitState> result = new SplitReadResult<>();
        MySqlSplitState currentSplitState = null;
        if (!readBinlog && split != null) {
            currentSplitState = new MySqlSnapshotSplitState(split.asSnapshotSplit());
        }

        Iterator<SourceRecord> filteredIterator =
                new FilteredRecordIterator(
                        currentSplitRecords,
                        currentSplitState,
                        fetchSize,
                        readBinlog,
                        pureBinlogPhase,
                        result);

        result.setRecordIterator(filteredIterator);
        jobRuntimeContext.setCurrentSplitRecords(null);
        result.setSplitState(currentSplitState);
        result.setReadBinlog(readBinlog);
        result.setPureBinlogPhase(pureBinlogPhase);
        result.setSplit(split);

        // set splitId and defaultOffset
        if (split != null) {
            result.setSplitId(split.splitId());
            if (readBinlog && split instanceof MySqlBinlogSplit) {
                result.setDefaultOffset(split.asBinlogSplit().getStartingOffset().getOffset());
            }
        } else if (readBinlog) {
            result.setSplitId(BINLOG_SPLIT_ID);
        }

        return result;
    }

    /** build RecordWithMeta */
    private RecordWithMeta buildRecordResponse(
            FetchRecordReq fetchRecord,
            JobConfig jobConfig,
            Map<String, String> offsetMeta,
            RecordWithMeta recordResponse,
            SplitReadResult<MySqlSplit, MySqlSplitState> readResult)
            throws Exception {
        boolean readBinlog = readResult.isReadBinlog();
        boolean pureBinlogPhase = readResult.isPureBinlogPhase();
        MySqlSplit split = readResult.getSplit();
        MySqlSplitState currentSplitState = readResult.getSplitState();

        // Serialize records and add them to the response (collect from iterator)
        Iterator<SourceRecord> iterator = readResult.getRecordIterator();
        while (iterator != null && iterator.hasNext()) {
            SourceRecord element = iterator.next();
            List<String> serializedRecords = serializer.deserialize(jobConfig.getConfig(), element);
            if (!CollectionUtils.isEmpty(serializedRecords)) {
                recordResponse.getRecords().addAll(serializedRecords);

                // update meta
                Map<String, String> lastMeta = RecordUtils.getBinlogPosition(element).getOffset();
                if (readBinlog) {
                    lastMeta.put(SPLIT_ID, BINLOG_SPLIT_ID);
                    lastMeta.put(PURE_BINLOG_PHASE, String.valueOf(pureBinlogPhase));
                    recordResponse.setMeta(lastMeta);
                }
            }
        }

        // Set meta information
        if (!readBinlog && currentSplitState != null) {
            BinlogOffset highWatermark =
                    currentSplitState.asSnapshotSplitState().getHighWatermark();
            Map<String, String> offsetRes = highWatermark.getOffset();
            offsetRes.put(SPLIT_ID, split.splitId());
            recordResponse.setMeta(offsetRes);
        }
        if (CollectionUtils.isEmpty(recordResponse.getRecords())) {
            if (readBinlog) {
                Map<String, String> offsetRes = new HashMap<>(offsetMeta);
                if (split != null) {
                    offsetRes = split.asBinlogSplit().getStartingOffset().getOffset();
                }
                offsetRes.put(SPLIT_ID, BINLOG_SPLIT_ID);
                offsetRes.put(PURE_BINLOG_PHASE, String.valueOf(pureBinlogPhase));
                recordResponse.setMeta(offsetRes);
            } else {
                recordResponse.setMeta(fetchRecord.getMeta());
            }
        } else if (readResult.getLastMeta() != null) {
            // If there is meta from the last record, use it
            recordResponse.setMeta(readResult.getLastMeta());
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
        JobRuntimeContext jobContext = jobContexts.get(jobId);
        if (jobContext == null) {
            return;
        }
        Map<TableId, TableChanges.TableChange> tableChangeMap = jobContext.getTableSchemas();
        if (tableChangeMap == null) {
            tableChangeMap = new ConcurrentHashMap<>();
            jobContext.setTableSchemas(tableChangeMap);
        }
        for (TableChanges.TableChange tblChange : changes) {
            tableChangeMap.put(tblChange.getTable().id(), tblChange);
        }
    }

    private Tuple2<MySqlSplit, Boolean> createMySqlSplit(
            Map<String, String> offset, JobConfig jobConfig) throws JsonProcessingException {
        Tuple2<MySqlSplit, Boolean> splitRes = null;
        String splitId = offset.get(SPLIT_ID);
        if (!BINLOG_SPLIT_ID.equals(splitId)) {
            MySqlSnapshotSplit split = createSnapshotSplit(offset, jobConfig);
            splitRes = Tuple2.of(split, false);
        } else {
            splitRes = createBinlogSplit(offset, jobConfig);
        }
        return splitRes;
    }

    private MySqlSnapshotSplit createSnapshotSplit(Map<String, String> offset, JobConfig jobConfig)
            throws JsonProcessingException {
        String splitId = offset.get(SPLIT_ID);
        SnapshotSplit snapshotSplit = objectMapper.convertValue(offset, SnapshotSplit.class);
        TableId tableId = TableId.parse(snapshotSplit.getTableId());
        Object[] splitStart =
                snapshotSplit.getSplitStart() == null
                        ? null
                        : objectMapper.readValue(snapshotSplit.getSplitStart(), Object[].class);
        Object[] splitEnd =
                snapshotSplit.getSplitEnd() == null
                        ? null
                        : objectMapper.readValue(snapshotSplit.getSplitEnd(), Object[].class);
        String splitKey = snapshotSplit.getSplitKey();
        Map<TableId, TableChanges.TableChange> tableSchemas = getTableSchemas(jobConfig);
        TableChanges.TableChange tableChange = tableSchemas.get(tableId);
        Column splitColumn = tableChange.getTable().columnWithName(splitKey);
        RowType splitType = ChunkUtils.getChunkKeyColumnType(splitColumn);
        MySqlSnapshotSplit split =
                new MySqlSnapshotSplit(
                        tableId, splitId, splitType, splitStart, splitEnd, null, tableSchemas);
        return split;
    }

    private Tuple2<MySqlSplit, Boolean> createBinlogSplit(
            Map<String, String> meta, JobConfig config) throws JsonProcessingException {
        MySqlSourceConfig sourceConfig = getSourceConfig(config);
        BinlogOffset offsetConfig = null;
        if (sourceConfig.getStartupOptions() != null) {
            offsetConfig = sourceConfig.getStartupOptions().binlogOffset;
        }

        List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();
        BinlogOffset minOffsetFinishSplits = null;
        BinlogOffset maxOffsetFinishSplits = null;
        if (meta.containsKey(FINISH_SPLITS) && meta.containsKey(ASSIGNED_SPLITS)) {
            // Construct binlogsplit based on the finished split and assigned split.
            String finishSplitsOffset = meta.remove(FINISH_SPLITS);
            String assignedSplits = meta.remove(ASSIGNED_SPLITS);
            Map<String, Map<String, String>> splitFinishedOffsets =
                    objectMapper.readValue(
                            finishSplitsOffset,
                            new TypeReference<Map<String, Map<String, String>>>() {});
            Map<String, SnapshotSplit> assignedSplitsMap =
                    objectMapper.readValue(
                            assignedSplits, new TypeReference<Map<String, SnapshotSplit>>() {});
            List<SnapshotSplit> assignedSplitLists =
                    assignedSplitsMap.values().stream()
                            .sorted(Comparator.comparing(AbstractSourceSplit::getSplitId))
                            .collect(Collectors.toList());

            for (SnapshotSplit split : assignedSplitLists) {
                // find the min binlog offset
                Map<String, String> offsetMap = splitFinishedOffsets.get(split.getSplitId());
                BinlogOffset binlogOffset = new BinlogOffset(offsetMap);
                if (minOffsetFinishSplits == null || binlogOffset.isBefore(minOffsetFinishSplits)) {
                    minOffsetFinishSplits = binlogOffset;
                }
                if (maxOffsetFinishSplits == null || binlogOffset.isAfter(maxOffsetFinishSplits)) {
                    maxOffsetFinishSplits = binlogOffset;
                }
                Object[] splitStart =
                        split.getSplitStart() == null
                                ? null
                                : objectMapper.readValue(split.getSplitStart(), Object[].class);
                Object[] splitEnd =
                        split.getSplitEnd() == null
                                ? null
                                : objectMapper.readValue(split.getSplitEnd(), Object[].class);

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
        MySqlBinlogSplit binlogSplit =
                MySqlBinlogSplit.fillTableSchemas(split.asBinlogSplit(), getTableSchemas(config));
        return Tuple2.of(binlogSplit, pureBinlogPhase);
    }

    private List<MySqlSnapshotSplit> startSplitChunks(
            MySqlSourceConfig sourceConfig, String snapshotTable, Map<String, String> config) {
        List<TableId> remainingTables = new ArrayList<>();
        if (snapshotTable != null) {
            // need add database name
            String database = config.get(LoadConstants.DATABASE_NAME);
            remainingTables.add(TableId.parse(database + "." + snapshotTable));
        }
        List<MySqlSnapshotSplit> remainingSplits = new ArrayList<>();
        MySqlSnapshotSplitAssigner splitAssigner =
                new MySqlSnapshotSplitAssigner(sourceConfig, 1, remainingTables, false);
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
        JobRuntimeContext jobContext = getJobRuntimeContext(jobConfig.getJobId());
        Iterator<SourceRecords> dataIt = null;
        String currentSplitId = null;
        DebeziumReader<SourceRecords, MySqlSplit> currentReader = null;
        LOG.info("Get a split: {}", split.splitId());
        if (split instanceof MySqlSnapshotSplit) {
            currentReader = getSnapshotSplitReader(jobConfig);
        } else if (split instanceof MySqlBinlogSplit) {
            currentReader = getBinlogSplitReader(jobConfig);
        }
        jobContext.setCurrentReader(currentReader);
        currentReader.submitSplit(split);
        currentSplitId = split.splitId();
        // make split record available
        Thread.sleep(100);
        dataIt = currentReader.pollSplitRecords();
        if (currentReader instanceof SnapshotSplitReader) {
            closeSnapshotReader(jobConfig.getJobId());
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
        JobRuntimeContext jobContext = getJobRuntimeContext(config.getJobId());
        SnapshotSplitReader snapshotReader = jobContext.getSnapshotReader();
        if (snapshotReader == null) {
            final MySqlConnection jdbcConnection =
                    DebeziumUtils.createMySqlConnection(sourceConfig);
            final BinaryLogClient binaryLogClient =
                    DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration());
            final StatefulTaskContext statefulTaskContext =
                    new StatefulTaskContext(sourceConfig, binaryLogClient, jdbcConnection);
            snapshotReader = new SnapshotSplitReader(statefulTaskContext, 0);
            jobContext.setSnapshotReader(snapshotReader);
        }
        return snapshotReader;
    }

    private BinlogSplitReader getBinlogSplitReader(JobConfig config) {
        MySqlSourceConfig sourceConfig = getSourceConfig(config);
        JobRuntimeContext jobContext = getJobRuntimeContext(config.getJobId());
        BinlogSplitReader binlogReader = jobContext.getBinlogReader();
        if (binlogReader == null) {
            final MySqlConnection jdbcConnection =
                    DebeziumUtils.createMySqlConnection(sourceConfig);
            final BinaryLogClient binaryLogClient =
                    DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration());
            final StatefulTaskContext statefulTaskContext =
                    new StatefulTaskContext(sourceConfig, binaryLogClient, jdbcConnection);
            binlogReader = new BinlogSplitReader(statefulTaskContext, 0);
            jobContext.setBinlogReader(binlogReader);
        }
        return binlogReader;
    }

    private void closeSnapshotReader(Long jobId) {
        JobRuntimeContext jobContext = jobContexts.get(jobId);
        if (jobContext == null) {
            return;
        }
        SnapshotSplitReader reusedSnapshotReader = jobContext.getSnapshotReader();
        if (reusedSnapshotReader != null) {
            LOG.debug(
                    "Close snapshot reader {}", reusedSnapshotReader.getClass().getCanonicalName());
            reusedSnapshotReader.close();
            DebeziumReader<SourceRecords, MySqlSplit> currentReader = jobContext.getCurrentReader();
            if (reusedSnapshotReader == currentReader) {
                jobContext.setCurrentReader(null);
            }
            jobContext.setSnapshotReader(null);
        }
    }

    private void closeBinlogReader(Long jobId) {
        JobRuntimeContext jobContext = jobContexts.get(jobId);
        if (jobContext == null) {
            return;
        }
        BinlogSplitReader reusedBinlogReader = jobContext.getBinlogReader();
        if (reusedBinlogReader != null) {
            LOG.debug("Close binlog reader {}", reusedBinlogReader.getClass().getCanonicalName());
            reusedBinlogReader.close();
            DebeziumReader<SourceRecords, MySqlSplit> currentReader = jobContext.getCurrentReader();
            if (reusedBinlogReader == currentReader) {
                jobContext.setCurrentReader(null);
            }
            jobContext.setBinlogReader(null);
        }
    }

    private MySqlSourceConfig getSourceConfig(JobConfig config) {
        return ConfigUtil.generateMySqlConfig(config);
    }

    @Override
    public void close(Long jobId) {
        JobRuntimeContext jobContext = jobContexts.remove(jobId);
        if (jobContext == null) {
            return;
        }
        jobContext.close();
        LOG.info("Close source reader for job {}", jobId);
    }

    private Map<TableId, TableChanges.TableChange> getTableSchemas(JobConfig config) {
        JobRuntimeContext jobContext = getJobRuntimeContext(config.getJobId());
        Map<TableId, TableChanges.TableChange> schemas = jobContext.getTableSchemas();
        if (schemas == null) {
            schemas = discoverTableSchemas(config);
            jobContext.setTableSchemas(schemas);
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

    private JobRuntimeContext getJobRuntimeContext(Long jobId) {
        Objects.requireNonNull(jobId, "jobId");
        return jobContexts.computeIfAbsent(jobId, JobRuntimeContext::new);
    }

    /** 临时保存一次 read 调用中与 split 相关的状态，避免大量方法参数。 */
    private static final class SplitProcessingContext {
        private SplitRecords splitRecords;
        private MySqlSplit split;
        private boolean readBinlog = true;
        private boolean pureBinlogPhase = true;

        private SplitRecords getSplitRecords() {
            return splitRecords;
        }

        private void setSplitRecords(SplitRecords splitRecords) {
            this.splitRecords = splitRecords;
        }

        private MySqlSplit getSplit() {
            return split;
        }

        private void setSplit(MySqlSplit split) {
            this.split = split;
        }

        private boolean isReadBinlog() {
            return readBinlog;
        }

        private void setReadBinlog(boolean readBinlog) {
            this.readBinlog = readBinlog;
        }

        private boolean isPureBinlogPhase() {
            return pureBinlogPhase;
        }

        private void setPureBinlogPhase(boolean pureBinlogPhase) {
            this.pureBinlogPhase = pureBinlogPhase;
        }
    }

    private static final class JobRuntimeContext {
        private final long jobId;
        private SnapshotSplitReader snapshotReader;
        private BinlogSplitReader binlogReader;
        private DebeziumReader<SourceRecords, MySqlSplit> currentReader;
        private Map<TableId, TableChanges.TableChange> tableSchemas;
        private SplitRecords currentSplitRecords;

        private JobRuntimeContext(Long jobId) {
            this.jobId = jobId;
        }

        private SnapshotSplitReader getSnapshotReader() {
            return snapshotReader;
        }

        private void setSnapshotReader(SnapshotSplitReader snapshotReader) {
            this.snapshotReader = snapshotReader;
        }

        private BinlogSplitReader getBinlogReader() {
            return binlogReader;
        }

        private void setBinlogReader(BinlogSplitReader binlogReader) {
            this.binlogReader = binlogReader;
        }

        private DebeziumReader<SourceRecords, MySqlSplit> getCurrentReader() {
            return currentReader;
        }

        private void setCurrentReader(DebeziumReader<SourceRecords, MySqlSplit> currentReader) {
            this.currentReader = currentReader;
        }

        private SplitRecords getCurrentSplitRecords() {
            return currentSplitRecords;
        }

        private void setCurrentSplitRecords(SplitRecords currentSplitRecords) {
            this.currentSplitRecords = currentSplitRecords;
        }

        private Map<TableId, TableChanges.TableChange> getTableSchemas() {
            return tableSchemas;
        }

        private void setTableSchemas(Map<TableId, TableChanges.TableChange> tableSchemas) {
            this.tableSchemas = tableSchemas;
        }

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
     * Filtered record iterator that only returns data change records, filtering out watermark, heartbeat and other events.
     * This is a private static inner class that encapsulates record filtering logic, making the main method cleaner.
     */
    private static class FilteredRecordIterator implements Iterator<SourceRecord> {
        private final Iterator<SourceRecord> sourceIterator;
        private final MySqlSplitState splitState;
        private final int fetchSize;
        private final boolean readBinlog;
        private final boolean pureBinlogPhase;
        private final SplitReadResult<MySqlSplit, MySqlSplitState> result;

        private SourceRecord nextRecord;
        private int count = 0;

        FilteredRecordIterator(
                SplitRecords currentSplitRecords,
                MySqlSplitState splitState,
                int fetchSize,
                boolean readBinlog,
                boolean pureBinlogPhase,
                SplitReadResult<MySqlSplit, MySqlSplitState> result) {
            this.sourceIterator =
                    currentSplitRecords != null && !currentSplitRecords.isEmpty()
                            ? currentSplitRecords.getIterator()
                            : null;
            this.splitState = splitState;
            this.fetchSize = fetchSize;
            this.readBinlog = readBinlog;
            this.pureBinlogPhase = pureBinlogPhase;
            this.result = result;
        }

        @Override
        public boolean hasNext() {
            if (sourceIterator == null) {
                return false;
            }
            if (nextRecord != null) {
                return true;
            }
            if (count >= fetchSize) {
                return false;
            }

            while (sourceIterator.hasNext()) {
                SourceRecord element = sourceIterator.next();
                if (RecordUtils.isWatermarkEvent(element)) {
                    BinlogOffset watermark = RecordUtils.getWatermark(element);
                    if (RecordUtils.isHighWatermarkEvent(element) && splitState != null) {
                        splitState.asSnapshotSplitState().setHighWatermark(watermark);
                    }
                } else if (RecordUtils.isHeartbeatEvent(element)) {
                    LOG.debug("Receive heartbeat event: {}", element);
                } else if (RecordUtils.isDataChangeRecord(element)) {
                    nextRecord = element;
                    count++;

                    // update meta
                    Map<String, String> lastMeta =
                            RecordUtils.getBinlogPosition(element).getOffset();
                    if (readBinlog) {
                        lastMeta.put(SPLIT_ID, BINLOG_SPLIT_ID);
                        lastMeta.put(PURE_BINLOG_PHASE, String.valueOf(pureBinlogPhase));
                        result.setLastMeta(lastMeta);
                    }
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
