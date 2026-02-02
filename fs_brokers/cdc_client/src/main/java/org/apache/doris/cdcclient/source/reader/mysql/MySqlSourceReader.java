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

import org.apache.doris.cdcclient.common.Constants;
import org.apache.doris.cdcclient.source.deserialize.DebeziumJsonDeserializer;
import org.apache.doris.cdcclient.source.deserialize.SourceRecordDeserializer;
import org.apache.doris.cdcclient.source.factory.DataSource;
import org.apache.doris.cdcclient.source.reader.SourceReader;
import org.apache.doris.cdcclient.source.reader.SplitReadResult;
import org.apache.doris.cdcclient.source.reader.SplitRecords;
import org.apache.doris.cdcclient.utils.ConfigUtil;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.cdc.request.CompareOffsetRequest;
import org.apache.doris.job.cdc.request.FetchTableSplitsRequest;
import org.apache.doris.job.cdc.request.JobBaseConfig;
import org.apache.doris.job.cdc.request.JobBaseRecordRequest;
import org.apache.doris.job.cdc.split.AbstractSourceSplit;
import org.apache.doris.job.cdc.split.BinlogSplit;
import org.apache.doris.job.cdc.split.SnapshotSplit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.api.connector.source.SourceSplit;
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
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffsetUtils;
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
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
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
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.doris.cdcclient.utils.ConfigUtil.is13Timestamp;
import static org.apache.doris.cdcclient.utils.ConfigUtil.isJson;
import static org.apache.doris.cdcclient.utils.ConfigUtil.toStringMap;
import static org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner.BINLOG_SPLIT_ID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.mysql.cj.conf.ConnectionUrl;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.document.Array;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class MySqlSourceReader implements SourceReader {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceReader.class);
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new FlinkJsonTableChangeSerializer();
    private SourceRecordDeserializer<SourceRecord, List<String>> serializer;
    private DebeziumReader<SourceRecords, MySqlSplit> currentReader;
    private Map<TableId, TableChanges.TableChange> tableSchemas;
    private SplitRecords currentSplitRecords;
    private MySqlSplit currentSplit;

    public MySqlSourceReader() {
        this.serializer = new DebeziumJsonDeserializer();
    }

    @Override
    public void initialize(String jobId, DataSource dataSource, Map<String, String> config) {
        this.serializer.init(config);
    }

    @Override
    public List<AbstractSourceSplit> getSourceSplits(FetchTableSplitsRequest ftsReq) {
        LOG.info("Get table {} splits for job {}", ftsReq.getSnapshotTable(), ftsReq.getJobId());
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

    @Override
    public SplitReadResult readSplitRecords(JobBaseRecordRequest baseReq) throws Exception {
        Map<String, Object> offsetMeta = baseReq.getMeta();
        if (offsetMeta == null || offsetMeta.isEmpty()) {
            throw new RuntimeException("miss meta offset");
        }
        // Create a new snapshot/binlog split based on offset and start the reader.
        LOG.info("create new split reader for {} with offset {}", baseReq.getJobId(), offsetMeta);
        // build split
        Tuple2<MySqlSplit, Boolean> splitFlag = createMySqlSplit(offsetMeta, baseReq);
        MySqlSplit split = splitFlag.f0;
        // it's necessary to ensure that the binlog reader is already closed.
        this.currentSplitRecords = pollSplitRecordsWithSplit(split, baseReq);
        this.currentSplit = split;

        // build response with iterator
        SplitReadResult result = new SplitReadResult();
        MySqlSplitState currentSplitState = null;
        MySqlSplit currentSplit = this.getCurrentSplit();
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
        Map<TableId, TableChanges.TableChange> tableChangeMap = this.getTableSchemas();
        if (tableChangeMap == null) {
            tableChangeMap = new ConcurrentHashMap<>();
            this.setTableSchemas(tableChangeMap);
        }
        for (TableChanges.TableChange tblChange : changes) {
            tableChangeMap.put(tblChange.getTable().id(), tblChange);
        }
    }

    private Tuple2<MySqlSplit, Boolean> createMySqlSplit(
            Map<String, Object> offsetMeta, JobBaseConfig jobConfig)
            throws JsonProcessingException {
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

    private MySqlSnapshotSplit createSnapshotSplit(
            Map<String, Object> offset, JobBaseConfig jobConfig) throws JsonProcessingException {
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
            Map<String, Object> meta, JobBaseConfig config) {
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
            String database = config.get(DataSourceConfigKeys.DATABASE);
            remainingTables.add(TableId.parse(database + "." + snapshotTable));
        }
        List<MySqlSnapshotSplit> remainingSplits = new ArrayList<>();
        MySqlSnapshotSplitAssigner splitAssigner =
                new MySqlSnapshotSplitAssigner(
                        sourceConfig, 1, remainingTables, false, new MockSplitEnumeratorContext(1));
        splitAssigner.open();
        try {
            while (true) {
                Optional<MySqlSplit> mySqlSplit = splitAssigner.getNext();
                if (mySqlSplit.isPresent()) {
                    MySqlSnapshotSplit snapshotSplit = mySqlSplit.get().asSnapshotSplit();
                    remainingSplits.add(snapshotSplit);
                } else {
                    break;
                }
            }
        } finally {
            // splitAssigner.close();
            closeChunkSplitterOnly(splitAssigner);
        }
        return remainingSplits;
    }

    /**
     * The JdbcConnectionPools inside MySqlSnapshotSplitAssigner are singletons. Calling
     * MySqlSnapshotSplitAssigner.close() closes the entire JdbcConnectionPools, which can cause
     * problems under high concurrency. This only closes the connection of the current
     * MySqlSnapshotSplitAssigner.
     */
    private void closeChunkSplitterOnly(MySqlSnapshotSplitAssigner splitAssigner) {
        try {
            // call closeExecutorService()
            java.lang.reflect.Method closeExecutorMethod =
                    MySqlSnapshotSplitAssigner.class.getDeclaredMethod("closeExecutorService");
            closeExecutorMethod.setAccessible(true);
            closeExecutorMethod.invoke(splitAssigner);

            // call chunkSplitter.close()
            java.lang.reflect.Field field =
                    MySqlSnapshotSplitAssigner.class.getDeclaredField("chunkSplitter");
            field.setAccessible(true);
            Object chunkSplitter = field.get(splitAssigner);

            if (chunkSplitter != null) {
                java.lang.reflect.Method closeMethod = chunkSplitter.getClass().getMethod("close");
                closeMethod.invoke(chunkSplitter);
                LOG.info("Closed chunkSplitter JDBC connection");
            }
        } catch (Exception e) {
            LOG.warn("Failed to close chunkSplitter via reflection,", e);
        }
    }

    private SplitRecords pollSplitRecordsWithSplit(MySqlSplit split, JobBaseConfig jobConfig)
            throws Exception {
        Preconditions.checkState(split != null, "split is null");
        SourceRecords sourceRecords = null;
        String currentSplitId = null;
        DebeziumReader<SourceRecords, MySqlSplit> currentReader = null;
        LOG.info("Get a split: {}", split.toString());
        if (split instanceof MySqlSnapshotSplit) {
            currentReader = getSnapshotSplitReader(jobConfig);
        } else if (split instanceof MySqlBinlogSplit) {
            currentReader = getBinlogSplitReader(jobConfig);
        }
        this.setCurrentReader(currentReader);
        currentReader.submitSplit(split);
        currentSplitId = split.splitId();
        // make split record available
        sourceRecords =
                pollUntilDataAvailable(currentReader, Constants.POLL_SPLIT_RECORDS_TIMEOUTS, 500);
        if (currentReader instanceof SnapshotSplitReader) {
            closeCurrentReader();
        }
        return new SplitRecords(currentSplitId, sourceRecords.iterator());
    }

    /** Poll data from the current reader, only for binlog reader */
    private SplitRecords pollSplitRecordsWithCurrentReader(
            DebeziumReader<SourceRecords, MySqlSplit> currentReader) throws Exception {
        Iterator<SourceRecords> dataIt = null;
        if (currentReader instanceof BinlogSplitReader) {
            dataIt = currentReader.pollSplitRecords();
            return dataIt == null
                    ? null
                    : new SplitRecords(BINLOG_SPLIT_ID, dataIt.next().iterator());
        } else {
            throw new IllegalStateException("Unsupported reader type.");
        }
    }

    /**
     * Split tasks are submitted asynchronously, and data is sent to the Debezium queue. Therefore,
     * there will be a time interval between retrieving data; it's necessary to fetch data until the
     * queue has data.
     */
    private SourceRecords pollUntilDataAvailable(
            DebeziumReader<SourceRecords, MySqlSplit> reader,
            long maxWaitTimeMs,
            long pollIntervalMs)
            throws InterruptedException {
        long startTime = System.currentTimeMillis();
        long elapsedTime = 0;
        int attemptCount = 0;
        LOG.info("Polling until data available");
        Iterator<SourceRecords> lastDataIt = null;
        while (elapsedTime < maxWaitTimeMs) {
            attemptCount++;
            lastDataIt = reader.pollSplitRecords();
            if (lastDataIt != null && lastDataIt.hasNext()) {
                SourceRecords sourceRecords = lastDataIt.next();
                if (sourceRecords != null && !sourceRecords.getSourceRecordList().isEmpty()) {
                    LOG.info(
                            "Data available after {} ms ({} attempts). {} Records received.",
                            elapsedTime,
                            attemptCount,
                            sourceRecords.getSourceRecordList().size());
                    // todo: Until debezium_heartbeat is consumed
                    return sourceRecords;
                }
            }

            // No records yet, continue polling
            if (elapsedTime + pollIntervalMs < maxWaitTimeMs) {
                Thread.sleep(pollIntervalMs);
                elapsedTime = System.currentTimeMillis() - startTime;
            } else {
                // Last attempt before timeout
                break;
            }
        }

        LOG.warn(
                "Timeout: No data (heartbeat or data change) received after {} ms ({} attempts).",
                elapsedTime,
                attemptCount);
        return new SourceRecords(new ArrayList<>());
    }

    private SnapshotSplitReader getSnapshotSplitReader(JobBaseConfig config) {
        MySqlSourceConfig sourceConfig = getSourceConfig(config);
        final MySqlConnection jdbcConnection = DebeziumUtils.createMySqlConnection(sourceConfig);
        final BinaryLogClient binaryLogClient =
                DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration());
        final StatefulTaskContext statefulTaskContext =
                new StatefulTaskContext(sourceConfig, binaryLogClient, jdbcConnection);
        SnapshotSplitReader snapshotReader = new SnapshotSplitReader(statefulTaskContext, 0);
        return snapshotReader;
    }

    private BinlogSplitReader getBinlogSplitReader(JobBaseConfig config) {
        MySqlSourceConfig sourceConfig = getSourceConfig(config);
        final MySqlConnection jdbcConnection = DebeziumUtils.createMySqlConnection(sourceConfig);
        final BinaryLogClient binaryLogClient =
                DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration());
        final StatefulTaskContext statefulTaskContext =
                new StatefulTaskContext(sourceConfig, binaryLogClient, jdbcConnection);
        BinlogSplitReader binlogReader = new BinlogSplitReader(statefulTaskContext, 0);
        return binlogReader;
    }

    private void closeCurrentReader() {
        DebeziumReader<SourceRecords, MySqlSplit> currentReader = this.getCurrentReader();
        if (currentReader != null) {
            LOG.info("Close current reader {}", currentReader.getClass().getCanonicalName());
            currentReader.close();
        }
        this.setCurrentReader(null);
    }

    private MySqlSourceConfig getSourceConfig(JobBaseConfig config) {
        return generateMySqlConfig(config);
    }

    /** Generate MySQL source config from JobBaseConfig */
    private MySqlSourceConfig generateMySqlConfig(JobBaseConfig config) {
        return generateMySqlConfig(config.getConfig(), ConfigUtil.getServerId(config.getJobId()));
    }

    /** Generate MySQL source config from Map config */
    private MySqlSourceConfig generateMySqlConfig(Map<String, String> cdcConfig, String serverId) {
        MySqlSourceConfigFactory configFactory = new MySqlSourceConfigFactory();
        ConnectionUrl cu =
                ConnectionUrl.getConnectionUrlInstance(
                        cdcConfig.get(DataSourceConfigKeys.JDBC_URL), null);
        configFactory.hostname(cu.getMainHost().getHost());
        configFactory.port(cu.getMainHost().getPort());
        configFactory.username(cdcConfig.get(DataSourceConfigKeys.USER));
        configFactory.password(cdcConfig.get(DataSourceConfigKeys.PASSWORD));
        String databaseName = cdcConfig.get(DataSourceConfigKeys.DATABASE);
        configFactory.databaseList(databaseName);
        configFactory.serverId(serverId);
        configFactory.serverTimeZone(
                ConfigUtil.getTimeZoneFromProps(cu.getOriginalProperties()).toString());

        configFactory.includeSchemaChanges(false);

        // Set table list
        String[] tableList = ConfigUtil.getTableList(databaseName, cdcConfig);
        com.google.common.base.Preconditions.checkArgument(
                tableList.length >= 1, "include_tables or table is required");
        configFactory.tableList(tableList);

        // setting startMode
        String startupMode = cdcConfig.get(DataSourceConfigKeys.OFFSET);
        if (DataSourceConfigKeys.OFFSET_INITIAL.equalsIgnoreCase(startupMode)) {
            configFactory.startupOptions(StartupOptions.initial());
        } else if (DataSourceConfigKeys.OFFSET_EARLIEST.equalsIgnoreCase(startupMode)) {
            configFactory.startupOptions(StartupOptions.earliest());
            BinlogOffset binlogOffset =
                    initializeEffectiveOffset(
                            configFactory, StartupOptions.earliest().binlogOffset);
            configFactory.startupOptions(StartupOptions.specificOffset(binlogOffset));
        } else if (DataSourceConfigKeys.OFFSET_LATEST.equalsIgnoreCase(startupMode)) {
            configFactory.startupOptions(StartupOptions.latest());
            BinlogOffset binlogOffset =
                    initializeEffectiveOffset(configFactory, StartupOptions.latest().binlogOffset);
            configFactory.startupOptions(StartupOptions.specificOffset(binlogOffset));
        } else if (isJson(startupMode)) {
            // start from specific offset
            Map<String, String> offsetMap = toStringMap(startupMode);
            if (MapUtils.isEmpty(offsetMap)) {
                throw new RuntimeException("Incorrect offset " + startupMode);
            }
            if (offsetMap.containsKey(BinlogOffset.BINLOG_FILENAME_OFFSET_KEY)
                    && offsetMap.containsKey(BinlogOffset.BINLOG_POSITION_OFFSET_KEY)) {
                BinlogOffset binlogOffset =
                        BinlogOffset.builder()
                                .setBinlogFilePosition(
                                        offsetMap.get(BinlogOffset.BINLOG_FILENAME_OFFSET_KEY),
                                        Long.parseLong(
                                                offsetMap.get(
                                                        BinlogOffset.BINLOG_POSITION_OFFSET_KEY)))
                                .build();
                configFactory.startupOptions(StartupOptions.specificOffset(binlogOffset));
            } else {
                throw new RuntimeException("Incorrect offset " + startupMode);
            }
        } else if (is13Timestamp(startupMode)) {
            // start from timestamp
            Long ts = Long.parseLong(startupMode);
            BinlogOffset binlogOffset =
                    initializeEffectiveOffset(
                            configFactory, StartupOptions.timestamp(ts).binlogOffset);
            configFactory.startupOptions(StartupOptions.specificOffset(binlogOffset));
        } else {
            throw new RuntimeException("Unknown offset " + startupMode);
        }

        Properties jdbcProperteis = new Properties();
        jdbcProperteis.putAll(cu.getOriginalProperties());
        configFactory.jdbcProperties(jdbcProperteis);

        Properties dbzProps = ConfigUtil.getDefaultDebeziumProps();
        configFactory.debeziumProperties(dbzProps);
        if (cdcConfig.containsKey(DataSourceConfigKeys.SPLIT_SIZE)) {
            configFactory.splitSize(
                    Integer.parseInt(cdcConfig.get(DataSourceConfigKeys.SPLIT_SIZE)));
        }

        return configFactory.createConfig(0);
    }

    private BinlogOffset initializeEffectiveOffset(
            MySqlSourceConfigFactory configFactory, BinlogOffset binlogOffset) {
        MySqlSourceConfig config = configFactory.createConfig(0);
        try (MySqlConnection connection = DebeziumUtils.createMySqlConnection(config)) {
            return BinlogOffsetUtils.initializeEffectiveOffset(binlogOffset, connection, config);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, String> extractSnapshotStateOffset(Object splitState) {
        Preconditions.checkNotNull(splitState, "splitState is null");
        MySqlSplitState mysqlSplitState = (MySqlSplitState) splitState;
        BinlogOffset highWatermark = mysqlSplitState.asSnapshotSplitState().getHighWatermark();
        Map<String, String> offsetRes = new HashMap<>(highWatermark.getOffset());
        return offsetRes;
    }

    @Override
    public Map<String, String> extractBinlogStateOffset(Object splitState) {
        Preconditions.checkNotNull(splitState, "splitState is null");
        MySqlSplitState mysqlSplitState = (MySqlSplitState) splitState;
        BinlogOffset startingOffset = mysqlSplitState.asBinlogSplitState().getStartingOffset();
        return new HashMap<>(startingOffset.getOffset());
    }

    @Override
    public Map<String, String> extractBinlogOffset(SourceSplit split) {
        Preconditions.checkNotNull(split, "split is null");
        MySqlSplit mysqlSplit = (MySqlSplit) split;
        Map<String, String> offsetRes =
                new HashMap<>(mysqlSplit.asBinlogSplit().getStartingOffset().getOffset());
        return offsetRes;
    }

    @Override
    public boolean isBinlogSplit(SourceSplit split) {
        Preconditions.checkNotNull(split, "split is null");
        MySqlSplit mysqlSplit = (MySqlSplit) split;
        return mysqlSplit.isBinlogSplit();
    }

    @Override
    public boolean isSnapshotSplit(SourceSplit split) {
        Preconditions.checkNotNull(split, "split is null");
        MySqlSplit mysqlSplit = (MySqlSplit) split;
        return mysqlSplit.isSnapshotSplit();
    }

    @Override
    public void finishSplitRecords() {
        this.setCurrentSplitRecords(null);
        // Close after each read, the binlog client will occupy the connection.
        closeCurrentReader();
    }

    @Override
    public Map<String, String> getEndOffset(JobBaseConfig jobConfig) {
        MySqlSourceConfig sourceConfig = getSourceConfig(jobConfig);
        try (MySqlConnection jdbc = DebeziumUtils.createMySqlConnection(sourceConfig)) {
            BinlogOffset binlogOffset = DebeziumUtils.currentBinlogOffset(jdbc);
            return binlogOffset.getOffset();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public int compareOffset(CompareOffsetRequest compareOffsetRequest) {
        Map<String, String> offsetFirst = compareOffsetRequest.getOffsetFirst();
        Map<String, String> offsetSecond = compareOffsetRequest.getOffsetSecond();
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

    private Map<TableId, TableChanges.TableChange> getTableSchemas(JobBaseConfig config) {
        Map<TableId, TableChanges.TableChange> schemas = this.getTableSchemas();
        if (schemas == null) {
            schemas = discoverTableSchemas(config);
            this.setTableSchemas(schemas);
        }
        return schemas;
    }

    private Map<TableId, TableChanges.TableChange> discoverTableSchemas(JobBaseConfig config) {
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

    @Override
    public void close(JobBaseConfig jobConfig) {
        LOG.info("Close source reader for job {}", jobConfig.getJobId());
        closeCurrentReader();
        currentReader = null;
        currentSplitRecords = null;
        if (tableSchemas != null) {
            tableSchemas.clear();
            tableSchemas = null;
        }
    }

    @Override
    public List<String> deserialize(Map<String, String> config, SourceRecord element)
            throws IOException {
        return serializer.deserialize(config, element);
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
                    if (splitState.isBinlogSplitState()) {
                        BinlogOffset position = RecordUtils.getBinlogPosition(element);
                        splitState.asBinlogSplitState().setStartingOffset(position);
                    }
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
