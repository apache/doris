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

package org.apache.doris.cdcloader.mysql.loader;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.debezium.connector.mysql.MySqlConnection;
import org.apache.doris.cdcloader.mysql.config.LoaderOptions;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.BinlogSplitReader;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.DebeziumReader;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.SnapshotSplitReader;
import org.apache.flink.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

import static org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner.BINLOG_SPLIT_ID;

public class MySqlSplitReader implements SplitReader {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlSplitReader.class);
    private BlockingQueue<MySqlSplit> splits;
    private BlockingQueue<SplitRecords> elementsQueue;
    private BinlogSplitReader binlogReader;
    private SnapshotSplitReader snapshotSplitReader;
    private MySqlSourceConfig sourceConfig;
    @Nullable
    private SnapshotSplitReader reusedSnapshotReader;
    @Nullable private BinlogSplitReader reusedBinlogReader;
    private DebeziumReader<SourceRecords, MySqlSplit> currentReader = null;
    private DorisRecordSerializer<SourceRecord> serializer;
    private int maxRows = 100;
    private SplitAssigner splitAssigner;
    private ExecutorService executor;
    private AtomicBoolean started;
    public MySqlSplitReader(LoaderOptions options, SplitAssigner splitAssigner) {
        this.sourceConfig = options.generateMySqlConfig();
        BinaryLogClient binaryLogClient = DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration());
        MySqlConnection mySqlConnection = DebeziumUtils.createMySqlConnection(sourceConfig);
        StatefulTaskContext statefulTaskContext = new StatefulTaskContext(sourceConfig, binaryLogClient, mySqlConnection);
        this.snapshotSplitReader = new SnapshotSplitReader(statefulTaskContext, 0);
        this.binlogReader = new BinlogSplitReader(statefulTaskContext, 0);
        this.splits = LoadContext.getInstance().getSplits();
        this.serializer = new CsvSerializer();
        this.splitAssigner = splitAssigner;
        this.elementsQueue = LoadContext.getInstance().getElementsQueue();
        this.started = new AtomicBoolean(false);
    }

    @Override
    public void start() {
        if (!started.get()) {
            if (executor == null) {
                ThreadFactory threadFactory =
                    new ThreadFactoryBuilder().setNameFormat("fetch-records").build();
                executor = Executors.newSingleThreadExecutor(threadFactory);
                started.set(true);
                executor.submit(this::fetch);
                LOG.info("split reader is started.");
            }
        }
    }

    private void fetch() {
        try {
            while (started.get()) {
                SplitRecords splitRecords = pollSplitRecords();
                if (splitRecords != null && !splitRecords.isEmpty()) {
                    LOG.info("put split records: {}", splitRecords);
                    elementsQueue.put(splitRecords);
                }
            }
        } catch (Exception e) {
            LOG.error("fetch records error", e);
            started.set(false);
        }
    }

    @Override
    public List<DorisRecord> fetchRecords(int fetchSize, boolean schedule) throws IOException {
        List<DorisRecord> result = new ArrayList<>();
        SplitRecords splitRecords = elementsQueue.peek();
        if(splitRecords == null || splitRecords.isEmpty()){
            return result;
        }
        String currentSplitId = splitRecords.getSplitId();
        if(schedule){
            splitRecords.setIterator(splitRecords.getRecords().iterator());
        }
        Iterator<SourceRecord> recordIt = splitRecords.getIterator();
        MySqlSplitState currentSplitState = splitAssigner.getSplitStates().get(currentSplitId);

        while (recordIt != null && recordIt.hasNext()) {
            SourceRecord element = recordIt.next();
            LOG.info("Read record: {}", element);
            if (RecordUtils.isWatermarkEvent(element)) {
                BinlogOffset watermark = RecordUtils.getWatermark(element);
                if (RecordUtils.isHighWatermarkEvent(element)) {
                    currentSplitState.asSnapshotSplitState().setHighWatermark(watermark);
                }
            }else if (RecordUtils.isHeartbeatEvent(element)) {
                updateStartingOffsetForSplit(currentSplitState, element);
            } else if (RecordUtils.isDataChangeRecord(element)) {
                updateStartingOffsetForSplit(currentSplitState, element);
                DorisRecord serialize = serializer.serialize(element);
                result.add(serialize);
//                if(result.size() > fetchSize && currentSplitState.toMySqlSplit().isBinlogSplit()){
//                    return result;
//                }
                if(result.size() >= fetchSize){
                    return result;
                }
                //todo: 当大于max_batch_size时，更新状态？
            } else {
                // unknown element
                System.out.println("Meet unknown element {}, just skip." + element);
            }
        }
        if(currentSplitState.toMySqlSplit().isSnapshotSplit()){
            BinlogOffset highWatermark = currentSplitState.asSnapshotSplitState().getHighWatermark();
            splitAssigner.snapshotSplitFinished(currentSplitState.toMySqlSplit(), highWatermark);
        }
        //remove the finished split
        elementsQueue.poll();
        return result;
    }

    private void updateStartingOffsetForSplit(MySqlSplitState splitState, SourceRecord element) {
        if (splitState.isBinlogSplitState()) {
            BinlogOffset position = RecordUtils.getBinlogPosition(element);
            splitState.asBinlogSplitState().setStartingOffset(position);
        }
    }

    private SplitRecords pollSplitRecords() throws Exception {
        Iterator<SourceRecords> dataIt = null;
        String currentSplitId = null;
        if (currentReader == null) {
            if(splits.size() > 0){
                MySqlSplit nextSplit = splits.poll();
                if (nextSplit instanceof MySqlSnapshotSplit) {
                    currentReader = getSnapshotSplitReader();
                } else if (nextSplit instanceof MySqlBinlogSplit) {
                    currentReader = getBinlogSplitReader();
                }
                currentReader.submitSplit(nextSplit);
                currentSplitId = nextSplit.splitId();
            } else {
                //LOG.info("No available split to read.");
                return null;
            }
            dataIt = currentReader.pollSplitRecords();
            if(currentReader instanceof SnapshotSplitReader){
                closeSnapshotReader();
            }
            return dataIt == null ? null : new SplitRecords(currentSplitId, dataIt.next());
        } else if (currentReader instanceof SnapshotSplitReader) {
            dataIt = currentReader.pollSplitRecords();
            if (dataIt != null) {
                // first fetch data of snapshot split, return and emit the records of snapshot split
                MySqlSplit nextSplit = splits.poll();
                if (nextSplit != null) {
                    currentReader.submitSplit(nextSplit);
                    currentSplitId = nextSplit.splitId();
                } else {
                    closeSnapshotReader();
                }
                return new SplitRecords(currentSplitId, dataIt.next());
            }
            return null;
        } else if (currentReader instanceof BinlogSplitReader) {
            // (3) switch to snapshot split reading if there are newly added snapshot splits
            dataIt = currentReader.pollSplitRecords();
            if (dataIt != null) {
                // try to switch to read snapshot split if there are new added snapshot
                MySqlSplit nextSplit = splits.poll();
                if (nextSplit != null) {
                    currentSplitId = nextSplit.splitId();
//                    closeBinlogReader();
//                    LOG.info("It's turn to switch next fetch reader to snapshot split reader");
//                    currentReader = getSnapshotSplitReader();
//                    currentReader.submitSplit(nextSplit);
                    return new SplitRecords(currentSplitId, dataIt.next());
                }
                return new SplitRecords(BINLOG_SPLIT_ID, dataIt.next());
            } else {
                // null will be returned after receiving suspend binlog event
                // finish current binlog split reading
                closeBinlogReader();
                return null;
            }
        } else {
            throw new IllegalStateException("Unsupported reader type.");
        }
    }

    private SnapshotSplitReader getSnapshotSplitReader() {
        if (reusedSnapshotReader == null) {
            final MySqlConnection jdbcConnection =
                DebeziumUtils.createMySqlConnection(sourceConfig);
            final BinaryLogClient binaryLogClient =
                DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration());
            final StatefulTaskContext statefulTaskContext =
                new StatefulTaskContext(sourceConfig, binaryLogClient, jdbcConnection);
            reusedSnapshotReader =
                new SnapshotSplitReader(statefulTaskContext, 0);
        }
        return reusedSnapshotReader;
    }

    private BinlogSplitReader getBinlogSplitReader() {
        if (reusedBinlogReader == null) {
            final MySqlConnection jdbcConnection =
                DebeziumUtils.createMySqlConnection(sourceConfig);
            final BinaryLogClient binaryLogClient =
                DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration());
            final StatefulTaskContext statefulTaskContext =
                new StatefulTaskContext(sourceConfig, binaryLogClient, jdbcConnection);
            reusedBinlogReader = new BinlogSplitReader(statefulTaskContext, 0);
        }
        return reusedBinlogReader;
    }

    private void closeSnapshotReader() {
        if (reusedSnapshotReader != null) {
            LOG.debug(
                "Close snapshot reader {}", reusedSnapshotReader.getClass().getCanonicalName());
            reusedSnapshotReader.close();
            if (reusedSnapshotReader == currentReader) {
                currentReader = null;
            }
            reusedSnapshotReader = null;
        }
    }

    private void closeBinlogReader() {
        if (reusedBinlogReader != null) {
            LOG.debug("Close binlog reader {}", reusedBinlogReader.getClass().getCanonicalName());
            reusedBinlogReader.close();
            if (reusedBinlogReader == currentReader) {
                currentReader = null;
            }
            reusedBinlogReader = null;
        }
    }

    @Override
    public boolean isStarted() {
        return started.get();
    }

    @Override
    public void close() {
        if(started.get()){
            if(executor != null){
                executor.shutdown();
                closeSnapshotReader();
                closeBinlogReader();
            }
            started.set(false);
        }
    }
}
