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

package org.apache.doris.load.sync.canal;

import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.load.sync.position.EntryPosition;
import org.apache.doris.load.sync.model.Events;
import org.apache.doris.load.sync.position.PositionMeta;
import org.apache.doris.load.sync.position.PositionRange;
import org.apache.doris.load.sync.SyncChannelHandle;
import org.apache.doris.load.sync.SyncDataConsumer;
import org.apache.doris.load.sync.SyncFailMsg;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class CanalSyncDataConsumer extends SyncDataConsumer {
    private static Logger logger = LogManager.getLogger(CanalSyncDataConsumer.class);

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final long MIN_COMMIT_EVENT_SIZE = Config.min_sync_commit_size;
    private static final long MIN_COMMIT_MEM_SIZE = Config.min_bytes_sync_commit;
    private static final long MAX_COMMIT_MEM_SIZE = Config.max_bytes_sync_commit;

    private CanalSyncJob syncJob;
    private CanalConnector connector;
    private Map<Long, CanalSyncChannel> idToChannels;
    private Set<Long> ackBatches;
    private PositionMeta<EntryPosition> positionMeta;
    private LinkedBlockingQueue<Events<CanalEntry.Entry, EntryPosition>> dataBlockingQueue;
    private SyncChannelHandle handle;
    private ReentrantLock getLock;
    private int sleepTimeMs;
    private long commitIntervalSecond;

    public void setChannels(Map<Long, CanalSyncChannel> idToChannels) {
        for (CanalSyncChannel channel : idToChannels.values()) {
            this.positionMeta.setCommitPosition(channel.getId(), EntryPosition.MIN_POS);
            channel.setCallback(handle);
        }
        this.idToChannels = idToChannels;
    }

    public CanalSyncDataConsumer(CanalSyncJob syncJob, CanalConnector connector, ReentrantLock getLock, boolean debug) {
        super(debug);
        this.syncJob = syncJob;
        this.connector = connector;
        this.dataBlockingQueue = Queues.newLinkedBlockingQueue(1024);
        this.ackBatches = Sets.newHashSet();
        this.positionMeta = new PositionMeta<>();
        this.getLock = getLock;
        this.handle = new SyncChannelHandle();
        this.commitIntervalSecond = Config.sync_commit_interval_second;
        this.sleepTimeMs = 500;
    }

    public void stop(boolean needCleanUp) {
        super.stop();
        if (needCleanUp) {
            cleanUp();
        }
    }

    @Override
    public void beginForTxn() {
        handle.reset(idToChannels.size());
        for (CanalSyncChannel channel : idToChannels.values()) {
            channel.initTxn(Config.max_stream_load_timeout_second);
            handle.mark(channel);
        }
    }

    @Override
    public void abortForTxn(String reason) {
        abortForTxn(idToChannels.values(), reason);
    }

    public void abortForTxn(Collection<CanalSyncChannel> channels, String reason) {
        logger.info("client is aborting transactions. JobId: {}, reason: {}", syncJob.getId(), reason);
        for (CanalSyncChannel channel : channels) {
            if (channel.isTxnBegin()) {
                try {
                    channel.abortTxn(reason);
                } catch (Exception e) {
                    logger.warn("Abort channel failed. jobId: {}，channel: {}, target: {}, msg: {}",
                            syncJob.getId(), channel.getId(), channel.getTargetTable(), e.getMessage());
                }
            }
        }
        rollback();
    }

    @Override
    public void commitForTxn() {
        logger.info("client is committing transactions. JobId: {}", syncJob.getId());
        boolean success = true;
        EntryPosition latestPosition = positionMeta.getLatestPosition();
        for (CanalSyncChannel channel : idToChannels.values()) {
            if (channel.isTxnBegin()) {
                try {
                    channel.commitTxn();
                    this.positionMeta.setCommitPosition(channel.getId(), latestPosition);
                } catch (Exception ce) {
                    logger.warn("Commit channel failed. JobId: {}, channel: {}, target: {}, msg: {}",
                            syncJob.getId(), channel.getId(), channel.getTargetTable(), ce.getMessage());
                    try {
                        channel.abortTxn(ce.getMessage());
                    } catch (Exception ae) {
                        logger.warn("Abort channel failed. JobId: {}，channel: {}, target: {}, msg: {}",
                                syncJob.getId(), channel.getId(), channel.getTargetTable(), ae.getMessage());
                    }
                    success = false;
                }
            }
        }
        if (success) {
            ack();
        } else {
            rollback();
        }
    }

    public Status waitForTxn() {
        for (CanalSyncChannel channel : idToChannels.values()) {
            channel.submitEOF();
        }

        Status st = Status.CANCELLED;
        try {
            handle.join();
            st = handle.getStatus();
        } catch (InterruptedException e) {
            logger.warn("InterruptedException: ", e);
        }
        return st;
    }

    public void cleanForTxn() {
        for (CanalSyncChannel channel : idToChannels.values()) {
            if (channel.isTxnInit()) {
                channel.clearTxn();
            }
        }
    }

    @Override
    public void process() {
        while (running) {
            try {
                int totalSize = 0;
                long totalMemSize = 0L;
                long startTime = System.currentTimeMillis();
                beginForTxn();
                while (running) {
                    Events<CanalEntry.Entry, EntryPosition> dataEvents = null;
                    try {
                        dataEvents = dataBlockingQueue.poll(CanalConfigs.pollWaitingTimeoutMs, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        // do nothing
                    }
                    if (dataEvents == null) {
                        // If not, continue to wait for the next batch of data
                        if (totalSize >= MIN_COMMIT_EVENT_SIZE || totalMemSize >= MIN_COMMIT_MEM_SIZE) {
                            break;
                        }
                        try {
                            Thread.sleep(sleepTimeMs);
                        } catch (InterruptedException e) {
                            // do nothing
                        }
                    } else {
                        if (debug) {
                            // print summary of the batch
                            CanalUtils.printSummary(dataEvents);
                        }
                        List<CanalEntry.Entry> entries = dataEvents.getDatas();
                        int size = entries.size();
                        ackBatches.add(dataEvents.getId());
                        positionMeta.addBatch(dataEvents.getId(), dataEvents.getPositionRange());
                        executeOneBatch(dataEvents);
                        totalSize += size;
                        totalMemSize += dataEvents.getMemSize();
                        // size of bytes received so far is larger than max commit memory size.
                        if (totalMemSize >= MAX_COMMIT_MEM_SIZE) {
                            break;
                        }
                    }

                    if (System.currentTimeMillis() - startTime >= commitIntervalSecond * 1000) {
                        break;
                    }
                }

                // wait all channels done
                Status st = waitForTxn();
                if (!running) {
                    abortForTxn("stopping client");
                    continue;
                }
                if (st.ok()) {
                    commitForTxn();
                } else {
                    abortForTxn(st.getErrorMsg());
                    syncJob.cancel(SyncFailMsg.MsgType.RUN_FAIL, st.getErrorMsg());
                }
            } catch (Exception e) {
                logger.error("Executor is error!", e);
                abortForTxn(e.getMessage());
                syncJob.cancel(SyncFailMsg.MsgType.SUBMIT_FAIL, e.getMessage());
            } finally {
                cleanForTxn();
            }
        }
    }

    public void put(Message message, int size) {
        List<CanalEntry.Entry> entries;
        if (message.isRaw()) {
            entries = new ArrayList<>(message.getRawEntries().size());
            for (ByteString rawEntry : message.getRawEntries()) {
                try {
                    entries.add(CanalEntry.Entry.parseFrom(rawEntry));
                } catch (InvalidProtocolBufferException e) {
                    throw new CanalException(e);
                }
            }
        } else {
            entries = message.getEntries();
        }

        int startIndex = 0;
        // if last ack position is null, it is the first time to consume batch
        EntryPosition lastAckPosition = positionMeta.getAckPosition();
        if (lastAckPosition != null) {
            EntryPosition firstPosition = EntryPosition.createPosition(entries.get(0));
            // only get data after the last ack position
            if (EntryPosition.min(firstPosition, lastAckPosition).equals(firstPosition)) {
                for (int i = 0; i <= entries.size() - 1; i++) {
                    startIndex++;
                    if (EntryPosition.checkPosition(entries.get(i), lastAckPosition)) {
                        break;
                    }
                }
            }
        }

        if (startIndex <= size - 1) {
            Events<CanalEntry.Entry, EntryPosition> dataEvents = new Events<>(message.getId());
            PositionRange<EntryPosition> range = new PositionRange<>();
            dataEvents.setPositionRange(range);
            range.setStart(EntryPosition.createPosition(entries.get(startIndex)));
            range.setEnd(EntryPosition.createPosition(entries.get(size - 1)));
            dataEvents.setDatas(entries);
            long memsize = 0L;
            for (CanalEntry.Entry entry : entries) {
                memsize += entry.getHeader().getEventLength();
            }
            dataEvents.setMemSize(memsize);
            try {
                dataBlockingQueue.put(dataEvents);
            } catch (InterruptedException e) {
                logger.error("put message to executor error:", e);
                throw new CanalException(e);
            }
        }
    }

    private void executeOneBatch(Events<CanalEntry.Entry, EntryPosition> dataEvents) throws UserException {
        final long batchId = dataEvents.getId();
        Map<String, CanalSyncChannel> preferChannels = Maps.newHashMap();
        Map<String, CanalSyncChannel> secondaryChannels = Maps.newHashMap();
        EntryPosition startPosition = dataEvents.getPositionRange().getStart();
        EntryPosition endPosition = dataEvents.getPositionRange().getEnd();
        for (CanalSyncChannel channel : idToChannels.values()) {
            String key = CanalUtils.getFullName(channel.getSrcDataBase(), channel.getSrcTable());
            // if last commit position is null, it is the first time to execute batch
            EntryPosition commitPosition = positionMeta.getCommitPosition(channel.getId());
            if (commitPosition != null) {
                if (commitPosition.compareTo(startPosition) < 0) {
                    preferChannels.put(key, channel);
                } else if (commitPosition.compareTo(endPosition) < 0) {
                    secondaryChannels.put(key, channel);
                }
            } else {
                preferChannels.put(key, channel);
            }
        }

        // distribute data to channels
        for (CanalEntry.Entry entry : dataEvents.getDatas()) {
            CanalEntry.EntryType entryType = entry.getEntryType();
            try {
                if (entryType == CanalEntry.EntryType.ROWDATA) {
                    CanalEntry.RowChange rowChange;
                    try {
                        rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new CanalException("parse event has an error , data:" + entry.toString(), e);
                    }
                    final CanalEntry.Header header = entry.getHeader();
                    final CanalEntry.EventType eventType = rowChange.getEventType();
                    if (!CanalUtils.isDML(eventType) || rowChange.getIsDdl()) {
                        String sql = rowChange.getSql();
                        processDDL(header, eventType, sql);
                        return;
                    }
                    String schemaTableName = CanalUtils.getFullName(header.getSchemaName(), header.getTableName());
                    if (preferChannels.containsKey(schemaTableName)) {
                        CanalSyncChannel channel = preferChannels.get(schemaTableName);
                        channel.submit(batchId, eventType, rowChange);
                    } else if (secondaryChannels.containsKey(schemaTableName)) {
                        CanalSyncChannel channel = secondaryChannels.get(schemaTableName);
                        EntryPosition position = EntryPosition.createPosition(entry);
                        EntryPosition commitPosition = positionMeta.getCommitPosition(channel.getId());
                        if (position.compareTo(commitPosition) > 0) {
                            channel.submit(batchId, eventType, rowChange);
                        }
                    }
                    // print row
                    if (debug) {
                        CanalUtils.printRow(rowChange, header);
                    }
                }
            } catch (Exception e) {
                logger.error("execute event has an error, data: {}, msg: {}", entry.toString(), e);
                throw new UserException("execute batch failed, batchId: " + batchId + " ,msg: " + e.getMessage());
            }
        }
    }

    // currently not support ddl
    private void processDDL(CanalEntry.Header header, CanalEntry.EventType eventType, String sql) {
        String table = header.getSchemaName() + "." + header.getTableName();
        switch (eventType) {
            case CREATE:
                logger.warn("parse create table event, table: {}, sql: {}", table, sql);
                return;
            case ALTER:
                logger.warn("parse alter table event, table: {}, sql: {}", table, sql);
                return;
            case TRUNCATE:
                logger.warn("parse truncate table event, table: {}, sql: {}", table, sql);
                return;
            case ERASE:
            case QUERY:
                logger.warn("parse event : {}, sql: {} . ignored!", eventType.name(), sql);
                return;
            case RENAME:
                logger.warn("parse rename table event, table: {}, sql: {}", table, sql);
                return;
            case CINDEX:
                logger.warn("parse create index event, table: {}, sql: {}", table, sql);
                return;
            case DINDEX:
                logger.warn("parse delete index event, table: {}, sql: {}", table, sql);
                return;
            default:
                logger.warn("parse unknown event: {}, table: {}, sql: {}", eventType.name(), table, sql);
                break;
        }
    }

    private void ack() {
        if (!ackBatches.isEmpty()) {
            logger.info("client ack batches: {}", ackBatches);
            while (!ackBatches.isEmpty()) {
                // ack the oldest batch
                long minBatchId = Collections.min(ackBatches);
                connector.ack(minBatchId);
                ackBatches.remove(minBatchId);
                PositionRange<EntryPosition> positionRange = positionMeta.removeBatch(minBatchId);
                positionMeta.setAckPosition(positionRange.getEnd());
                positionMeta.setAckTime(System.currentTimeMillis());
            }
        }
    }

    private void rollback() {
        holdGetLock();
        try {
            // Wait for the receiver to put the last message into the queue before clearing queue
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                // ignore
            }

            if (!ackBatches.isEmpty()) {
                connector.rollback();
            }
        } finally {
            releaseGetLock();
        }
        dataBlockingQueue.clear();
        ackBatches.clear();
        positionMeta.clearAllBatch();
    }

    public String getPositionInfo() {
        EntryPosition ackPosition = positionMeta.getAckPosition();
        long ackTime = positionMeta.getAckTime();
        StringBuilder sb = new StringBuilder();
        if (ackPosition != null) {
            SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
            long executeTime = ackPosition.getExecuteTime();
            long delayTime = ackTime - executeTime;
            Date date = new Date(executeTime);
            sb.append("position:").append(ackPosition)
                    .append(", executeTime:[").append(format.format(date)).append("], ")
                    .append("delay:").append(delayTime).append("ms");
            if (StringUtils.isNotEmpty(ackPosition.getGtid())) {
                sb.append(", gtid(").append(ackPosition.getGtid())
                        .append(") ");
            }
        } else {
            sb.append("position:").append("N/A");
        }
        return sb.toString();
    }

    private void cleanUp() {
        dataBlockingQueue.clear();
        ackBatches.clear();
        positionMeta.cleanUp();
    }

    private void holdGetLock() {
        getLock.lock();
    }

    private void releaseGetLock() {
        getLock.unlock();
    }
}