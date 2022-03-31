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

package org.apache.doris.load.sync.debezium;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.load.sync.SyncChannelHandle;
import org.apache.doris.load.sync.SyncDataConsumer;
import org.apache.doris.load.sync.SyncFailMsg;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DebeziumSyncDataConsumer extends SyncDataConsumer
        implements DebeziumEngine.ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>> {

    private static final Logger logger = LogManager.getLogger(DebeziumSyncDataConsumer.class);

    private static final long MIN_COMMIT_EVENT_SIZE = Config.min_sync_commit_size;
    private static final long MIN_COMMIT_MEM_SIZE = Config.min_bytes_sync_commit;
    private static final long MAX_COMMIT_MEM_SIZE = Config.max_bytes_sync_commit;
    private static final long MAX_COMMIT_INTERVAL = Config.sync_commit_interval_second;

    private final DebeziumSyncJob syncJob;
    private Map<Long, DebeziumSyncChannel> idToChannels;
    private Map<String, DebeziumSyncChannel> tableToChannels;
    private final SyncChannelHandle handle;
    private final DebeziumState debeziumState;
    private final DebeziumStateSerializer stateSerializer;
    private final int sleepTimeMs;
    private final LinkedBlockingQueue<SourceRecord> dataBlockingQueue;

    public void setChannels(Map<Long, DebeziumSyncChannel> idToChannels) {
        for (DebeziumSyncChannel channel : idToChannels.values()) {
            channel.setCallback(handle);
        }
        this.idToChannels = idToChannels;
        this.tableToChannels = Maps.newHashMap();
        for (DebeziumSyncChannel channel : idToChannels.values()) {
            String key = DebeziumUtils.getFullName(channel.getSrcDataBase(), channel.getSrcTable());
            tableToChannels.put(key, channel);
        }
    }

    public DebeziumSyncDataConsumer(DebeziumSyncJob syncJob, boolean debug) {
        super(debug);
        this.syncJob = syncJob;
        this.handle = new SyncChannelHandle();
        this.debeziumState = new DebeziumState();
        this.stateSerializer = new DebeziumStateSerializer();
        this.dataBlockingQueue = Queues.newLinkedBlockingQueue(1024);
        this.sleepTimeMs = 500;
    }

    @Override
    public void handleBatch(
            List<ChangeEvent<SourceRecord, SourceRecord>> changeEvents,
            DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer) {
        try {
            for (ChangeEvent<SourceRecord, SourceRecord> event : changeEvents) {
                SourceRecord record = event.value();
                String schemaTableName = DebeziumUtils.getFullName(record);
                if (!tableToChannels.containsKey(schemaTableName)) continue;
                if (debug) {
                    logger.info("debezium consumer handle record: " + record);
                }
                dataBlockingQueue.put(record);
                debeziumState.setSourcePartition(record.sourcePartition());
                debeziumState.setSourceOffset(record.sourceOffset());
            }
        } catch (InterruptedException e) {
            logger.warn("put change event to consumer queue error: " + e);
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
                    SourceRecord dataEvent = null;
                    try {
                        dataEvent = dataBlockingQueue.poll(80L, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        // do nothing
                    }
                    if (dataEvent == null) {
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
                        executeOneEvent(dataEvent);
                        totalSize += 1;
                        totalMemSize += dataEvent.value().toString().length();
                        // size of bytes received so far is larger than max commit memory size.
                        if (totalMemSize >= MAX_COMMIT_MEM_SIZE) {
                            break;
                        }
                    }

                    if (System.currentTimeMillis() - startTime >= MAX_COMMIT_INTERVAL * 1000) {
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

    private void executeOneEvent(SourceRecord dataEvent) throws UserException {
        try {
            String schemaTableName = DebeziumUtils.getFullName(dataEvent);
            DebeziumSyncChannel channel = tableToChannels.get(schemaTableName);
            channel.submit(dataEvent);
            // print row
            if (debug) {
                logger.info("debezium consumer execute record: " + dataEvent);
            }
        } catch (Exception e) {
            logger.error("execute event has an error, data: {}, msg: {}", dataEvent.toString(), e);
            throw new UserException("execute batch failed, batchId: ,msg: " + e.getMessage());
        }
    }

    public void beginForTxn() {
        handle.reset(idToChannels.size());
        for (DebeziumSyncChannel channel : idToChannels.values()) {
            channel.initTxn(Config.max_stream_load_timeout_second);
            handle.mark(channel);
        }
    }

    public void abortForTxn(String reason) {
        abortForTxn(idToChannels.values(), reason);
    }

    public void abortForTxn(Collection<DebeziumSyncChannel> channels, String reason) {
        logger.info("client is aborting transactions. JobId: {}, reason: {}", syncJob.getId(), reason);
        for (DebeziumSyncChannel channel : channels) {
            if (channel.isTxnBegin()) {
                try {
                    channel.abortTxn(reason);
                } catch (Exception e) {
                    logger.warn("Abort channel failed. jobId: {}，channel: {}, target: {}, msg: {}",
                            syncJob.getId(), channel.getId(), channel.getTargetTable(), e.getMessage());
                }
            }
        }
    }

    public void commitForTxn() {
        logger.info("client is committing transactions. JobId: {}", syncJob.getId());
        for (DebeziumSyncChannel channel : idToChannels.values()) {
            if (channel.isTxnBegin()) {
                try {
                    channel.commitTxn();
                    byte[] state = stateSerializer.serialize(debeziumState);
                    String position = new String(state, StandardCharsets.UTF_8);
                    syncJob.setStatus(position);
                } catch (Exception ce) {
                    logger.warn("Commit channel failed. JobId: {}, channel: {}, target: {}, msg: {}",
                            syncJob.getId(), channel.getId(), channel.getTargetTable(), ce.getMessage());
                    try {
                        channel.abortTxn(ce.getMessage());
                    } catch (Exception ae) {
                        logger.warn("Abort channel failed. JobId: {}，channel: {}, target: {}, msg: {}",
                                syncJob.getId(), channel.getId(), channel.getTargetTable(), ae.getMessage());
                    }
                }
            }
        }
    }

    public Status waitForTxn() {
        for (DebeziumSyncChannel channel : idToChannels.values()) {
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
        for (DebeziumSyncChannel channel : idToChannels.values()) {
            if (channel.isTxnInit()) {
                channel.clearTxn();
            }
        }
    }

}
