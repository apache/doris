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

import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.UserException;
import org.apache.doris.load.sync.SyncChannel;
import org.apache.doris.load.sync.SyncChannelCallback;
import org.apache.doris.load.sync.SyncJob;
import org.apache.doris.load.sync.model.Data;
import org.apache.doris.proto.InternalService;
import org.apache.doris.qe.InsertStreamTxnExecutor;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TMergeType;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TTxnParams;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TransactionEntry;
import org.apache.doris.transaction.TransactionState;

import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.protocol.CanalEntry;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CanalSyncChannel extends SyncChannel {
    private static final Logger LOG = LogManager.getLogger(CanalSyncChannel.class);

    private static final String DELETE_COLUMN = "_delete_sign_";
    private static final String DELETE_CONDITION = DELETE_COLUMN + "=1";
    private static final String NULL_VALUE_FOR_LOAD = "\\N";

    private long timeoutSecond;
    private long lastBatchId;
    private LinkedBlockingQueue<Data<InternalService.PDataRow>> pendingQueue;
    private Data<InternalService.PDataRow> batchBuffer;
    private InsertStreamTxnExecutor txnExecutor;

    public CanalSyncChannel(SyncJob syncJob, Database db, OlapTable table, List<String> columns, String srcDataBase, String srcTable) {
        super(syncJob, db, table, columns, srcDataBase, srcTable);
        this.batchBuffer = new Data<>();
        this.pendingQueue = Queues.newLinkedBlockingQueue(128);
        this.lastBatchId = -1L;
        this.timeoutSecond = -1L;
    }

    public void process() {
        while (running) {
            if (!isTxnInit()) {
                continue;
            }
            // if txn has begun, send all data in queue
            if (isTxnBegin()) {
                while (!pendingQueue.isEmpty()) {
                    try {
                        Data<InternalService.PDataRow> rows = pendingQueue.poll(CanalConfigs.channelWaitingTimeoutMs, TimeUnit.MILLISECONDS);
                        if (rows != null) {
                            sendData(rows);
                        }
                    } catch (Exception e) {
                        String errMsg = "encounter exception in channel, channel " + id + ", " +
                                "msg: " + e.getMessage() + ", table: " + targetTable;
                        LOG.error(errMsg);
                        callback.onFailed(errMsg);
                    }
                }
            }
            if (callback.state()) {
                callback.onFinished(id);
            }
        }
    }

    @Override
    public void beginTxn(long batchId) throws UserException, TException, TimeoutException,
            InterruptedException, ExecutionException {
        if (!isTxnBegin()) {
            long currentTime = System.currentTimeMillis();
            String label = "label_job" + + jobId + "_channel" + id + "_db" + db.getId() + "_tbl" + tbl.getId()
                    + "_batch" + batchId + "_" + currentTime;
            String targetColumn = Joiner.on(",").join(columns) + "," + DELETE_COLUMN;
            GlobalTransactionMgr globalTransactionMgr = Catalog.getCurrentGlobalTransactionMgr();
            TransactionEntry txnEntry = txnExecutor.getTxnEntry();
            TTxnParams txnConf = txnEntry.getTxnConf();
            TransactionState.LoadJobSourceType sourceType = TransactionState.LoadJobSourceType.INSERT_STREAMING;
            TStreamLoadPutRequest request = null;
            try {
                long txnId = globalTransactionMgr.beginTransaction(db.getId(), Lists.newArrayList(tbl.getId()), label,
                        new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, FrontendOptions.getLocalHostAddress()), sourceType, timeoutSecond);
                String authCodeUuid = Catalog.getCurrentGlobalTransactionMgr().getTransactionState(
                        db.getId(), txnId).getAuthCode();
                request = new TStreamLoadPutRequest()
                        .setTxnId(txnId).setDb(txnConf.getDb()).setTbl(txnConf.getTbl())
                        .setFileType(TFileType.FILE_STREAM).setFormatType(TFileFormatType.FORMAT_CSV_PLAIN)
                        .setThriftRpcTimeoutMs(5000).setLoadId(txnExecutor.getLoadId())
                        .setMergeType(TMergeType.MERGE).setDeleteCondition(DELETE_CONDITION)
                        .setColumns(targetColumn);
                txnConf.setTxnId(txnId).setAuthCodeUuid(authCodeUuid);
                txnEntry.setLabel(label);
                txnExecutor.setTxnId(txnId);
            } catch (DuplicatedRequestException e) {
                LOG.warn("duplicate request for sync channel. channel: {}, request id: {}, txn: {}, table: {}",
                        id, e.getDuplicatedRequestId(), e.getTxnId(), targetTable);
                txnExecutor.setTxnId(e.getTxnId());
            } catch (LabelAlreadyUsedException e) {
                // this happens when channel re-consume same batch, we should just pass through it without begin a new txn
                LOG.warn("Label already used in channel {}, label: {}, table: {}, batch: {}", id, label, targetTable, batchId);
                return;
            } catch (AnalysisException | BeginTransactionException e) {
                LOG.warn("encounter an error when beginning txn in channel {}, table: {}", id, targetTable);
                throw e;
            } catch (UserException e) {
                LOG.warn("encounter an error when creating plan in channel {}, table: {}", id, targetTable);
                throw e;
            }
            try {
                // async exec begin transaction
                long txnId = txnExecutor.getTxnId();
                if (txnId != -1L) {
                    this.txnExecutor.beginTransaction(request);
                    LOG.info("begin txn in channel {}, table: {}, label:{}, txn id: {}", id, targetTable, label, txnExecutor.getTxnId());
                }
            } catch (TException e) {
                LOG.warn("Failed to begin txn in channel {}, table: {}, txn: {}, msg:{}", id, targetTable, txnExecutor.getTxnId(), e.getMessage());
                throw e;
            } catch (TimeoutException | InterruptedException | ExecutionException e) {
                LOG.warn("Error occur while waiting begin txn response in channel {}, table: {}, txn: {}, msg:{}",
                        id, targetTable, txnExecutor.getTxnId(), e.getMessage());
                throw e;
            }
        }
    }

    @Override
    public void abortTxn(String reason) throws TException, TimeoutException, InterruptedException, ExecutionException {
        if (!isTxnBegin()) {
            LOG.warn("No transaction to abort in channel {}, table: {}", id, targetTable);
            return;
        }
        try {
            this.txnExecutor.abortTransaction();
            LOG.info("abort txn in channel {}, table: {}, txn id: {}, last batch: {}, reason: {}",
                    id, targetTable, txnExecutor.getTxnId(), lastBatchId, reason);
        } catch (TException e) {
            LOG.warn("Failed to abort txn in channel {}, table: {}, txn: {}, msg:{}", id, targetTable, txnExecutor.getTxnId(), e.getMessage());
            throw e;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.warn("Error occur while waiting abort txn response in channel {}, table: {}, txn: {}, msg:{}",
                    id, targetTable, txnExecutor.getTxnId(), e.getMessage());
            throw e;
        }  finally {
            this.batchBuffer = new Data<>();
            this.pendingQueue.clear();
            updateBatchId(-1L);
        }
    }
    @Override
    public void commitTxn() throws TException, TimeoutException, InterruptedException, ExecutionException {
        if (!isTxnBegin()) {
            LOG.warn("No transaction to commit in channel {}, table: {}", id, targetTable);
            return;
        }
        try {
            flushData();
            this.txnExecutor.commitTransaction();
            LOG.info("commit txn in channel {}, table: {}, txn id: {}, last batch: {}",
                    id, targetTable, txnExecutor.getTxnId(), lastBatchId);
        } catch (TException e) {
            LOG.warn("Failed to commit txn in channel {}, table: {}, txn: {}, msg:{}", id, targetTable, txnExecutor.getTxnId(), e.getMessage());
            throw e;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.warn("Error occur while waiting commit txn return in channel {}, table: {}, txn: {}, msg:{}",
                    id, targetTable, txnExecutor.getTxnId(), e.getMessage());
            throw e;
        } finally {
            this.batchBuffer = new Data<>();
            this.pendingQueue.clear();
            updateBatchId(-1L);
        }
    }
    @Override
    public void initTxn(long timeoutSecond) {
        if (!isTxnInit()) {
            UUID uuid = UUID.randomUUID();
            TUniqueId loadId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
            this.timeoutSecond = timeoutSecond;
            TTxnParams txnConf = new TTxnParams().setNeedTxn(true).setThriftRpcTimeoutMs(5000)
                    .setTxnId(-1).setDb(db.getFullName()).setTbl(tbl.getName()).setDbId(db.getId());
            this.txnExecutor = new InsertStreamTxnExecutor(new TransactionEntry(txnConf, db, tbl));
            txnExecutor.setTxnId(-1L);
            txnExecutor.setLoadId(loadId);
        }
    }

    public void clearTxn() {
        this.txnExecutor = null;
    }

    public void submit(long batchId, CanalEntry.EventType eventType, CanalEntry.RowChange rowChange) {
        String sql = rowChange.getSql();
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            switch (eventType) {
                case DELETE:
                    execute(batchId, eventType, rowData.getBeforeColumnsList());
                    break;
                case INSERT:
                    execute(batchId, eventType, rowData.getAfterColumnsList());
                    break;
                case UPDATE:
                    execute(batchId, eventType, rowData.getAfterColumnsList());
                    break;
                default:
                    LOG.warn("ignore event, channel: {}, schema: {}, table: {}, SQL: {}", id, srcDataBase, srcTable, sql);
            }
        }
    }

    private void execute(long batchId, CanalEntry.EventType eventType, List<CanalEntry.Column> columns) {
        InternalService.PDataRow row = parseRow(eventType, columns);
        try {
            Preconditions.checkState(isTxnInit());
            if (batchId > lastBatchId) {
                if (!isTxnBegin()) {
                    beginTxn(batchId);
                } else {
                    this.pendingQueue.put(this.batchBuffer);
                    this.batchBuffer = new Data<>();
                }
                updateBatchId(batchId);
            }
        } catch (Exception e) {
            String errMsg = "encounter exception when submit in channel " + id + ", table: "
                    + targetTable + ", batch: " + batchId;
            LOG.error(errMsg, e);
            throw new CanalException(errMsg, e);
        }
        this.batchBuffer.addRow(row);
    }

    private InternalService.PDataRow parseRow(CanalEntry.EventType eventType, List<CanalEntry.Column> columns) {
        InternalService.PDataRow.Builder row = InternalService.PDataRow.newBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getIsNull()) {
                row.addColBuilder().setValue(NULL_VALUE_FOR_LOAD);
            } else {
                row.addColBuilder().setValue(columns.get(i).getValue());
            }
        }
        // add batch delete condition to the tail
        if (eventType == CanalEntry.EventType.DELETE) {
            row.addColBuilder().setValue("1");
        } else {
            row.addColBuilder().setValue("0");
        }
        return row.build();
    }

    private void sendData(Data<InternalService.PDataRow> rows) throws TException, TimeoutException,
            InterruptedException, ExecutionException {
        Preconditions.checkState(isTxnBegin());
        TransactionEntry txnEntry = txnExecutor.getTxnEntry();
        txnEntry.setDataToSend(rows.getDatas());
        this.txnExecutor.sendData();
    }

    public void flushData() throws TException, TimeoutException,
            InterruptedException, ExecutionException {
        if (batchBuffer.isNotEmpty()) {
            sendData(batchBuffer);
            batchBuffer = new Data<>();
        }
    }

    public boolean isTxnBegin() {
        return isTxnInit() && this.txnExecutor.getTxnId() != -1;
    }

    public boolean isTxnInit() {
        return this.txnExecutor != null;
    }

    private void updateBatchId(long batchId) {
        this.lastBatchId = batchId;
    }

    public String getInfo() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(srcDataBase).append(".").append(srcTable);
        stringBuilder.append("->");
        stringBuilder.append(targetTable);
        return stringBuilder.toString();
    }

    public long getId() {
        return id;
    }

    public String getSrcTable() {
        return srcTable;
    }

    public String getSrcDataBase() {
        return srcDataBase;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setCallback(SyncChannelCallback callback) {
        this.callback = callback;
    }

    public void setPartitions(PartitionNames partitionNames) {
        this.partitionNames = partitionNames;
    }
}