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

package org.apache.doris.cloud.transaction;

import org.apache.doris.catalog.CloudTabletStatMgr;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.CommitTxnResponse;
import org.apache.doris.cloud.proto.Cloud.RLTaskTxnCommitAttachmentPB;
import org.apache.doris.cloud.proto.Cloud.RoutineLoadProgressPB;
import org.apache.doris.cloud.proto.Cloud.StreamingTaskCommitAttachmentPB;
import org.apache.doris.cloud.proto.Cloud.TableStatsPB;
import org.apache.doris.cloud.proto.Cloud.TxnCommitAttachmentPB;
import org.apache.doris.cloud.proto.Cloud.TxnCommitAttachmentPB.LoadJobFinalOperationPB;
import org.apache.doris.cloud.proto.Cloud.TxnCommitAttachmentPB.LoadJobFinalOperationPB.EtlStatusPB;
import org.apache.doris.cloud.proto.Cloud.TxnCommitAttachmentPB.LoadJobFinalOperationPB.FailMsgPB;
import org.apache.doris.cloud.proto.Cloud.TxnCommitAttachmentPB.LoadJobFinalOperationPB.JobStatePB;
import org.apache.doris.cloud.proto.Cloud.TxnCoordinatorPB;
import org.apache.doris.cloud.proto.Cloud.TxnInfoPB;
import org.apache.doris.cloud.proto.Cloud.TxnSourceTypePB;
import org.apache.doris.cloud.proto.Cloud.UniqueIdPB;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.event.DataChangeEvent;
import org.apache.doris.job.extensions.insert.streaming.StreamingTaskTxnCommitAttachment;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.load.loadv2.LoadJobFinalOperation;
import org.apache.doris.load.routineload.KafkaProgress;
import org.apache.doris.load.routineload.RLTaskTxnCommitAttachment;
import org.apache.doris.thrift.TEtlState;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;
import org.apache.doris.transaction.TransactionStatus;
import org.apache.doris.transaction.TxnCommitAttachment;
import org.apache.doris.transaction.TxnStateCallbackFactory;
import org.apache.doris.transaction.TxnStateChangeCallback;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class TxnUtil {
    private static final Logger LOG = LogManager.getLogger(TxnUtil.class);

    /**
     * Backoff policy for retry: sleep random milliseconds in [20ms, 200ms].
     * Used to avoid txn conflict when retrying RPC calls.
     */
    public static void backoff() {
        int randomMillis = 20 + (int) (Math.random() * (200 - 20));
        try {
            Thread.sleep(randomMillis);
        } catch (InterruptedException e) {
            LOG.info("InterruptedException: ", e);
        }
    }

    public static EtlStatusPB.EtlStatePB etlStateToPb(TEtlState tEtlState) {
        switch (tEtlState) {
            case RUNNING:
                return EtlStatusPB.EtlStatePB.RUNNING;
            case FINISHED:
                return EtlStatusPB.EtlStatePB.FINISHED;
            case CANCELLED:
                return EtlStatusPB.EtlStatePB.CANCELLED;
            default:
                return EtlStatusPB.EtlStatePB.UNKNOWN;
        }
    }

    public static TEtlState etlStateFromPb(EtlStatusPB.EtlStatePB etlStatePB) {
        switch (etlStatePB) {
            case RUNNING:
                return TEtlState.RUNNING;
            case FINISHED:
                return TEtlState.FINISHED;
            case CANCELLED:
                return TEtlState.CANCELLED;
            default:
                return TEtlState.UNKNOWN;
        }
    }

    public static JobStatePB jobStateToPb(JobState jobState) {
        switch (jobState) {
            case PENDING:
                return JobStatePB.PENDING;
            case ETL:
                return JobStatePB.ETL;
            case LOADING:
                return JobStatePB.LOADING;
            case COMMITTED:
                return JobStatePB.COMMITTED;
            case FINISHED:
                return JobStatePB.FINISHED;
            case CANCELLED:
                return JobStatePB.CANCELLED;
            default:
                return JobStatePB.UNKNOWN;
        }
    }

    public static JobState jobStateFromPb(JobStatePB jobStatePb) {
        switch (jobStatePb) {
            case PENDING:
                return JobState.PENDING;
            case ETL:
                return JobState.ETL;
            case LOADING:
                return JobState.LOADING;
            case COMMITTED:
                return JobState.COMMITTED;
            case FINISHED:
                return JobState.FINISHED;
            case CANCELLED:
                return JobState.CANCELLED;
            default:
                return JobState.UNKNOWN;
        }
    }

    public static FailMsgPB failMsgToPb(FailMsg failMsg) {
        FailMsgPB.Builder builder = FailMsgPB.newBuilder();
        builder.setMsg(failMsg.getMsg());

        switch (failMsg.getCancelType()) {
            case USER_CANCEL:
                builder.setCancelType(FailMsgPB.CancelTypePB.USER_CANCEL);
                break;
            case ETL_SUBMIT_FAIL:
                builder.setCancelType(FailMsgPB.CancelTypePB.ETL_SUBMIT_FAIL);
                break;
            case ETL_RUN_FAIL:
                builder.setCancelType(FailMsgPB.CancelTypePB.ETL_RUN_FAIL);
                break;
            case ETL_QUALITY_UNSATISFIED:
                builder.setCancelType(FailMsgPB.CancelTypePB.ETL_QUALITY_UNSATISFIED);
                break;
            case LOAD_RUN_FAIL:
                builder.setCancelType(FailMsgPB.CancelTypePB.LOAD_RUN_FAIL);
                break;
            case TIMEOUT:
                builder.setCancelType(FailMsgPB.CancelTypePB.TIMEOUT);
                break;
            case TXN_UNKNOWN:
                builder.setCancelType(FailMsgPB.CancelTypePB.TXN_UNKNOWN);
                break;
            default:
                builder.setCancelType(FailMsgPB.CancelTypePB.UNKNOWN);
                break;
        }
        return builder.build();
    }

    public static FailMsg failMsgFromPb(FailMsgPB failMsgPb) {
        FailMsg failMsg = new FailMsg();
        failMsg.setMsg(failMsgPb.getMsg());
        switch (failMsgPb.getCancelType()) {
            case USER_CANCEL:
                failMsg.setCancelType(FailMsg.CancelType.USER_CANCEL);
                break;
            case ETL_SUBMIT_FAIL:
                failMsg.setCancelType(FailMsg.CancelType.ETL_SUBMIT_FAIL);
                break;
            case ETL_RUN_FAIL:
                failMsg.setCancelType(FailMsg.CancelType.ETL_RUN_FAIL);
                break;
            case ETL_QUALITY_UNSATISFIED:
                failMsg.setCancelType(FailMsg.CancelType.ETL_QUALITY_UNSATISFIED);
                break;
            case LOAD_RUN_FAIL:
                failMsg.setCancelType(FailMsg.CancelType.LOAD_RUN_FAIL);
                break;
            case TIMEOUT:
                failMsg.setCancelType(FailMsg.CancelType.TIMEOUT);
                break;
            case TXN_UNKNOWN:
                failMsg.setCancelType(FailMsg.CancelType.TXN_UNKNOWN);
                break;
            default:
                failMsg.setCancelType(FailMsg.CancelType.UNKNOWN);
                break;
        }
        return failMsg;
    }

    public static EtlStatusPB etlStatusToPb(EtlStatus etlStatus) {
        EtlStatusPB.Builder builder = EtlStatusPB.newBuilder();
        builder.setState(TxnUtil.etlStateToPb(etlStatus.getState()))
                .setTrackingUrl(etlStatus.getTrackingUrl())
                .putAllStats(etlStatus.getStats())
                .putAllCounters(etlStatus.getCounters());
        return builder.build();
    }

    public static EtlStatus etlStatusFromPb(EtlStatusPB etlStatusPB) {
        EtlStatus etlStatus = new EtlStatus();

        etlStatus.setState(TxnUtil.etlStateFromPb(etlStatusPB.getState()));
        etlStatus.setTrackingUrl(etlStatusPB.getTrackingUrl());
        etlStatus.setStats(etlStatusPB.getStats());
        etlStatus.setCounters(etlStatusPB.getCounters());
        return etlStatus;
    }

    public static TxnCommitAttachmentPB loadJobFinalOperationToPb(LoadJobFinalOperation loadJobFinalOperation) {
        LOG.info("loadJobFinalOperation:{}", loadJobFinalOperation);
        TxnCommitAttachmentPB.Builder attachementBuilder = TxnCommitAttachmentPB.newBuilder();
        attachementBuilder.setType(TxnCommitAttachmentPB.Type.LODD_JOB_FINAL_OPERATION);

        TxnCommitAttachmentPB.LoadJobFinalOperationPB.Builder builder =
                TxnCommitAttachmentPB.LoadJobFinalOperationPB.newBuilder();

        builder.setId(loadJobFinalOperation.getId())
                .setLoadingStatus(TxnUtil.etlStatusToPb(loadJobFinalOperation.getLoadingStatus()))
                .setProgress(loadJobFinalOperation.getProgress())
                .setLoadStartTimestamp(loadJobFinalOperation.getLoadStartTimestamp())
                .setFinishTimestamp(loadJobFinalOperation.getFinishTimestamp())
                .setJobState(TxnUtil.jobStateToPb(loadJobFinalOperation.getJobState()));

        if (loadJobFinalOperation.getFailMsg() != null) {
            builder.setFailMsg(TxnUtil.failMsgToPb(loadJobFinalOperation.getFailMsg()));
        }
        // copy into
        builder.setCopyId(loadJobFinalOperation.getCopyId()).setLoadFilePaths(loadJobFinalOperation.getLoadFilePaths());

        attachementBuilder.setLoadJobFinalOperation(builder.build());
        return attachementBuilder.build();
    }

    public static TxnCommitAttachmentPB rlTaskTxnCommitAttachmentToPb(RLTaskTxnCommitAttachment
            rtTaskTxnCommitAttachment) {
        LOG.info("rtTaskTxnCommitAttachment:{}", rtTaskTxnCommitAttachment);
        TxnCommitAttachmentPB.Builder attachementBuilder = TxnCommitAttachmentPB.newBuilder();
        attachementBuilder.setType(TxnCommitAttachmentPB.Type.RT_TASK_TXN_COMMIT_ATTACHMENT);

        RLTaskTxnCommitAttachmentPB.Builder builder =
                RLTaskTxnCommitAttachmentPB.newBuilder();

        UniqueIdPB.Builder taskIdBuilder = UniqueIdPB.newBuilder();
        taskIdBuilder.setHi(rtTaskTxnCommitAttachment.getTaskId().getHi());
        taskIdBuilder.setLo(rtTaskTxnCommitAttachment.getTaskId().getLo());

        RoutineLoadProgressPB.Builder progressBuilder = RoutineLoadProgressPB.newBuilder();
        progressBuilder.putAllPartitionToOffset(((KafkaProgress) rtTaskTxnCommitAttachment.getProgress())
                .getOffsetByPartition());

        builder.setJobId(rtTaskTxnCommitAttachment.getJobId())
                .setTaskId(taskIdBuilder)
                .setFilteredRows(rtTaskTxnCommitAttachment.getFilteredRows())
                .setLoadedRows(rtTaskTxnCommitAttachment.getLoadedRows())
                .setProgress(progressBuilder)
                .setUnselectedRows(rtTaskTxnCommitAttachment.getUnselectedRows())
                .setReceivedBytes(rtTaskTxnCommitAttachment.getReceivedBytes())
                .setTaskExecutionTimeMs(rtTaskTxnCommitAttachment.getTaskExecutionTimeMs());

        if (rtTaskTxnCommitAttachment.getErrorLogUrl() != null) {
            builder.setErrorLogUrl(rtTaskTxnCommitAttachment.getErrorLogUrl());
        }

        attachementBuilder.setRlTaskTxnCommitAttachment(builder.build());
        return attachementBuilder.build();
    }

    public static RLTaskTxnCommitAttachment rtTaskTxnCommitAttachmentFromPb(
            TxnCommitAttachmentPB txnCommitAttachmentPB) {
        RLTaskTxnCommitAttachmentPB rlTaskTxnCommitAttachmentPB = txnCommitAttachmentPB.getRlTaskTxnCommitAttachment();
        if (LOG.isDebugEnabled()) {
            LOG.debug("RLTaskTxnCommitAttachmentPB={}", rlTaskTxnCommitAttachmentPB);
        }
        return new RLTaskTxnCommitAttachment(txnCommitAttachmentPB.getRlTaskTxnCommitAttachment());
    }

    public static TxnCommitAttachmentPB streamingTaskTxnCommitAttachmentToPb(StreamingTaskTxnCommitAttachment
            streamingTaskTxnCommitAttachment) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("streamingTaskTxnCommitAttachment:{}", streamingTaskTxnCommitAttachment);
        }
        TxnCommitAttachmentPB.Builder attachementBuilder = TxnCommitAttachmentPB.newBuilder();
        attachementBuilder.setType(TxnCommitAttachmentPB.Type.STREAMING_TASK_TXN_COMMIT_ATTACHMENT);

        StreamingTaskCommitAttachmentPB.Builder builder =
                StreamingTaskCommitAttachmentPB.newBuilder();

        builder.setJobId(streamingTaskTxnCommitAttachment.getJobId())
                .setScannedRows(streamingTaskTxnCommitAttachment.getScannedRows())
                .setLoadBytes(streamingTaskTxnCommitAttachment.getLoadBytes())
                .setNumFiles(streamingTaskTxnCommitAttachment.getNumFiles())
                .setFileBytes(streamingTaskTxnCommitAttachment.getFileBytes());

        if (streamingTaskTxnCommitAttachment.getOffset() != null) {
            builder.setOffset(streamingTaskTxnCommitAttachment.getOffset());
        }

        attachementBuilder.setStreamingTaskTxnCommitAttachment(builder.build());
        return attachementBuilder.build();
    }

    public static StreamingTaskTxnCommitAttachment streamingTaskTxnCommitAttachmentFromPb(
            TxnCommitAttachmentPB txnCommitAttachmentPB) {
        StreamingTaskCommitAttachmentPB streamingTaskCommitAttachmentPB =
                txnCommitAttachmentPB.getStreamingTaskTxnCommitAttachment();
        if (LOG.isDebugEnabled()) {
            LOG.debug("StreamingTaskCommitAttachmentPB={}", streamingTaskCommitAttachmentPB);
        }
        return new StreamingTaskTxnCommitAttachment(streamingTaskCommitAttachmentPB);
    }

    public static LoadJobFinalOperation loadJobFinalOperationFromPb(TxnCommitAttachmentPB txnCommitAttachmentPB) {
        LoadJobFinalOperationPB loadJobFinalOperationPB = txnCommitAttachmentPB.getLoadJobFinalOperation();
        if (LOG.isDebugEnabled()) {
            LOG.debug("loadJobFinalOperationPB={}", loadJobFinalOperationPB);
        }
        FailMsg failMsg = loadJobFinalOperationPB.hasFailMsg()
                ? TxnUtil.failMsgFromPb(loadJobFinalOperationPB.getFailMsg()) : null;
        return new LoadJobFinalOperation(loadJobFinalOperationPB.getId(),
                TxnUtil.etlStatusFromPb(loadJobFinalOperationPB.getLoadingStatus()),
                loadJobFinalOperationPB.getProgress(), loadJobFinalOperationPB.getLoadStartTimestamp(),
                loadJobFinalOperationPB.getFinishTimestamp(),
                TxnUtil.jobStateFromPb(loadJobFinalOperationPB.getJobState()), failMsg,
                loadJobFinalOperationPB.getCopyId(), loadJobFinalOperationPB.getLoadFilePaths(), new HashMap<>());
    }

    public static TxnCoordinatorPB txnCoordinatorToPb(TxnCoordinator txnCoordinator) {
        TxnCoordinatorPB.Builder builder = TxnCoordinatorPB.newBuilder();
        builder.setSourceType(TxnSourceTypePB.forNumber(txnCoordinator.sourceType.value()));
        builder.setId(txnCoordinator.id);
        builder.setIp(txnCoordinator.ip);
        builder.setStartTime(txnCoordinator.startTime);
        return builder.build();
    }

    public static TxnCoordinator txnCoordinatorFromPb(TxnCoordinatorPB txnCoordinatorPB) {
        return new TxnCoordinator(TxnSourceType.valueOf(txnCoordinatorPB.getSourceType().getNumber()),
                txnCoordinatorPB.getId(), txnCoordinatorPB.getIp(), txnCoordinatorPB.getStartTime());
    }

    public static TransactionState transactionStateFromPb(TxnInfoPB txnInfo) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("txnInfo={}", txnInfo);
        }
        long dbId = txnInfo.getDbId();
        List<Long> tableIdList = txnInfo.getTableIdsList();
        long transactionId = txnInfo.getTxnId();
        String label = txnInfo.getLabel();

        TUniqueId requestId = null;
        if (txnInfo.hasRequestId()) {
            requestId = new TUniqueId(txnInfo.getRequestId().getHi(), txnInfo.getRequestId().getLo());
        }

        LoadJobSourceType loadJobSourceType = null;
        if (txnInfo.hasLoadJobSourceType()) {
            loadJobSourceType = LoadJobSourceType.valueOf(txnInfo.getLoadJobSourceType().getNumber());
        }
        TxnCoordinator txnCoordinator = null;
        if (txnInfo.hasCoordinator()) {
            txnCoordinator = TxnUtil.txnCoordinatorFromPb(txnInfo.getCoordinator());
        }

        TransactionStatus transactionStatus = TransactionStatus.valueOf(txnInfo.getStatus().getNumber());
        String reason = txnInfo.getReason();
        long callbackId = txnInfo.getListenerId();
        long timeoutMs = txnInfo.getTimeoutMs();

        TxnCommitAttachment commitAttachment = null;
        if (txnInfo.hasCommitAttachment()) {
            if (txnInfo.getCommitAttachment().getType() == TxnCommitAttachmentPB.Type.LODD_JOB_FINAL_OPERATION) {
                commitAttachment =
                    TxnUtil.loadJobFinalOperationFromPb(txnInfo.getCommitAttachment());
            }

            if (txnInfo.getCommitAttachment().getType() == TxnCommitAttachmentPB.Type.RT_TASK_TXN_COMMIT_ATTACHMENT) {
                commitAttachment =
                    TxnUtil.rtTaskTxnCommitAttachmentFromPb(txnInfo.getCommitAttachment());
            }

            if (txnInfo.getCommitAttachment().getType()
                    == TxnCommitAttachmentPB.Type.STREAMING_TASK_TXN_COMMIT_ATTACHMENT) {
                commitAttachment =
                    TxnUtil.streamingTaskTxnCommitAttachmentFromPb(txnInfo.getCommitAttachment());
            }
        }

        long prepareTime = txnInfo.hasPrepareTime() ? txnInfo.getPrepareTime() : -1;
        long preCommitTime = txnInfo.hasPrecommitTime() ? txnInfo.getPrecommitTime() : -1;
        long commitTime = txnInfo.hasCommitTime() ? txnInfo.getCommitTime() : -1;
        long finishTime = txnInfo.hasFinishTime() ? txnInfo.getFinishTime() : -1;

        TransactionState transactionState = new TransactionState(
                dbId,
                tableIdList,
                transactionId,
                label,
                requestId,
                loadJobSourceType,
                txnCoordinator,
                transactionStatus,
                reason,
                callbackId,
                timeoutMs,
                commitAttachment,
                prepareTime,
                preCommitTime,
                commitTime,
                finishTime
        );
        if (LOG.isDebugEnabled()) {
            LOG.debug("transactionState={}", transactionState);
        }
        return transactionState;
    }

    /**
     * Execute callback handling after transaction commit/publish succeeds.
     * This method extracts common callback logic used by both one-phase and two-phase commit.
     *
     * @param txnId transaction id
     * @param txnCommitAttachment transaction commit attachment
     * @param txnState transaction state (may be null for two-phase publish phase)
     * @param callbackFactory callback factory to get callbacks
     * @param isTwoPhase whether this is two-phase commit (affects which callbacks to call)
     * @param txnOperated whether the transaction operation succeeded (affects callback execution)
     */
    public static void executeCommitCallbacks(long txnId,
                                              TxnCommitAttachment txnCommitAttachment,
                                              TransactionState txnState,
                                              TxnStateCallbackFactory callbackFactory,
                                              boolean isTwoPhase,
                                              boolean txnOperated) {
        long callbackId = 0L;

        // Determine callbackId from txnCommitAttachment or txnState
        if (txnCommitAttachment != null && txnCommitAttachment instanceof RLTaskTxnCommitAttachment) {
            callbackId = ((RLTaskTxnCommitAttachment) txnCommitAttachment).getJobId();
        } else if (txnCommitAttachment != null
                && txnCommitAttachment instanceof StreamingTaskTxnCommitAttachment) {
            callbackId = ((StreamingTaskTxnCommitAttachment) txnCommitAttachment).getJobId();
        } else if (txnState != null) {
            callbackId = txnState.getCallbackId();
        }

        if (callbackId == 0L) {
            return;
        }

        TxnStateChangeCallback cb = callbackFactory.getCallback(callbackId);
        if (cb != null) {
            try {
                LOG.info("execute commit callbacks, transactionId:{} callbackId:{}, isTwoPhase:{}, txnOperated:{}",
                        txnId, callbackId, isTwoPhase, txnOperated);
                if (isTwoPhase) {
                    // For two-phase commit, the transaction is already committed,
                    // we only call afterVisible when publish completes
                    cb.afterVisible(txnState, txnOperated);
                } else {
                    // For one-phase commit, call both afterCommitted and afterVisible
                    cb.afterCommitted(txnState, txnOperated);
                    // do not execute afterVisible if commit txn failed in cloud mode
                    if (txnOperated) {
                        cb.afterVisible(txnState, txnOperated);
                    }
                }
            } catch (Throwable t) {
                LOG.warn("callback execution failed for txnId={}, callbackId={}", txnId, callbackId, t);
            }
        }
    }

    /**
     * Common after-commit operations: update versions, stats, and produce events.
     * This method is the single source of truth for all post-visible operations
     * used by both one-phase commit (via afterCommitTxnResp) and two-phase publish
     * (via CloudPublishDaemon.executeLightweightPublish).
     *
     * @param commitTxnResponse commit txn response from MS (either commit or lightweight publish)
     * @param tabletIds list of tablet ids involved in the transaction, for tablet stat notification
     */
    public static void afterCommitCommon(CommitTxnResponse commitTxnResponse, List<Long> tabletIds) {
        long dbId = commitTxnResponse.getTxnInfo().getDbId();
        long txnId = commitTxnResponse.getTxnInfo().getTxnId();

        // 1. update rowCount for AnalysisManager
        Map<Long, Long> updatedRows = new HashMap<>();
        for (Cloud.TableStatsPB tableStats : commitTxnResponse.getTableStatsList()) {
            if (tableStats.hasUpdatedRowCount()) {
                LOG.info("Update RowCount for AnalysisManager. transactionId:{}, table_id:{}, updated_row_count:{}",
                        txnId, tableStats.getTableId(), tableStats.getUpdatedRowCount());
                updatedRows.put(tableStats.getTableId(), tableStats.getUpdatedRowCount());
            }
        }
        Env env = Env.getCurrentEnv();
        env.getAnalysisManager().updateUpdatedRows(updatedRows);

        // 2. update table and partition version
        Map<Long, List<Long>> tablePartitionMap = updateVersion(commitTxnResponse);

        // 3. notify partition first load
        env.getAnalysisManager().setNewPartitionLoaded(
                tablePartitionMap.keySet().stream().collect(Collectors.toList()));
        if (LOG.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<Long, List<Long>> entry : tablePartitionMap.entrySet()) {
                sb.append(entry.getKey()).append(":[");
                for (Long partitionId : entry.getValue()) {
                    sb.append(partitionId).append(",");
                }
                sb.append("];");
            }
            if (sb.length() > 0) {
                LOG.debug("notify partition first load. {}", sb);
            }
        }

        // 4. notify update tablet stats
        if (tabletIds != null && !tabletIds.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("force sync tablet stats for txnId: {}, tabletNum: {}, tabletIds: {}", txnId,
                        tabletIds.size(), tabletIds);
            }
            CloudTabletStatMgr.getInstance().addActiveTablets(tabletIds);
        }

        // 5. produce event
        List<Long> tableList = commitTxnResponse.getTxnInfo().getTableIdsList()
                .stream().distinct().collect(Collectors.toList());
        try {
            for (Long tableId : tableList) {
                Env.getCurrentEnv().getEventProcessor().processEvent(
                    new DataChangeEvent(InternalCatalog.INTERNAL_CATALOG_ID, dbId, tableId));
            }
        } catch (Throwable t) {
            LOG.warn("produceEvent failed, db {}, tables {} ", dbId, tableList, t);
        }
    }

    /**
     * Update partition and table versions from the MS commit/publish response.
     * Returns a map of tableId to first-load partition ids (partitions with version == 2).
     *
     * @param commitTxnResponse the response from MS containing partition versions and table stats
     * @return map of tableId to list of first-load partitionIds
     */
    public static Map<Long, List<Long>> updateVersion(CommitTxnResponse commitTxnResponse) {
        long dbId = commitTxnResponse.getTxnInfo().getDbId();
        long txnId = commitTxnResponse.getTxnInfo().getTxnId();
        int totalPartitionNum = commitTxnResponse.getPartitionIdsList().size();
        if (totalPartitionNum == 0 && commitTxnResponse.getTableStatsList().isEmpty()) {
            return Collections.emptyMap();
        }
        Env env = Env.getCurrentEnv();
        // partition -> <version, versionUpdateTime>
        Map<CloudPartition, Pair<Long, Long>> partitionVersionMap = new HashMap<>();
        // a map to record <tableId, [firstLoadPartitionIds]>
        Map<Long, List<Long>> tablePartitionMap = Maps.newHashMap();
        for (int idx = 0; idx < totalPartitionNum; ++idx) {
            long version = commitTxnResponse.getVersions(idx);
            long tableId = commitTxnResponse.getTableIds(idx);
            if (version == 2) {
                // inform AnalysisManager first load partitions
                tablePartitionMap.computeIfAbsent(tableId, k -> Lists.newArrayList());
                tablePartitionMap.get(tableId).add(commitTxnResponse.getPartitionIds(idx));
            }
            // update CloudPartition
            OlapTable olapTable = (OlapTable) env.getInternalCatalog().getDb(dbId)
                    .flatMap(db -> db.getTable(tableId)).filter(t -> t.isManagedTable())
                    .orElse(null);
            if (olapTable == null) {
                continue;
            }
            CloudPartition partition = (CloudPartition) olapTable.getPartition(
                    commitTxnResponse.getPartitionIds(idx));
            if (partition == null) {
                continue;
            }
            if (version == 2) {
                partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)
                        .stream().forEach(i -> i.setRowCountReported(false));
            }
            partitionVersionMap.put(partition, Pair.of(version, commitTxnResponse.getVersionUpdateTimeMs()));
        }
        // collect table versions
        Optional<Database> dbOpt = env.getInternalCatalog().getDb(dbId);
        if (!dbOpt.isPresent()) {
            LOG.warn("updateVersion: database {} not found, skip table version update", dbId);
            pushVersionAsync(env, dbId, Collections.emptyList(), partitionVersionMap);
            return tablePartitionMap;
        }
        Database db = dbOpt.get();
        List<Pair<OlapTable, Long>> tableVersions = new ArrayList<>(commitTxnResponse.getTableStatsList().size());
        for (TableStatsPB tableStats : commitTxnResponse.getTableStatsList()) {
            if (!tableStats.hasTableVersion()) {
                continue;
            }
            Table table = db.getTableNullable(tableStats.getTableId());
            if (table == null || !table.isManagedTable()) {
                continue;
            }
            tableVersions.add(Pair.of((OlapTable) table, tableStats.getTableVersion()));
        }
        Collections.sort(tableVersions, Comparator.comparingLong(o -> o.first.getId()));
        // update partition version and table version
        for (Pair<OlapTable, Long> tableVersion : tableVersions) {
            tableVersion.first.versionWriteLock();
        }
        try {
            partitionVersionMap.forEach((partition, versionPair) -> {
                partition.setCachedVisibleVersion(versionPair.first, versionPair.second);
                LOG.info("Update Partition. transactionId:{}, table_id:{}, partition_id:{}, version:{}, update time:{}",
                        txnId, partition.getTableId(), partition.getId(), versionPair.first, versionPair.second);
            });
            for (Pair<OlapTable, Long> tableVersion : tableVersions) {
                tableVersion.first.setCachedTableVersion(tableVersion.second);
                LOG.info("Update Table. transactionId:{}, table_id:{}, version:{}", txnId, tableVersion.first.getId(),
                        tableVersion.second);
            }
        } finally {
            for (int i = tableVersions.size() - 1; i >= 0; i--) {
                tableVersions.get(i).first.versionWriteUnlock();
            }
        }
        // notify follower and observer FE to update their version cache
        pushVersionAsync(env, dbId, tableVersions, partitionVersionMap);
        return tablePartitionMap;
    }

    private static void pushVersionAsync(Env env, long dbId,
            List<Pair<OlapTable, Long>> tableVersions,
            Map<CloudPartition, Pair<Long, Long>> partitionVersionMap) {
        if (env instanceof CloudEnv) {
            ((CloudEnv) env).getCloudFEVersionSynchronizer()
                    .pushVersionAsync(dbId, tableVersions, partitionVersionMap);
        }
    }

}

