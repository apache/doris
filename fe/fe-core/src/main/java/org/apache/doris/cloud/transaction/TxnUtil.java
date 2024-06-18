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

import org.apache.doris.cloud.proto.Cloud.RLTaskTxnCommitAttachmentPB;
import org.apache.doris.cloud.proto.Cloud.RoutineLoadProgressPB;
import org.apache.doris.cloud.proto.Cloud.TxnCommitAttachmentPB;
import org.apache.doris.cloud.proto.Cloud.TxnCommitAttachmentPB.LoadJobFinalOperationPB;
import org.apache.doris.cloud.proto.Cloud.TxnCommitAttachmentPB.LoadJobFinalOperationPB.EtlStatusPB;
import org.apache.doris.cloud.proto.Cloud.TxnCommitAttachmentPB.LoadJobFinalOperationPB.FailMsgPB;
import org.apache.doris.cloud.proto.Cloud.TxnCommitAttachmentPB.LoadJobFinalOperationPB.JobStatePB;
import org.apache.doris.cloud.proto.Cloud.TxnCoordinatorPB;
import org.apache.doris.cloud.proto.Cloud.TxnInfoPB;
import org.apache.doris.cloud.proto.Cloud.TxnSourceTypePB;
import org.apache.doris.cloud.proto.Cloud.UniqueIdPB;
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;

public class TxnUtil {
    private static final Logger LOG = LogManager.getLogger(TxnUtil.class);

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
}
