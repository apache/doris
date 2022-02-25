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

package org.apache.doris.task;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.Predicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.thrift.TBrokerScanRange;
import org.apache.doris.thrift.TCondition;
import org.apache.doris.thrift.TDescriptorTable;
import org.apache.doris.thrift.TPriority;
import org.apache.doris.thrift.TPushReq;
import org.apache.doris.thrift.TPushType;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TTaskType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class PushTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(PushTask.class);

    private long replicaId;
    private int schemaHash;
    private long version;
    private String filePath;
    private long fileSize;
    private int timeoutSecond;
    private long loadJobId;
    private TPushType pushType;
    private List<Predicate> conditions;
    // for synchronous delete
    private MarkedCountDownLatch latch;

    // lzop decompress or not
    private boolean needDecompress;

    private TPriority priority;
    private boolean isSyncDelete;
    private long asyncDeleteJobId;
    
    private long transactionId;
    private boolean isSchemaChanging;

    // for load v2 (spark load)
    private TBrokerScanRange tBrokerScanRange;
    private TDescriptorTable tDescriptorTable;
    
    public PushTask(TResourceInfo resourceInfo, long backendId, long dbId, long tableId, long partitionId,
                    long indexId, long tabletId, long replicaId, int schemaHash, long version,
                    String filePath, long fileSize, int timeoutSecond, long loadJobId, TPushType pushType,
                    List<Predicate> conditions, boolean needDecompress, TPriority priority, TTaskType taskType, 
                    long transactionId, long signature) {
        super(resourceInfo, backendId, taskType, dbId, tableId, partitionId, indexId, tabletId, signature);
        this.replicaId = replicaId;
        this.schemaHash = schemaHash;
        this.version = version;
        this.filePath = filePath;
        this.fileSize = fileSize;
        this.timeoutSecond = timeoutSecond;
        this.loadJobId = loadJobId;
        this.pushType = pushType;
        this.conditions = conditions;
        this.latch = null;
        this.needDecompress = needDecompress;
        this.priority = priority;
        this.isSyncDelete = true;
        this.asyncDeleteJobId = -1;
        this.transactionId = transactionId;
        this.tBrokerScanRange = null;
        this.tDescriptorTable = null;
    }

    public PushTask(TResourceInfo resourceInfo, long backendId, long dbId, long tableId, long partitionId,
            long indexId, long tabletId, long replicaId, int schemaHash, long version, 
            String filePath, long fileSize, int timeoutSecond, long loadJobId, TPushType pushType,
            List<Predicate> conditions, boolean needDecompress, TPriority priority) {
        this(resourceInfo, backendId, dbId, tableId, partitionId, indexId, 
             tabletId, replicaId, schemaHash, version, filePath, 
             fileSize, timeoutSecond, loadJobId, pushType, conditions, needDecompress, 
             priority, TTaskType.PUSH, -1, tableId);
    }

    // for load v2 (SparkLoadJob)
    public PushTask(long backendId, long dbId, long tableId, long partitionId, long indexId, long tabletId,
                    long replicaId, int schemaHash, int timeoutSecond, long loadJobId, TPushType pushType,
                    TPriority priority, long transactionId, long signature,
                    TBrokerScanRange tBrokerScanRange, TDescriptorTable tDescriptorTable) {
        this(null, backendId, dbId, tableId, partitionId, indexId,
             tabletId, replicaId, schemaHash, -1, null,
             0, timeoutSecond, loadJobId, pushType, null, false,
             priority, TTaskType.REALTIME_PUSH, transactionId, signature);
        this.tBrokerScanRange = tBrokerScanRange;
        this.tDescriptorTable = tDescriptorTable;
    }

    public TPushReq toThrift() {
        TPushReq request = new TPushReq(tabletId, schemaHash, version, 0 /*versionHash*/, timeoutSecond, pushType);
        if (taskType == TTaskType.REALTIME_PUSH) {
            request.setPartitionId(partitionId);
            request.setTransactionId(transactionId);
        }
        request.setIsSchemaChanging(isSchemaChanging);
        switch (pushType) {
            case LOAD:
            case LOAD_DELETE:
                request.setHttpFilePath(filePath);
                if (fileSize != -1) {
                    request.setHttpFileSize(fileSize);
                }
                request.setNeedDecompress(needDecompress);
                break;
            case DELETE:
                List<TCondition> tConditions = new ArrayList<TCondition>();
                for (Predicate condition : conditions) {
                    TCondition tCondition = new TCondition();
                    ArrayList<String> conditionValues = new ArrayList<String>();
                    if (condition instanceof BinaryPredicate) {
                        BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
                        String columnName = ((SlotRef) binaryPredicate.getChild(0)).getColumnName();
                        String value = ((LiteralExpr) binaryPredicate.getChild(1)).getStringValue();
                        Operator op = binaryPredicate.getOp();
                        tCondition.setColumnName(columnName);
                        tCondition.setConditionOp(op.toString());
                        conditionValues.add(value);
                    } else if (condition instanceof IsNullPredicate) {
                        IsNullPredicate isNullPredicate = (IsNullPredicate) condition;
                        String columnName = ((SlotRef) isNullPredicate.getChild(0)).getColumnName();
                        String op = "IS";
                        String value = "NULL";
                        if (isNullPredicate.isNotNull()) {
                            value = "NOT NULL";
                        }
                        tCondition.setColumnName(columnName);
                        tCondition.setConditionOp(op);
                        conditionValues.add(value);
                    } else if (condition instanceof InPredicate) {
                        InPredicate inPredicate = (InPredicate) condition;
                        String columnName = ((SlotRef) inPredicate.getChild(0)).getColumnName();
                        String op = inPredicate.isNotIn() ? "!*=" : "*=";
                        tCondition.setColumnName(columnName);
                        tCondition.setConditionOp(op);
                        for (int i = 1; i <= inPredicate.getInElementNum(); i++) {
                            conditionValues.add(((LiteralExpr)inPredicate.getChild(i)).getStringValue());
                        }
                    }

                    tCondition.setConditionValues(conditionValues);

                    tConditions.add(tCondition);
                }
                request.setDeleteConditions(tConditions);
                break;
            case LOAD_V2:
                request.setBrokerScanRange(tBrokerScanRange);
                request.setDescTbl(tDescriptorTable);
                break;
            default:
                LOG.warn("unknown push type. type: " + pushType.name());
                break;
        }

        return request;
    }
    
    public void setCountDownLatch(MarkedCountDownLatch latch) {
        this.latch = latch;
    }

    public void countDownLatch(long backendId, long tabletId) {
        if (this.latch != null) {
            if (latch.markedCountDown(backendId, tabletId)) {
                LOG.debug("pushTask current latch count: {}. backend: {}, tablet:{}",
                         latch.getCount(), backendId, tabletId);
            }
        }
    }

    public long getReplicaId() {
        return replicaId;
    }
    
    public int getSchemaHash() {
        return schemaHash;
    }
    
    public long getVersion() {
        return version;
    }

    public long getLoadJobId() {
        return loadJobId;
    }
    
    public TPushType getPushType() {
        return pushType;
    }

    public TPriority getPriority() {
        return priority;
    }

    public void setIsSyncDelete(boolean isSyncDelete) {
        this.isSyncDelete = isSyncDelete;
    }

    public boolean isSyncDelete() {
        return isSyncDelete;
    }

    public void setAsyncDeleteJobId(long jobId) {
        this.asyncDeleteJobId = jobId;
    }

    public long getAsyncDeleteJobId() {
        return asyncDeleteJobId;
    }

    public long getTransactionId() {
        return transactionId;
    }
    
    public void setIsSchemaChanging(boolean isSchemaChanging) {
        this.isSchemaChanging = isSchemaChanging;
    }
}
