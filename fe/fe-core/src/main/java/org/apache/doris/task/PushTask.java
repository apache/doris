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
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.Predicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.thrift.TBrokerScanRange;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TCondition;
import org.apache.doris.thrift.TDescriptorTable;
import org.apache.doris.thrift.TPriority;
import org.apache.doris.thrift.TPushReq;
import org.apache.doris.thrift.TPushType;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    // for light schema change
    private List<TColumn> columnsDesc = null;

    public PushTask(TResourceInfo resourceInfo, long backendId, long dbId, long tableId, long partitionId, long indexId,
            long tabletId, long replicaId, int schemaHash, long version, String filePath, long fileSize,
            int timeoutSecond, long loadJobId, TPushType pushType, List<Predicate> conditions, boolean needDecompress,
            TPriority priority, TTaskType taskType, long transactionId, long signature, List<TColumn> columnsDesc) {
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
        this.columnsDesc = columnsDesc;
    }

    // for load v2 (SparkLoadJob)
    public PushTask(long backendId, long dbId, long tableId, long partitionId, long indexId, long tabletId,
            long replicaId, int schemaHash, int timeoutSecond, long loadJobId, TPushType pushType, TPriority priority,
            long transactionId, long signature, TBrokerScanRange tBrokerScanRange, TDescriptorTable tDescriptorTable,
            List<TColumn> columnsDesc) {
        this(null, backendId, dbId, tableId, partitionId, indexId, tabletId, replicaId, schemaHash, -1, null, 0,
                timeoutSecond, loadJobId, pushType, null, false, priority, TTaskType.REALTIME_PUSH, transactionId,
                signature, columnsDesc);
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
                request.setHttpFilePath(filePath);
                if (fileSize != -1) {
                    request.setHttpFileSize(fileSize);
                }
                request.setNeedDecompress(needDecompress);
                break;
            case DELETE:
                List<TCondition> tConditions = new ArrayList<TCondition>();
                Map<String, TColumn> colNameToColDesc = columnsDesc.stream()
                        .collect(Collectors.toMap(c -> c.getColumnName(), Function.identity(), (v1, v2) -> v1,
                                () -> Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER)));
                for (Predicate condition : conditions) {
                    TCondition tCondition = new TCondition();
                    ArrayList<String> conditionValues = new ArrayList<String>();

                    SlotRef slotRef = (SlotRef) condition.getChild(0);
                    String columnName = new String(slotRef.getColumnName());
                    TColumn column = colNameToColDesc.get(slotRef.getColumnName());
                    if (column == null) {
                        columnName = CreateMaterializedViewStmt.mvColumnBuilder(columnName);
                        column = colNameToColDesc.get(columnName);
                        // condition's name and column's name may have inconsistent case
                        columnName = column.getColumnName();
                    }

                    tCondition.setColumnName(columnName);

                    if (condition instanceof BinaryPredicate) {
                        BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
                        String value = ((LiteralExpr) binaryPredicate.getChild(1)).getStringValue();
                        Operator op = binaryPredicate.getOp();
                        tCondition.setConditionOp(op.toString());
                        conditionValues.add(value);
                    } else if (condition instanceof IsNullPredicate) {
                        IsNullPredicate isNullPredicate = (IsNullPredicate) condition;
                        String op = "IS";
                        String value = "NULL";
                        if (isNullPredicate.isNotNull()) {
                            value = "NOT NULL";
                        }
                        tCondition.setConditionOp(op);
                        conditionValues.add(value);
                    } else if (condition instanceof InPredicate) {
                        InPredicate inPredicate = (InPredicate) condition;
                        String op = inPredicate.isNotIn() ? "!*=" : "*=";
                        tCondition.setConditionOp(op);
                        for (int i = 1; i <= inPredicate.getInElementNum(); i++) {
                            conditionValues.add(inPredicate.getChild(i).getStringValue());
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
        request.setColumnsDesc(columnsDesc);

        return request;
    }

    public void setCountDownLatch(MarkedCountDownLatch latch) {
        this.latch = latch;
    }

    public void countDownLatch(long backendId, long tabletId) {
        if (this.latch != null) {
            if (latch.markedCountDown(backendId, tabletId)) {
                LOG.debug("pushTask current latch count: {}. backend: {}, tablet:{}", latch.getCount(), backendId,
                        tabletId);
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
