// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.task;

import com.baidu.palo.analysis.BinaryPredicate;
import com.baidu.palo.analysis.BinaryPredicate.Operator;
import com.baidu.palo.analysis.IsNullPredicate;
import com.baidu.palo.analysis.LiteralExpr;
import com.baidu.palo.analysis.Predicate;
import com.baidu.palo.analysis.SlotRef;
import com.baidu.palo.common.MarkedCountDownLatch;
import com.baidu.palo.thrift.TCondition;
import com.baidu.palo.thrift.TPriority;
import com.baidu.palo.thrift.TPushReq;
import com.baidu.palo.thrift.TPushType;
import com.baidu.palo.thrift.TResourceInfo;
import com.baidu.palo.thrift.TTaskType;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.ArrayList;
import java.util.List;

public class PushTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(CreateReplicaTask.class);

    private long replicaId;
    private int schemaHash;
    private long version;
    private long versionHash;
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

    public PushTask(TResourceInfo resourceInfo, long backendId, long dbId, long tableId, long partitionId,
                    long indexId, long tabletId, long replicaId, int schemaHash, long version, long versionHash, 
                    String filePath, long fileSize, int timeoutSecond, long loadJobId, TPushType pushType,
                    List<Predicate> conditions, boolean needDecompress, TPriority priority) {
        super(resourceInfo, backendId, TTaskType.PUSH, dbId, tableId, partitionId, indexId, tabletId);
        this.replicaId = replicaId;
        this.schemaHash = schemaHash;
        this.version = version;
        this.versionHash = versionHash;
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
    }

    public TPushReq toThrift() {
        TPushReq request = new TPushReq(tabletId, schemaHash, version, versionHash, timeoutSecond, pushType);
        switch (pushType) {
            case LOAD:
            case LOAD_DELETE:
                request.setHttp_file_path(filePath);
                if (fileSize != -1) {
                    request.setHttp_file_size(fileSize);
                }
                request.setNeed_decompress(needDecompress);
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
                        tCondition.setColumn_name(columnName);
                        tCondition.setCondition_op(op.toString());
                        conditionValues.add(value);
                    } else if (condition instanceof IsNullPredicate) {
                        IsNullPredicate isNullPredicate = (IsNullPredicate) condition;
                        String columnName = ((SlotRef) isNullPredicate.getChild(0)).getColumnName();
                        String op = "IS";
                        String value = "NULL";
                        if (isNullPredicate.isNotNull()) {
                            value = "NOT NULL";
                        }
                        tCondition.setColumn_name(columnName);
                        tCondition.setCondition_op(op);
                        conditionValues.add(value);
                    }

                    tCondition.setCondition_values(conditionValues);

                    tConditions.add(tCondition);
                }
                request.setDelete_conditions(tConditions);
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
                LOG.info("pushTask current latch count: {}. backend: {}, tablet:{}",
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
    
    public long getVersionHash() {
        return versionHash;
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
}
