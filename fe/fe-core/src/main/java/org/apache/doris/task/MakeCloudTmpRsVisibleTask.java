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

import org.apache.doris.thrift.TMakeCloudTmpRsVisibleRequest;
import org.apache.doris.thrift.TTaskType;

import java.util.List;
import java.util.Map;

/**
 * Task to notify BE to make temporary cloud rowsets visible.
 * After FE commits a transaction to MS, this task notifies BE to promote
 * the temporary rowsets from CloudCommittedRSMgr to tablet meta.
 */
public class MakeCloudTmpRsVisibleTask extends AgentTask {
    private final long txnId;
    private final List<Long> tabletIds; // tablets on this BE involved in the transaction
    private final Map<Long, Long> partitionVersionMap; // partition_id -> version
    private final long updateVersionVisibleTime;

    public MakeCloudTmpRsVisibleTask(long backendId, long txnId,
                                     List<Long> tabletIds,
                                     Map<Long, Long> partitionVersionMap,
                                     long updateVersionVisibleTime) {
        super(null, backendId, TTaskType.MAKE_CLOUD_TMP_RS_VISIBLE,
              -1L, -1L, -1L, -1L, -1L, txnId, System.currentTimeMillis());
        this.txnId = txnId;
        this.tabletIds = tabletIds;
        this.partitionVersionMap = partitionVersionMap;
        this.updateVersionVisibleTime = updateVersionVisibleTime;
    }

    public long getTxnId() {
        return txnId;
    }

    public List<Long> getTabletIds() {
        return tabletIds;
    }

    public Map<Long, Long> getPartitionVersionMap() {
        return partitionVersionMap;
    }

    public long getUpdateVersionVisibleTime() {
        return updateVersionVisibleTime;
    }

    public TMakeCloudTmpRsVisibleRequest toThrift() {
        TMakeCloudTmpRsVisibleRequest request = new TMakeCloudTmpRsVisibleRequest();
        request.setTxnId(txnId);
        request.setTabletIds(tabletIds);
        request.setPartitionVersionMap(partitionVersionMap);
        if (updateVersionVisibleTime > 0) {
            request.setUpdateVersionVisibleTime(updateVersionVisibleTime);
        }
        return request;
    }
}
