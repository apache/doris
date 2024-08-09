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

import org.apache.doris.thrift.TPartitionVersionInfo;
import org.apache.doris.thrift.TPublishVersionRequest;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PublishVersionTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(PublishVersionTask.class);

    private long transactionId;
    @Getter
    private List<TPartitionVersionInfo> partitionVersionInfos;

    /**
     * for delta rows statistics to exclude rollup tablets
     */
    private Set<Long> baseTabletsIds = Sets.newHashSet();

    private List<Long> errorTablets;

    // tabletId => version, current version = 0
    private Map<Long, Long> succTablets;

    /**
     * To collect loaded rows for each tablet from each BE
     */
    private final Map<Long, Map<Long, Long>> tableIdToTabletDeltaRows = Maps.newHashMap();

    public PublishVersionTask(long backendId, long transactionId, long dbId,
            List<TPartitionVersionInfo> partitionVersionInfos, long createTime) {
        super(null, backendId, TTaskType.PUBLISH_VERSION, dbId, -1L, -1L, -1L, -1L, transactionId, createTime);
        this.transactionId = transactionId;
        this.partitionVersionInfos = partitionVersionInfos;
        this.succTablets = null;
        this.errorTablets = new ArrayList<>();
        this.isFinished = false;
    }

    public TPublishVersionRequest toThrift() {
        TPublishVersionRequest publishVersionRequest = new TPublishVersionRequest(transactionId,
                partitionVersionInfos);
        publishVersionRequest.setBaseTabletIds(baseTabletsIds);
        return publishVersionRequest;
    }

    public void setBaseTabletsIds(Set<Long> rollupTabletIds) {
        this.baseTabletsIds = rollupTabletIds;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public Map<Long, Long> getSuccTablets() {
        return succTablets;
    }

    public void setSuccTablets(Map<Long, Long> succTablets) {
        this.succTablets = succTablets;
    }

    public synchronized List<Long> getErrorTablets() {
        return errorTablets;
    }

    public synchronized void addErrorTablets(List<Long> errorTablets) {
        this.errorTablets.clear();
        if (errorTablets == null) {
            return;
        }
        this.errorTablets.addAll(errorTablets);
    }

    public void setTableIdTabletsDeltaRows(Map<Long, Map<Long, Long>> tableIdToTabletDeltaRows) {
        this.tableIdToTabletDeltaRows.putAll(tableIdToTabletDeltaRows);
    }

    public Map<Long, Map<Long, Long>> getTableIdToTabletDeltaRows() {
        return tableIdToTabletDeltaRows;
    }

    @Override
    public String toString() {
        return super.toString() + ", txnId=" + transactionId;
    }
}
