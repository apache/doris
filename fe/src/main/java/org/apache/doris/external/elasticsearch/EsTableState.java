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

package org.apache.doris.external.elasticsearch;

import java.util.Map;
import java.util.Random;

import org.apache.doris.catalog.PartitionInfo;
import com.google.common.collect.Maps;
import org.apache.doris.thrift.TNetworkAddress;

/**
 * save the dynamic info parsed from es cluster state such as shard routing, partition info
 */
public class EsTableState {

    private PartitionInfo partitionInfo;
    private Map<Long, String> partitionIdToIndices;
    private Map<String, EsIndexState> partitionedIndexStates;
    private Map<String, EsIndexState> unPartitionedIndexStates;

    public EsTableState() {
        partitionInfo = null;
        partitionIdToIndices = Maps.newHashMap();
        partitionedIndexStates = Maps.newHashMap();
        unPartitionedIndexStates = Maps.newHashMap();
    }

    public void addHttpAddress(Map<String, EsNodeInfo> nodesInfo) {
        for (EsIndexState indexState : partitionedIndexStates.values()) {
            indexState.addHttpAddress(nodesInfo);
        }
        for (EsIndexState indexState : unPartitionedIndexStates.values()) {
            indexState.addHttpAddress(nodesInfo);
        }

    }

    public TNetworkAddress randomAddress(Map<String, EsNodeInfo> nodesInfo) {
        int seed = new Random().nextInt() % nodesInfo.size();
        EsNodeInfo[] nodeInfos = (EsNodeInfo[]) nodesInfo.values().toArray();
        return nodeInfos[seed].getPublishAddress();
    }
    
    public PartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public void setPartitionInfo(PartitionInfo partitionInfo) {
        this.partitionInfo = partitionInfo;
    }

    public Map<Long, String> getPartitionIdToIndices() {
        return partitionIdToIndices;
    }
    
    public void addPartition(String indexName, long partitionId) {
        partitionIdToIndices.put(partitionId, indexName);
    }
    
    public void addIndexState(String indexName, EsIndexState indexState) {
        if (indexState.getPartitionDesc() != null) {
            partitionedIndexStates.put(indexName, indexState);
        } else {
            unPartitionedIndexStates.put(indexName, indexState);
        }
    }

    public Map<String, EsIndexState> getPartitionedIndexStates() {
        return partitionedIndexStates;
    }

    public Map<String, EsIndexState> getUnPartitionedIndexStates() {
        return unPartitionedIndexStates;
    }
    
    public EsIndexState getIndexState(long partitionId) {
        if (partitionIdToIndices.containsKey(partitionId)) {
            return partitionedIndexStates.get(partitionIdToIndices.get(partitionId));
        }
        return null;
    }
    
    public EsIndexState getIndexState(String indexName) {
        if (partitionedIndexStates.containsKey(indexName)) {
            return partitionedIndexStates.get(indexName);
        }
        return unPartitionedIndexStates.get(indexName);
    }
}
