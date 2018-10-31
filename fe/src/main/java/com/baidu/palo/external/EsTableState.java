// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.external;

import java.util.Map;

import com.baidu.palo.catalog.PartitionInfo;
import com.google.common.collect.Maps;

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
