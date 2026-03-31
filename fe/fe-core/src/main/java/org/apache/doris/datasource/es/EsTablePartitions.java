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

package org.apache.doris.datasource.es;

import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.common.DdlException;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * save the dynamic info parsed from es cluster state such as shard routing, partition info
 */
public class EsTablePartitions {

    private static final Logger LOG = LogManager.getLogger(EsTablePartitions.class);

    private PartitionInfo partitionInfo;
    private Map<Long, String> partitionIdToIndices;
    private Map<String, EsShardPartitions> partitionedIndexStates;
    private Map<String, EsShardPartitions> unPartitionedIndexStates;

    public EsTablePartitions() {
        partitionInfo = null;
        partitionIdToIndices = Maps.newHashMap();
        partitionedIndexStates = Maps.newHashMap();
        unPartitionedIndexStates = Maps.newHashMap();
    }

    /**
     * Build EsTablePartitions from shard partitions for ES Catalog.
     * ES Catalog tables do not have user-defined partition info, so partitionInfo is always null.
     */
    public static EsTablePartitions fromShardPartitions(EsExternalTable esTable, EsShardPartitions shardPartitions)
            throws DorisEsException, DdlException {
        EsTablePartitions esTablePartitions = new EsTablePartitions();
        // ES Catalog tables do not have user-defined partition info
        esTablePartitions.addIndexState(esTable.getIndexName(), shardPartitions);
        if (LOG.isDebugEnabled()) {
            LOG.debug("add index {} to es table {}", shardPartitions, esTable.getName());
        }
        return esTablePartitions;
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

    public void addIndexState(String indexName, EsShardPartitions indexState) {
        if (indexState.getPartitionDesc() != null) {
            partitionedIndexStates.put(indexName, indexState);
        } else {
            unPartitionedIndexStates.put(indexName, indexState);
        }
    }

    public Map<String, EsShardPartitions> getPartitionedIndexStates() {
        return partitionedIndexStates;
    }

    public Map<String, EsShardPartitions> getUnPartitionedIndexStates() {
        return unPartitionedIndexStates;
    }

    public EsShardPartitions getEsShardPartitions(long partitionId) {
        if (partitionIdToIndices.containsKey(partitionId)) {
            return partitionedIndexStates.get(partitionIdToIndices.get(partitionId));
        }
        return null;
    }

    public EsShardPartitions getEsShardPartitions(String indexName) {
        if (partitionedIndexStates.containsKey(indexName)) {
            return partitionedIndexStates.get(indexName);
        }
        return unPartitionedIndexStates.get(indexName);
    }
}
