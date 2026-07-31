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

package org.apache.doris.connector.es;

import org.apache.doris.connector.api.DorisConnectorException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Shard partitions for an ES index — maps shard IDs to routing entries.
 * Adapted from fe-core's EsShardPartitions — no SinglePartitionDesc/PartitionKey dependencies.
 */
public class EsShardPartitions {

    private static final Logger LOG = LogManager.getLogger(EsShardPartitions.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String indexName;
    private final Map<Integer, List<EsShardRouting>> shardRoutings;

    public EsShardPartitions(String indexName) {
        this.indexName = indexName;
        this.shardRoutings = new HashMap<>();
    }

    /**
     * Parse shard routings from the _search_shards JSON response.
     */
    public static EsShardPartitions findShardPartitions(String indexName, String searchShards) {
        EsShardPartitions partitions = new EsShardPartitions(indexName);
        try {
            ObjectNode jsonObject = (ObjectNode) MAPPER.readTree(searchShards);
            ArrayNode shards = (ArrayNode) jsonObject.get("shards");
            if (shards == null) {
                throw new DorisConnectorException(
                        "ES _search_shards response missing 'shards' for index: " + indexName);
            }
            int size = shards.size();
            for (int i = 0; i < size; i++) {
                List<EsShardRouting> singleShardRouting = new ArrayList<>();
                ArrayNode shardsArray = (ArrayNode) shards.get(i);
                for (int j = 0; j < shardsArray.size(); j++) {
                    ObjectNode indexShard = (ObjectNode) shardsArray.get(j);
                    String shardState = indexShard.get("state").asText();
                    if ("STARTED".equalsIgnoreCase(shardState)
                            || "RELOCATING".equalsIgnoreCase(shardState)) {
                        singleShardRouting.add(new EsShardRouting(
                                indexShard.get("index").asText(),
                                indexShard.get("shard").asInt(),
                                indexShard.get("primary").asBoolean(),
                                indexShard.get("node").asText()));
                    }
                }
                if (singleShardRouting.isEmpty()) {
                    LOG.error("could not find a healthy allocation for [{}][{}]",
                            indexName, i);
                    continue;
                }
                partitions.addShardRouting(i, singleShardRouting);
            }
        } catch (IOException e) {
            throw new DorisConnectorException(
                    "fetch [" + indexName + "] shard partitions failure: " + e.getMessage());
        }
        return partitions;
    }

    /**
     * Resolve HTTP addresses for shard routings from node info.
     */
    public void addHttpAddress(Map<String, EsNodeInfo> nodesInfo) {
        for (Map.Entry<Integer, List<EsShardRouting>> entry : shardRoutings.entrySet()) {
            for (EsShardRouting routing : entry.getValue()) {
                String nodeId = routing.getNodeId();
                if (nodesInfo.containsKey(nodeId)) {
                    EsNodeInfo node = nodesInfo.get(nodeId);
                    routing.setHttpAddress(node.getPublishHost(), node.getPublishPort());
                } else {
                    EsNodeInfo randomNode = randomNode(nodesInfo);
                    routing.setHttpAddress(
                            randomNode.getPublishHost(), randomNode.getPublishPort());
                }
            }
        }
    }

    private EsNodeInfo randomNode(Map<String, EsNodeInfo> nodesInfo) {
        if (nodesInfo.isEmpty()) {
            throw new DorisConnectorException("No available ES nodes for routing assignment");
        }
        int seed = new SecureRandom().nextInt(Short.MAX_VALUE) % nodesInfo.size();
        EsNodeInfo[] nodeInfos = nodesInfo.values().toArray(new EsNodeInfo[0]);
        return nodeInfos[seed];
    }

    public void addShardRouting(int shardId, List<EsShardRouting> singleShardRouting) {
        shardRoutings.put(shardId, singleShardRouting);
    }

    public String getIndexName() {
        return indexName;
    }

    public Map<Integer, List<EsShardRouting>> getShardRoutings() {
        return shardRoutings;
    }

    @Override
    public String toString() {
        return "EsShardPartitions{indexName='" + indexName + "', shards=" + shardRoutings.size() + "}";
    }
}
