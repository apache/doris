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

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SingleRangePartitionDesc;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class EsIndexState {
    
    private static final Logger LOG = LogManager.getLogger(EsIndexState.class);

    private final String indexName;
    // shardid -> host1, host2, host3
    private Map<Integer, List<EsShardRouting>> shardRoutings;
    private SingleRangePartitionDesc partitionDesc;
    private PartitionKey partitionKey;
    private long partitionId = -1;
    
    public EsIndexState(String indexName) {
        this.indexName = indexName;
        this.shardRoutings = Maps.newHashMap();
        this.partitionDesc = null;
        this.partitionKey = null;
    }


    public void addHttpAddress(Map<String, EsNodeInfo> nodesInfo) {
        for (Map.Entry<Integer, List<EsShardRouting>> entry : shardRoutings.entrySet()) {
            List<EsShardRouting> shardRoutings = entry.getValue();
            for (EsShardRouting shardRouting : shardRoutings) {
                String nodeId = shardRouting.getNodeId();
                if (nodesInfo.containsKey(nodeId)) {
                    shardRouting.setHttpAddress(nodesInfo.get(nodeId).getPublishAddress());
                } else {
                    shardRouting.setHttpAddress(randomAddress(nodesInfo));
                }
            }
        }
    }

    public TNetworkAddress randomAddress(Map<String, EsNodeInfo> nodesInfo) {
        int seed = new Random().nextInt() % nodesInfo.size();
        EsNodeInfo[] nodeInfos = (EsNodeInfo[]) nodesInfo.values().toArray();
        return nodeInfos[seed].getPublishAddress();
    }
    
    public static EsIndexState parseIndexStateV55(String indexName, JSONObject indicesRoutingMap, 
            JSONObject nodesMap, 
            JSONObject indicesMetaMap, PartitionInfo partitionInfo) throws AnalysisException {
        EsIndexState indexState = new EsIndexState(indexName);
        JSONObject shardRoutings = indicesRoutingMap.getJSONObject(indexName).getJSONObject("shards");
        for (String shardKey : shardRoutings.keySet()) {
            List<EsShardRouting> singleShardRouting = Lists.newArrayList();
            JSONArray shardRouting = shardRoutings.getJSONArray(shardKey);
            for (int i = 0; i < shardRouting.length(); ++i) {
                JSONObject shard = shardRouting.getJSONObject(i);
                String shardState = shard.getString("state");
                if ("STARTED".equalsIgnoreCase(shardState)) {
                    try {
                        singleShardRouting.add(EsShardRouting.parseShardRoutingV55(shardState, 
                                shardKey, shard, nodesMap));
                    } catch (Exception e) {
                        LOG.info("errors while parse shard routing from json [{}], ignore this shard", shard.toString(), e);
                    }
                } 
            }
            if (singleShardRouting.isEmpty()) {
                LOG.warn("could not find a healthy allocation for [{}][{}]", indexName, shardKey);
            }
            indexState.addShardRouting(Integer.valueOf(shardKey), singleShardRouting);
        }

        // get some meta info from es, could be used to prune index when query
        // index.bpack.partition.upperbound: stu_age
        if (partitionInfo != null && partitionInfo instanceof RangePartitionInfo) {
            JSONObject indexMeta = indicesMetaMap.getJSONObject(indexName);
            JSONObject partitionSetting = EsUtil.getJsonObject(indexMeta, "settings.index.bpack.partition", 0);
            LOG.debug("index {} range partition setting is {}", indexName, 
                    partitionSetting == null ? "" : partitionSetting.toString());
            if (partitionSetting != null && partitionSetting.has("upperbound")) {
                String upperBound = partitionSetting.getString("upperbound");
                List<PartitionValue> upperValues = Lists.newArrayList(new PartitionValue(upperBound));
                PartitionKeyDesc partitionKeyDesc = new PartitionKeyDesc(upperValues);
                // use index name as partition name
                SingleRangePartitionDesc desc = new SingleRangePartitionDesc(false, 
                        indexName, partitionKeyDesc, null);
                PartitionKey partitionKey = PartitionKey.createPartitionKey(
                        desc.getPartitionKeyDesc().getUpperValues(), 
                        ((RangePartitionInfo) partitionInfo).getPartitionColumns());
                desc.analyze(((RangePartitionInfo) partitionInfo).getPartitionColumns().size(), null);
                indexState.setPartitionDesc(desc);
                indexState.setPartitionKey(partitionKey);
            }
        }
        return indexState;
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

    public SingleRangePartitionDesc getPartitionDesc() {
        return partitionDesc;
    }

    public void setPartitionDesc(SingleRangePartitionDesc partitionDesc) {
        this.partitionDesc = partitionDesc;
    }

    public PartitionKey getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(PartitionKey partitionKey) {
        this.partitionKey = partitionKey;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(long partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    public String toString() {
        return "EsIndexState [indexName=" + indexName + ", partitionDesc=" + partitionDesc + ", partitionKey="
                + partitionKey + "]";
    }
}
