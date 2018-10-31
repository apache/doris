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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.mortbay.util.ajax.JSON;

import com.baidu.palo.analysis.PartitionKeyDesc;
import com.baidu.palo.analysis.SingleRangePartitionDesc;
import com.baidu.palo.catalog.PartitionInfo;
import com.baidu.palo.catalog.PartitionKey;
import com.baidu.palo.catalog.RangePartitionInfo;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.thrift.TNetworkAddress;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
                    singleShardRouting.add(EsShardRouting.parseShardRoutingV55(shardState, 
                            shardKey, shard, nodesMap));
                } 
            }
            if (singleShardRouting.isEmpty()) {
                LOG.warn("could not find a healthy allocation for [{}][{}]", indexName, shardKey);
            }
            indexState.addShardRouting(Integer.valueOf(shardKey), singleShardRouting);
        }

        // get some meta info from es, could be used to prune index when query
        // index.bpack.partition.upperbound: stu_age
        if (partitionInfo instanceof RangePartitionInfo) {
            JSONObject indexMeta = indicesMetaMap.getJSONObject(indexName);
            JSONObject partitionSetting = EsUtil.getJsonObject(indexMeta, "settings.index.bpack.partition", 0);
            LOG.debug("index {} range partition setting is {}", indexName, 
                    partitionSetting == null ? "" : partitionSetting.toString());
            if (partitionSetting != null && partitionSetting.has("upperbound")) {
                String upperBound = partitionSetting.getString("upperbound");
                List<String> upperValues = Stream.of(upperBound).collect(Collectors.toList());
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
