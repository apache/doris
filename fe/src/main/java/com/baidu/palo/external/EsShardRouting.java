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


import org.json.JSONObject;

import com.baidu.palo.thrift.TNetworkAddress;

public class EsShardRouting {

    private final String indexName;
    private final int shardId;
    private final boolean isPrimary;
    private final TNetworkAddress address;
    
    public EsShardRouting(String indexName, int shardId, boolean isPrimary, TNetworkAddress address) {
        this.indexName = indexName;
        this.shardId = shardId;
        this.isPrimary = isPrimary;
        this.address = address;
    }
    
    public static EsShardRouting parseShardRoutingV55(String indexName, String shardKey, 
            JSONObject shardInfo, JSONObject nodesMap) {
        String nodeId = shardInfo.getString("node");
        JSONObject nodeInfo = nodesMap.getJSONObject(nodeId);
        String[] transportAddr = nodeInfo.getString("transport_address").split(":");
        // TODO(ygl) should get thrift port from node info
        TNetworkAddress addr = new TNetworkAddress(transportAddr[0], Integer.valueOf(transportAddr[1]));
        boolean isPrimary = shardInfo.getBoolean("primary");
        return new EsShardRouting(indexName, Integer.valueOf(shardKey), 
                isPrimary, addr);
    }
    
    public int getShardId() {
        return shardId;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public TNetworkAddress getAddress() {
        return address;
    }

    public String getIndexName() {
        return indexName;
    }
}
