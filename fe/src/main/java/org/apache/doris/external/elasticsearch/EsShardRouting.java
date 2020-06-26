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


import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;

import org.apache.doris.thrift.TNetworkAddress;

public class EsShardRouting {

    private final String indexName;
    private final int shardId;
    private final boolean isPrimary;
    private final TNetworkAddress address;

    private TNetworkAddress httpAddress;
    private final String nodeId;
    
    public EsShardRouting(String indexName, int shardId, boolean isPrimary, TNetworkAddress address, String nodeId) {
        this.indexName = indexName;
        this.shardId = shardId;
        this.isPrimary = isPrimary;
        this.address = address;
        this.nodeId = nodeId;
    }
    
    public static EsShardRouting parseShardRouting(String indexName, String shardKey,
            JSONObject shardInfo, JSONObject nodesMap) {
        String nodeId = shardInfo.getString("node");
        JSONObject nodeInfo = nodesMap.getJSONObject(nodeId);
        String[] transportAddr = nodeInfo.getString("transport_address").split(":");
        // get thrift port from node info
        String thriftPort = nodeInfo.getJSONObject("attributes").optString("thrift_port");
        // In http transport mode, should ignore thrift_port, set address to null
        TNetworkAddress addr = null;
        if (!StringUtils.isEmpty(thriftPort)) {
            addr = new TNetworkAddress(transportAddr[0], Integer.parseInt(thriftPort));
        }
        boolean isPrimary = shardInfo.getBoolean("primary");
        return new EsShardRouting(indexName, Integer.parseInt(shardKey),
                isPrimary, addr, nodeId);
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

    public TNetworkAddress getHttpAddress() {
        return httpAddress;
    }

    public void setHttpAddress(TNetworkAddress httpAddress) {
        this.httpAddress = httpAddress;
    }

    public String getNodeId() {
        return nodeId;
    }

    @Override
    public String toString() {
        return "EsShardRouting{" +
                "indexName='" + indexName + '\'' +
                ", shardId=" + shardId +
                ", isPrimary=" + isPrimary +
                ", address=" + address +
                ", httpAddress=" + httpAddress +
                ", nodeId='" + nodeId + '\'' +
                '}';
    }
}
