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

/**
 * Represents one shard routing entry from ES _search_shards API.
 * Adapted from fe-core's EsShardRouting — uses plain host+port instead of TNetworkAddress.
 */
public class EsShardRouting {

    private final String indexName;
    private final int shardId;
    private final boolean isPrimary;
    private final String nodeId;
    private String httpHost;
    private int httpPort;

    public EsShardRouting(String indexName, int shardId, boolean isPrimary, String nodeId) {
        this.indexName = indexName;
        this.shardId = shardId;
        this.isPrimary = isPrimary;
        this.nodeId = nodeId;
    }

    public int getShardId() {
        return shardId;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getHttpHost() {
        return httpHost;
    }

    public int getHttpPort() {
        return httpPort;
    }

    /** Returns "host:port" string for convenience. */
    public String getHttpAddress() {
        return httpHost + ":" + httpPort;
    }

    public void setHttpAddress(String host, int port) {
        this.httpHost = host;
        this.httpPort = port;
    }

    public String getNodeId() {
        return nodeId;
    }

    @Override
    public String toString() {
        return "EsShardRouting{"
                + "indexName='" + indexName + '\''
                + ", shardId=" + shardId
                + ", isPrimary=" + isPrimary
                + ", httpAddress=" + httpHost + ":" + httpPort
                + ", nodeId='" + nodeId + '\''
                + '}';
    }
}
