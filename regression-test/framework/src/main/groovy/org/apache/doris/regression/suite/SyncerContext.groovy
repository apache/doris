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

package org.apache.doris.regression.suite

import org.apache.doris.regression.Config
import org.apache.doris.regression.json.BinlogData
import org.apache.doris.regression.suite.client.BackendClientImpl
import org.apache.doris.regression.suite.client.FrontendClientImpl
import org.apache.doris.thrift.TTabletCommitInfo
import org.apache.doris.thrift.TGetSnapshotResult
import com.google.gson.annotations.SerializedName

import java.sql.Connection

class PartitionMeta {
    public long version
    public long indexId
    public TreeMap<Long, Long> tabletMeta

    PartitionMeta(long indexId, long version) {
        this.indexId = indexId
        this.version = version
        this.tabletMeta = new TreeMap<Long, Long>()
    }

    String toString() {
        return "PartitionMeta: { version: " + version.toString() + ", " + tabletMeta.toString() + " }"
    }
}

class TableMeta {
    public long id
    public TreeMap<Long, PartitionMeta> partitionMap = new TreeMap<>()

    TableMeta(long tableId) {
        this.id = tableId
    }

    String toString() {
        return "TableMeta: { id: " + id.toString() + ", " + partitionMap.toString() + " }"
    }
}

class ExtraInfo {
    class NetworkAddr {
        @SerializedName("ip")
        String ip
        @SerializedName("port")
        int port

        NetworkAddr(String ip, int port) {
            this.ip = ip
            this.port = port
        }

        String toString() {
            return String.format("Addr: { Ip: %s, port %d }", ip, port)
        }
    }

    @SerializedName("be_network_map")
    HashMap<Long, NetworkAddr> beNetworkMap
    @SerializedName("token")
    String token

    ExtraInfo(String token) {
        this.token = token
        this.beNetworkMap = new HashMap<>()
    }

    String toString() {
        return String.format("ExtraInfo: { token: %s, beNetwork:%s }", token, beNetworkMap.toString())
    }

    void addBackendNetaddr(Long beId, String ip, int port) {
        beNetworkMap.put(beId, new NetworkAddr(ip, port))
    }
}

class SyncerContext {
    protected Connection targetConnection
    protected FrontendClientImpl sourceFrontendClient
    protected FrontendClientImpl targetFrontendClient

    protected long sourceDbId
    protected HashMap<String, TableMeta> sourceTableMap = new HashMap<>()
    protected long targetDbId
    protected HashMap<String, TableMeta> targetTableMap = new HashMap<>()

    protected HashMap<Long, BackendClientImpl> sourceBackendClients = new HashMap<Long, BackendClientImpl>()
    protected HashMap<Long, BackendClientImpl> targetBackendClients = new HashMap<Long, BackendClientImpl>()

    public ArrayList<TTabletCommitInfo> commitInfos = new ArrayList<TTabletCommitInfo>()

    public BinlogData lastBinlog

    public String labelName
    public String tableName
    public TGetSnapshotResult getSnapshotResult
    public String token

    public Config config
    public String user
    public String passwd
    public String db
    public long txnId
    public long seq

    SyncerContext(String dbName, Config config) {
        this.sourceDbId = -1
        this.targetDbId = -1
        this.db = dbName
        this.config = config
        this.user = config.feSyncerUser
        this.passwd = config.feSyncerPassword
        this.seq = -1
    }

    ExtraInfo genExtraInfo() {
        ExtraInfo info = new ExtraInfo(token)
        sourceBackendClients.forEach((id, client) -> {
            info.addBackendNetaddr(id, client.address.hostname, client.httpPort)
        })
        return info
    }

    FrontendClientImpl getSourceFrontClient() {
        if (sourceFrontendClient == null) {
            sourceFrontendClient = new FrontendClientImpl(config.feSourceThriftNetworkAddress)
        }
        return sourceFrontendClient
    }

    FrontendClientImpl getTargetFrontClient() {
        if (targetFrontendClient == null) {
            targetFrontendClient = new FrontendClientImpl(config.feTargetThriftNetworkAddress)
        }
        return targetFrontendClient
    }

    Boolean metaIsValid() {
        if (sourceTableMap.isEmpty() && targetTableMap.isEmpty()) {
            return false
        } else if (sourceTableMap.size() != targetTableMap.size()) {
            return false
        }

        sourceTableMap.forEach((tableName, srcTableMeta) -> {
            TableMeta tarTableMeta = targetTableMap.get(tableName)
            if (tarTableMeta == null) {
                return false
            } else if (srcTableMeta.partitionMap.isEmpty() && tarTableMeta.partitionMap.isEmpty()) {
                return false
            } else if (srcTableMeta.partitionMap.size() != tarTableMeta.partitionMap.size()) {
                return false
            }

            Iterator srcPartitionIter = srcTableMeta.partitionMap.iterator()
            Iterator tarPartitionIter = tarTableMeta.partitionMap.iterator()
            while (srcPartitionIter.hasNext()) {
                Map srcTabletMeta = srcPartitionIter.next().value.tabletMeta
                Map tarTabletMeta = tarPartitionIter.next().value.tabletMeta

                if (srcTabletMeta.isEmpty() && tarTabletMeta.isEmpty()) {
                    return false
                } else if (srcTabletMeta.size() != tarTabletMeta.size()) {
                    return false
                }
            }
        })

        return true
    }

    void closeBackendClients() {
        if (!sourceBackendClients.isEmpty()) {
            for (BackendClientImpl client in sourceBackendClients.values()) {
                client.close()
            }
        }
        sourceBackendClients.clear()
        if (!targetBackendClients.isEmpty()) {
            for (BackendClientImpl client in targetBackendClients.values()) {
                client.close()
            }
        }
        targetBackendClients.clear()
    }

    void closeAllClients() {
        if (sourceFrontendClient != null) {
            sourceFrontendClient.close()
        }
        if (targetFrontendClient != null) {
            targetFrontendClient.close()
        }
    }

    void closeConn() {
        targetConnection.close()
    }
}
