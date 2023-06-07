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
import org.apache.doris.regression.suite.client.BackendClientImpl
import org.apache.doris.regression.suite.client.FrontendClientImpl
import org.apache.doris.thrift.TTabletCommitInfo

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

class SyncerContext {
    protected Connection targetConnection
    protected FrontendClientImpl sourceFrontendClient
    protected FrontendClientImpl targetFrontendClient

    protected Long sourceDbId
    protected HashMap<String, Long> sourceTableMap = new HashMap<String, Long>()
    protected TreeMap<Long, PartitionMeta> sourcePartitionMap = new TreeMap<Long, PartitionMeta>()
    protected Long targetDbId
    protected HashMap<String, Long> targetTableMap = new HashMap<String, Long>()
    protected TreeMap<Long, PartitionMeta> targetPartitionMap = new TreeMap<Long, PartitionMeta>()

    protected HashMap<Long, BackendClientImpl> sourceBackendClients = new HashMap<Long, BackendClientImpl>()
    protected HashMap<Long, BackendClientImpl> targetBackendClients = new HashMap<Long, BackendClientImpl>()

    public ArrayList<TTabletCommitInfo> commitInfos = new ArrayList<TTabletCommitInfo>()

    public Config config
    public String user
    public String passwd
    public String db
    public long txnId
    public long seq

    SyncerContext(String dbName, Config config) {
        this.db = dbName
        this.config = config
        this.user = config.feSyncerUser
        this.passwd = config.feSyncerPassword
        this.seq = -1
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
        if (sourcePartitionMap.isEmpty() || targetPartitionMap.isEmpty()) {
            return false
        }

        if (sourcePartitionMap.size() != targetPartitionMap.size()) {
            return false
        }

        Iterator sourceIter = sourcePartitionMap.iterator()
        Iterator targetIter = targetPartitionMap.iterator()
        while (sourceIter.hasNext()) {
            if (sourceIter.next().value.tabletMeta.size() !=
                    targetIter.next().value.tabletMeta.size()) {
                return false
            }
        }

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
}
