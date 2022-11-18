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

package org.apache.doris.qe;

import org.apache.doris.common.ClientPool;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TInitExternalCtlMetaRequest;
import org.apache.doris.thrift.TInitExternalCtlMetaResult;
import org.apache.doris.thrift.TNetworkAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The client for Observer FE to forward external datasource object init request to master.
 * Including init ExternalCatalog, ExternalDatabase and ExternalTable.
 * This client will wait for the journal ID replayed at this Observer FE before return.
 */
public class MasterCatalogExecutor {

    private static final Logger LOG = LogManager.getLogger(MasterCatalogExecutor.class);

    private final ConnectContext ctx;
    private int waitTimeoutMs;

    public MasterCatalogExecutor() {
        ctx = ConnectContext.get();
        waitTimeoutMs = ctx.getSessionVariable().getQueryTimeoutS() * 1000;
    }

    public void forward(long catalogId, long dbId) throws Exception {
        if (!ctx.getEnv().isReady()) {
            throw new Exception("Current catalog is not ready, please wait for a while.");
        }
        String masterHost = ctx.getEnv().getMasterIp();
        int masterRpcPort = ctx.getEnv().getMasterRpcPort();
        TNetworkAddress thriftAddress = new TNetworkAddress(masterHost, masterRpcPort);

        FrontendService.Client client = null;
        try {
            client = ClientPool.frontendPool.borrowObject(thriftAddress, waitTimeoutMs);
        } catch (Exception e) {
            throw new Exception("Failed to get master client.", e);
        }
        TInitExternalCtlMetaRequest request = new TInitExternalCtlMetaRequest();
        request.setCatalogId(catalogId);
        if (dbId != -1) {
            request.setDbId(dbId);
        }
        boolean isReturnToPool = false;
        try {
            TInitExternalCtlMetaResult result = client.initExternalCtlMeta(request);
            ConnectContext.get().getEnv().getJournalObservable().waitOn(result.maxJournalId, waitTimeoutMs);
            isReturnToPool = true;
        } catch (Exception e) {
            LOG.warn("Failed to finish forward init operation, please try again. ", e);
            throw e;
        } finally {
            if (isReturnToPool) {
                ClientPool.frontendPool.returnObject(thriftAddress, client);
            } else {
                ClientPool.frontendPool.invalidateObject(thriftAddress, client);
            }
        }
    }
}
