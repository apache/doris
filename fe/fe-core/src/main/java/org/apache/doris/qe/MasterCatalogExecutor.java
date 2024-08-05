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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.UserException;
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

    public static final String STATUS_OK = "OK";

    private int waitTimeoutMs;

    public MasterCatalogExecutor(int waitTimeoutMs) {
        this.waitTimeoutMs = waitTimeoutMs;
    }

    public void forward(long catalogId, long dbId) throws Exception {
        Env.getCurrentEnv().checkReadyOrThrow();
        String masterHost = Env.getCurrentEnv().getMasterHost();
        int masterRpcPort = Env.getCurrentEnv().getMasterRpcPort();
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
            if (!result.getStatus().equalsIgnoreCase(STATUS_OK)) {
                throw new UserException(result.getStatus());
            } else {
                // DO NOT wait on journal replayed, this may cause deadlock.
                // 1. hold table read lock
                // 2. wait on journal replayed
                // 3. previous journal (eg, txn journal) replayed need to hold table write lock
                // 4. deadlock
                // But no waiting on journal replayed may cause some request on non-master FE failed for some time.
                // There is no good solution for this.
                // In feature version, this whole process is refactored, so we temporarily remove this waiting.
                // Env.getCurrentEnv().getJournalObservable().waitOn(result.maxJournalId, timeoutMs);
                isReturnToPool = true;
            }
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
