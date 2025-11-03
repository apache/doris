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

package org.apache.doris.datasource.connectivity;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.ClientPool;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageBackendType;
import org.apache.doris.thrift.TTestStorageConnectivityRequest;
import org.apache.doris.thrift.TTestStorageConnectivityResponse;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface StorageConnectivityTester {

    default void testFeConnection() throws Exception {
        // Default: test passes (no-op)
    }

    default TStorageBackendType getStorageType() {
        return null;
    }

    default Map<String, String> getBackendProperties() {
        return Collections.emptyMap();
    }

    /**
     * Returns the type of storage connectivity test for error messages.
     * Default implementation returns "Storage" for generic storage connectivity test.
     */
    default String getTestType() {
        return "Storage";
    }

    default void testBeConnection() throws Exception {
        List<Long> aliveBeIds = Env.getCurrentSystemInfo().getAllBackendIds(true);
        if (aliveBeIds.isEmpty()) {
            // no alive BE, skip the test
            return;
        }

        Collections.shuffle(aliveBeIds);
        testBeConnectionInternal(aliveBeIds.get(0));
    }

    default void testBeConnectionInternal(long backendId) throws Exception {
        Backend backend = Env.getCurrentSystemInfo().getBackend(backendId);
        if (backend == null) {
            // backend not found, skip the test
            return;
        }

        TTestStorageConnectivityRequest request = new TTestStorageConnectivityRequest();
        request.setType(getStorageType());
        request.setProperties(getBackendProperties());

        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBePort());
        BackendService.Client client = null;
        boolean ok = false;
        try {
            client = ClientPool.backendPool.borrowObject(address);
            TTestStorageConnectivityResponse response = client.testStorageConnectivity(request);

            if (response.status.getStatusCode() != TStatusCode.OK) {
                String errMsg = response.status.isSetErrorMsgs() && !response.status.getErrorMsgs().isEmpty()
                        ? response.status.getErrorMsgs().get(0) : "Unknown error";
                throw new Exception(errMsg);
            }
            ok = true;
        } finally {
            if (ok) {
                ClientPool.backendPool.returnObject(address, client);
            } else {
                ClientPool.backendPool.invalidateObject(address, client);
            }
        }
    }
}
