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

package org.apache.doris.connector;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.JdbcResource;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.connector.api.ConnectorValidationContext;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.proto.InternalService.PJdbcTestConnectionRequest;
import org.apache.doris.proto.InternalService.PJdbcTestConnectionResult;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import com.google.protobuf.ByteString;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Engine-side implementation of {@link ConnectorValidationContext}.
 *
 * <p>Provides driver validation (via {@link JdbcResource}), checksum computation,
 * and deferred BE→external connectivity testing (via BRPC) as infrastructure
 * services that connectors can call during pre-creation validation.</p>
 *
 * <p>Connectors register a BE connectivity test via {@link #requestBeConnectivityTest};
 * the engine calls {@link #executePendingBeTests()} after validation to send
 * the BRPC request to an alive backend.</p>
 */
public class DefaultConnectorValidationContext implements ConnectorValidationContext {

    private final long catalogId;
    private final CatalogProperty catalogProperty;

    // Pending BE connectivity test, populated by connector during preCreateValidation().
    private byte[] pendingBeTestPayload;
    private int pendingBeTestConnectionType;
    private String pendingBeTestQuery;

    public DefaultConnectorValidationContext(long catalogId, CatalogProperty catalogProperty) {
        this.catalogId = catalogId;
        this.catalogProperty = catalogProperty;
    }

    @Override
    public long getCatalogId() {
        return catalogId;
    }

    @Override
    public String getProperty(String key) {
        return catalogProperty.getOrDefault(key, null);
    }

    @Override
    public void storeProperty(String key, String value) {
        catalogProperty.addProperty(key, value);
    }

    @Override
    public String validateAndResolveDriverPath(String driverUrl) throws Exception {
        return JdbcResource.getFullDriverUrl(driverUrl);
    }

    @Override
    public String computeDriverChecksum(String driverUrl) throws Exception {
        return JdbcResource.computeObjectChecksum(driverUrl);
    }

    @Override
    public void requestBeConnectivityTest(byte[] serializedDescriptor, int connectionTypeValue,
            String testQuery) {
        this.pendingBeTestPayload = serializedDescriptor;
        this.pendingBeTestConnectionType = connectionTypeValue;
        this.pendingBeTestQuery = testQuery;
    }

    /**
     * Executes the pending BE connectivity test, if any was registered by
     * the connector during {@code preCreateValidation()}.
     *
     * <p>This is an engine-only method (not on the SPI interface). It finds
     * an alive backend and sends a BRPC test-connection request.</p>
     */
    public void executePendingBeTests() throws DdlException {
        if (pendingBeTestPayload == null) {
            return;
        }
        if (FeConstants.runningUnitTest) {
            return;
        }
        Backend aliveBe = findAliveBackend();
        TNetworkAddress address = new TNetworkAddress(aliveBe.getHost(), aliveBe.getBrpcPort());
        try {
            PJdbcTestConnectionRequest request = PJdbcTestConnectionRequest.newBuilder()
                    .setJdbcTable(ByteString.copyFrom(pendingBeTestPayload))
                    .setJdbcTableType(pendingBeTestConnectionType)
                    .setQueryStr(pendingBeTestQuery)
                    .build();
            Future<PJdbcTestConnectionResult> future = BackendServiceProxy.getInstance()
                    .testJdbcConnection(address, request);
            PJdbcTestConnectionResult result = future.get();
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                throw new DdlException("BE connectivity test failed: "
                        + result.getStatus().getErrorMsgs(0));
            }
        } catch (RpcException | ExecutionException | InterruptedException e) {
            throw new DdlException("BE connectivity test failed: " + e.getMessage(), e);
        }
    }

    private static Backend findAliveBackend() throws DdlException {
        try {
            for (Backend be : Env.getCurrentSystemInfo().getAllBackendsByAllCluster().values()) {
                if (be.isAlive()) {
                    return be;
                }
            }
        } catch (Exception e) {
            throw new DdlException("Failed to find alive backend: " + e.getMessage(), e);
        }
        throw new DdlException("BE connectivity test failed: No alive backends");
    }
}
