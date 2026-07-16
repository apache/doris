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

package org.apache.doris.catalog.authorizer.openfga;

import com.google.common.annotations.VisibleForTesting;
import dev.openfga.sdk.api.client.OpenFgaClient;
import dev.openfga.sdk.api.client.model.ClientCheckRequest;
import dev.openfga.sdk.api.client.model.ClientCheckResponse;
import dev.openfga.sdk.api.configuration.ApiToken;
import dev.openfga.sdk.api.configuration.ClientConfiguration;
import dev.openfga.sdk.api.configuration.Credentials;
import dev.openfga.sdk.errors.FgaInvalidParameterException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Thin wrapper over the OpenFGA Java SDK ({@code dev.openfga:openfga-sdk}) that turns an
 * {@link OpenFgaCheckRequest} into a single OpenFGA Check call.
 *
 * <p>The wrapper is built once per JVM by {@link OpenFgaDorisAccessControllerFactory} so there is a
 * single OpenFGA client (and its HTTP connection pool) for the whole process, matching the
 * one-refresher-per-JVM lesson from the Ranger controller.
 *
 * <p>It fails closed: if the OpenFGA service is unreachable, times out, or returns an error, the
 * check returns {@code false} (deny). A security control must never grant access because the policy
 * service was unavailable.
 */
public class OpenFgaClientWrapper implements OpenFgaChecker {
    private static final Logger LOG = LogManager.getLogger(OpenFgaClientWrapper.class);

    private final OpenFgaClient client;
    private final long readTimeoutMillis;

    public OpenFgaClientWrapper(OpenFgaConfig config) {
        this(buildClient(config), config.getReadTimeoutMillis());
    }

    @VisibleForTesting
    OpenFgaClientWrapper(OpenFgaClient client, long readTimeoutMillis) {
        this.client = client;
        this.readTimeoutMillis = readTimeoutMillis;
    }

    private static OpenFgaClient buildClient(OpenFgaConfig config) {
        // Setters are called as statements rather than chained so this does not depend on whether a
        // given setter returns Configuration or the ClientConfiguration subtype.
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.apiUrl(config.getApiUrl());
        clientConfiguration.storeId(config.getStoreId());
        clientConfiguration.connectTimeout(Duration.ofMillis(config.getConnectTimeoutMillis()));
        clientConfiguration.readTimeout(Duration.ofMillis(config.getReadTimeoutMillis()));
        if (StringUtils.isNotEmpty(config.getModelId())) {
            clientConfiguration.authorizationModelId(config.getModelId());
        }
        if (StringUtils.isNotEmpty(config.getToken())) {
            clientConfiguration.credentials(new Credentials(new ApiToken(config.getToken())));
        }
        try {
            return new OpenFgaClient(clientConfiguration);
        } catch (FgaInvalidParameterException e) {
            // Misconfiguration is fatal on purpose: better to refuse to start than to run with a
            // controller that can never reach OpenFGA and would deny every request.
            throw new IllegalStateException("failed to initialize the OpenFGA client, check "
                    + OpenFgaConfig.API_URL + " and " + OpenFgaConfig.STORE_ID, e);
        }
    }

    @Override
    public boolean check(OpenFgaCheckRequest request) {
        if (request.getRelation() == null || request.getObject() == null) {
            return false;
        }
        try {
            ClientCheckRequest body = new ClientCheckRequest()
                    .user(request.getUser())
                    .relation(request.getRelation())
                    ._object(request.getObject());
            ClientCheckResponse response = client.check(body).get(readTimeoutMillis, TimeUnit.MILLISECONDS);
            boolean allowed = response != null && Boolean.TRUE.equals(response.getAllowed());
            if (LOG.isDebugEnabled()) {
                LOG.debug("openfga check {} -> {}", request, allowed);
            }
            return allowed;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("openfga check interrupted, denying access for {}", request, e);
            return false;
        } catch (Exception e) {
            // Fail closed: any transport, timeout, or protocol error denies access.
            LOG.warn("openfga check failed, denying access for {}", request, e);
            return false;
        }
    }
}
