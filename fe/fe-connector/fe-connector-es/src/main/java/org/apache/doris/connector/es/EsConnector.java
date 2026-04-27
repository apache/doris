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

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTestResult;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * Elasticsearch connector implementation.
 * Created once per catalog lifecycle.
 */
public class EsConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(EsConnector.class);

    private final Map<String, String> properties;
    private final ConnectorContext context;
    private volatile EsConnectorRestClient restClient;

    public EsConnector(Map<String, String> properties, ConnectorContext context) {
        this.properties = EsConnectorProperties.processCompatible(properties);
        this.context = context;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new EsConnectorMetadata(getOrCreateRestClient(), properties);
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        return new EsScanPlanProvider(getOrCreateRestClient(), properties);
    }

    @Override
    public ConnectorTestResult testConnection(ConnectorSession session) {
        try {
            EsConnectorRestClient client = getOrCreateRestClient();
            client.listTable(false);
            return ConnectorTestResult.success();
        } catch (Exception e) {
            LOG.warn("ES connectivity test failed", e);
            return ConnectorTestResult.failure("ES connectivity test failed: " + e.getMessage());
        }
    }

    @Override
    public void close() throws IOException {
        // OkHttp clients are shared statics; nothing to close
    }

    @Override
    public String executeRestRequest(String path, String body) {
        return getOrCreateRestClient().executePassthrough(path, body);
    }

    private EsConnectorRestClient getOrCreateRestClient() {
        if (restClient == null) {
            synchronized (this) {
                if (restClient == null) {
                    String hosts = properties.getOrDefault(
                            EsConnectorProperties.HOSTS, "");
                    String sslEnabled = properties.getOrDefault(
                            EsConnectorProperties.HTTP_SSL_ENABLED,
                            EsConnectorProperties.HTTP_SSL_ENABLED_DEFAULT);
                    String[] hostUrls = hosts.trim().split(",");
                    EsConnectorProperties.fillUrlsWithSchema(
                            hostUrls, Boolean.parseBoolean(sslEnabled));

                    String user = properties.getOrDefault(
                            EsConnectorProperties.USER, "");
                    String password = properties.getOrDefault(
                            EsConnectorProperties.PASSWORD, "");
                    boolean ssl = Boolean.parseBoolean(sslEnabled);
                    restClient = new EsConnectorRestClient(
                            hostUrls, user, password, ssl,
                            context.getHttpSecurityHook());
                }
            }
        }
        return restClient;
    }
}
