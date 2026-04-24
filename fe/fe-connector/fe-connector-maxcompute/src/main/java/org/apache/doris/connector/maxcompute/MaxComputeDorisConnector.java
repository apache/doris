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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTestResult;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.ConnectorContext;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AccountFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * Main Connector implementation for MaxCompute (ODPS).
 * Manages the Odps client lifecycle and provides metadata access.
 *
 * <p>Note: EnvironmentSettings and SplitOptions (from odps-sdk-table-api)
 * are managed by {@link MaxComputeScanPlanProvider} which handles scan planning.
 */
public class MaxComputeDorisConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(
            MaxComputeDorisConnector.class);

    private final Map<String, String> properties;
    private final ConnectorContext context;

    private Odps odps;
    private String endpoint;
    private String defaultProject;
    private String quota;
    private McStructureHelper structureHelper;
    private MaxComputeScanPlanProvider scanPlanProvider;

    private volatile boolean initialized;

    public MaxComputeDorisConnector(Map<String, String> properties,
            ConnectorContext context) {
        this.properties = properties;
        this.context = context;
    }

    private void ensureInitialized() {
        if (!initialized) {
            synchronized (this) {
                if (!initialized) {
                    doInit();
                    initialized = true;
                }
            }
        }
    }

    private void doInit() {
        endpoint = MCConnectorEndpoint.resolveEndpoint(properties);

        defaultProject = properties.get(MCConnectorProperties.PROJECT);
        quota = properties.getOrDefault(
                MCConnectorProperties.QUOTA,
                MCConnectorProperties.DEFAULT_QUOTA);

        odps = MCConnectorClientFactory.createClient(properties);
        odps.setDefaultProject(defaultProject);
        odps.setEndpoint(endpoint);

        String accountFormatProp = properties.getOrDefault(
                MCConnectorProperties.ACCOUNT_FORMAT,
                MCConnectorProperties.DEFAULT_ACCOUNT_FORMAT);
        AccountFormat accountFormat;
        if (accountFormatProp.equals(
                MCConnectorProperties.ACCOUNT_FORMAT_ID)) {
            accountFormat = AccountFormat.ID;
        } else {
            accountFormat = AccountFormat.DISPLAYNAME;
        }
        odps.setAccountFormat(accountFormat);

        boolean enableNamespaceSchema = Boolean.parseBoolean(
                properties.getOrDefault(
                        MCConnectorProperties.ENABLE_NAMESPACE_SCHEMA,
                        MCConnectorProperties
                                .DEFAULT_ENABLE_NAMESPACE_SCHEMA));
        structureHelper = McStructureHelper.getHelper(
                enableNamespaceSchema, defaultProject);
        scanPlanProvider = new MaxComputeScanPlanProvider(this);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        ensureInitialized();
        return new MaxComputeConnectorMetadata(
                odps, structureHelper, defaultProject);
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        ensureInitialized();
        return scanPlanProvider;
    }

    @Override
    public ConnectorTestResult testConnection(ConnectorSession session) {
        try {
            ensureInitialized();
            odps.projects().exists(defaultProject);
            return ConnectorTestResult.success(
                    "MaxCompute project '" + defaultProject + "' is accessible");
        } catch (Exception e) {
            return ConnectorTestResult.failure(
                    "MaxCompute connection test failed: " + e.getMessage());
        }
    }

    public Odps getClient() {
        ensureInitialized();
        return odps;
    }

    public String getEndpoint() {
        ensureInitialized();
        return endpoint;
    }

    public String getDefaultProject() {
        ensureInitialized();
        return defaultProject;
    }

    public String getQuota() {
        ensureInitialized();
        return quota;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public McStructureHelper getStructureHelper() {
        ensureInitialized();
        return structureHelper;
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing MaxCompute connector for project: {}",
                defaultProject);
    }
}
