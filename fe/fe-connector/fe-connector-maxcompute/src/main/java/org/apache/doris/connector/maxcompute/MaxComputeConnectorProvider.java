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
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * SPI entry point for the MaxCompute (ODPS) connector plugin.
 */
public class MaxComputeConnectorProvider implements ConnectorProvider {

    private static final List<String> REQUIRED_PROPERTIES = Arrays.asList(
            MCConnectorProperties.PROJECT,
            MCConnectorProperties.ENDPOINT);

    private static final long MIN_SPLIT_BYTE_SIZE = 10485760L;

    @Override
    public String getType() {
        return "max_compute";
    }

    @Override
    public Connector create(Map<String, String> properties,
            ConnectorContext context) {
        return new MaxComputeDorisConnector(properties, context);
    }

    /**
     * Validates catalog properties at CREATE CATALOG time, mirroring the fail-fast
     * checks of the legacy {@code MaxComputeExternalCatalog.checkProperties}: required
     * PROJECT/ENDPOINT, split strategy + size floor, account_format enum, positive
     * connect/read timeout and retry count, and authentication completeness. Throws
     * {@link IllegalArgumentException}, which the caller
     * ({@code PluginDrivenExternalCatalog.checkProperties}) wraps into a DdlException.
     */
    @Override
    public void validateProperties(Map<String, String> properties) {
        // 1. Required properties: PROJECT + ENDPOINT (literal keys, mirroring legacy
        // REQUIRED_PROPERTIES; region/odps_endpoint/tunnel_endpoint are replay-only
        // backward-compat fallbacks, not valid for a new CREATE).
        for (String required : REQUIRED_PROPERTIES) {
            if (!properties.containsKey(required)) {
                throw new IllegalArgumentException(
                        "Required property '" + required + "' is missing");
            }
        }

        // 2. Split strategy and size/count floor.
        String splitStrategy = properties.getOrDefault(
                MCConnectorProperties.SPLIT_STRATEGY,
                MCConnectorProperties.DEFAULT_SPLIT_STRATEGY);
        try {
            if (splitStrategy.equals(
                    MCConnectorProperties.SPLIT_BY_BYTE_SIZE_STRATEGY)) {
                long splitByteSize = Long.parseLong(properties.getOrDefault(
                        MCConnectorProperties.SPLIT_BYTE_SIZE,
                        MCConnectorProperties.DEFAULT_SPLIT_BYTE_SIZE));
                if (splitByteSize < MIN_SPLIT_BYTE_SIZE) {
                    throw new IllegalArgumentException(
                            MCConnectorProperties.SPLIT_BYTE_SIZE
                                    + " must be greater than or equal to "
                                    + MIN_SPLIT_BYTE_SIZE);
                }
            } else if (splitStrategy.equals(
                    MCConnectorProperties.SPLIT_BY_ROW_COUNT_STRATEGY)) {
                long splitRowCount = Long.parseLong(properties.getOrDefault(
                        MCConnectorProperties.SPLIT_ROW_COUNT,
                        MCConnectorProperties.DEFAULT_SPLIT_ROW_COUNT));
                if (splitRowCount <= 0) {
                    throw new IllegalArgumentException(
                            MCConnectorProperties.SPLIT_ROW_COUNT
                                    + " must be greater than 0");
                }
            } else {
                throw new IllegalArgumentException(
                        "property " + MCConnectorProperties.SPLIT_STRATEGY
                                + " must be "
                                + MCConnectorProperties.SPLIT_BY_BYTE_SIZE_STRATEGY
                                + " or "
                                + MCConnectorProperties.SPLIT_BY_ROW_COUNT_STRATEGY);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "property " + MCConnectorProperties.SPLIT_BYTE_SIZE + "/"
                            + MCConnectorProperties.SPLIT_ROW_COUNT
                            + " must be an integer");
        }

        // 3. Account format enum: name | id.
        String accountFormat = properties.getOrDefault(
                MCConnectorProperties.ACCOUNT_FORMAT,
                MCConnectorProperties.DEFAULT_ACCOUNT_FORMAT);
        if (!accountFormat.equals(MCConnectorProperties.ACCOUNT_FORMAT_NAME)
                && !accountFormat.equals(
                        MCConnectorProperties.ACCOUNT_FORMAT_ID)) {
            throw new IllegalArgumentException(
                    "property " + MCConnectorProperties.ACCOUNT_FORMAT
                            + " only support name and id");
        }

        // 4. Positive connect/read timeout and retry count.
        checkPositiveInt(properties, MCConnectorProperties.CONNECT_TIMEOUT,
                MCConnectorProperties.DEFAULT_CONNECT_TIMEOUT);
        checkPositiveInt(properties, MCConnectorProperties.READ_TIMEOUT,
                MCConnectorProperties.DEFAULT_READ_TIMEOUT);
        checkPositiveInt(properties, MCConnectorProperties.RETRY_COUNT,
                MCConnectorProperties.DEFAULT_RETRY_COUNT);

        // 5. Authentication completeness (wires the otherwise-unused
        // MCConnectorClientFactory.checkAuthProperties).
        MCConnectorClientFactory.checkAuthProperties(properties);
    }

    private static void checkPositiveInt(Map<String, String> properties,
            String key, String defaultValue) {
        int value;
        try {
            value = Integer.parseInt(properties.getOrDefault(key, defaultValue));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "property " + key + " must be an integer");
        }
        if (value <= 0) {
            throw new IllegalArgumentException(
                    key + " must be greater than 0");
        }
    }
}
