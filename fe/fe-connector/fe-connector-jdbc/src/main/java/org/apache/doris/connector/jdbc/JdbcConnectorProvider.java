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

package org.apache.doris.connector.jdbc;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import java.util.Map;

/**
 * SPI entry point for the JDBC connector plugin.
 */
public class JdbcConnectorProvider implements ConnectorProvider {

    @Override
    public String getType() {
        return "jdbc";
    }

    @Override
    public Connector create(Map<String, String> properties, ConnectorContext context) {
        return new JdbcDorisConnector(properties, context);
    }

    /**
     * Validates catalog properties at CREATE/ALTER time. Building the holder covers what a stored
     * catalog must satisfy; checkCreateTimeOnlyRules covers the rest, which has only ever applied to a
     * statement (see {@link JdbcCatalogProperties}). The driver_url security rule stays a separate call
     * because it is enforced here and in preCreateValidation, and lives with the connector that owns it.
     */
    @Override
    public void validateProperties(Map<String, String> properties) {
        JdbcCatalogProperties props = JdbcCatalogProperties.of(properties).checkCreateTimeOnlyRules();
        JdbcDorisConnector.checkDriverUrlSecurityRule(props.getDriverUrl());
    }
}
