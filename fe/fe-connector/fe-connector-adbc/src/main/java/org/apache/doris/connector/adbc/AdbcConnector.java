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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;

import java.io.IOException;
import java.util.Map;

/**
 * ADBC connector implementation. Created once per catalog lifecycle.
 */
public class AdbcConnector implements Connector {

    private final Map<String, String> properties;
    private final ConnectorContext context;

    public AdbcConnector(Map<String, String> properties, ConnectorContext context) {
        this.properties = properties;
        this.context = context;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        throw new DorisConnectorException("ADBC catalog metadata is not wired up yet");
    }

    @Override
    public void close() throws IOException {
        // Nothing held yet.
    }
}
