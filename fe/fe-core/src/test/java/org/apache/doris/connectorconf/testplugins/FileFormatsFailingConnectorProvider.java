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

package org.apache.doris.connectorconf.testplugins;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import java.util.Map;
import java.util.Set;

/**
 * A connector provider whose {@link #getFileFormats()} throws, as a plugin with a bug there would. The engine reads
 * the formats when it admits the plugin, so the plugin's class bytes are copied into a temporary jar and loaded the
 * way FE loads a shipped connector.
 *
 * <p>It lives outside {@code org.apache.doris.connector.} for the reason {@link ConfProbeConnectorProviderA} does.
 */
public class FileFormatsFailingConnectorProvider implements ConnectorProvider {

    public static final String TYPE = "file_formats_failing";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public Connector create(Map<String, String> properties, ConnectorContext context) {
        throw new UnsupportedOperationException(TYPE + " exists to be admitted or skipped at load time");
    }

    @Override
    public Set<String> getFileFormats() {
        throw new IllegalStateException("the connector could not list its file formats");
    }
}
