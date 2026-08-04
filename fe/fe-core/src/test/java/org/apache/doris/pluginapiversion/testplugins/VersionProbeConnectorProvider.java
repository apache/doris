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

package org.apache.doris.pluginapiversion.testplugins;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import java.util.Map;

/**
 * A connector provider whose class bytes are copied into a temporary plugin jar, so that a test can load it
 * exactly the way FE loads a shipped connector.
 *
 * <p>It lives in {@code org.apache.doris.pluginapiversion.testplugins} on purpose. Every package the
 * CONNECTOR family declares parent-first — {@code org.apache.doris.connector.},
 * {@code org.apache.doris.filesystem.} and the mandatory defaults — would be loaded from the FE's own
 * classpath instead of from the plugin jar, and the loader reads the declared API version from the jar that
 * <em>defines</em> the factory class. A parent-first test provider would therefore always look undeclared and
 * every such test would pass for the wrong reason.
 */
public class VersionProbeConnectorProvider implements ConnectorProvider {

    @Override
    public String getType() {
        return "version_probe";
    }

    @Override
    public Connector create(Map<String, String> properties, ConnectorContext context) {
        throw new UnsupportedOperationException(
                "version_probe exists to be admitted or refused at load time; it never backs a catalog");
    }
}
