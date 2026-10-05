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

import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ForwardingConnectorContext;

import java.util.Map;
import java.util.Objects;

/**
 * A {@link ConnectorContext} carrying one plugin's own {@code <name>.conf}.
 *
 * <p>{@link ConnectorPluginManager#createConnector} wraps the engine context in this just before calling
 * {@code ConnectorProvider.create}, because that is the first moment the engine knows which plugin the
 * catalog type resolved to. Everything else forwards, so this class holds no engine behavior of its own —
 * see {@link ForwardingConnectorContext} for why that base class rather than hand-written pass-throughs.
 */
final class ConnectorConfigContext extends ForwardingConnectorContext {

    private final Map<String, String> conf;

    ConnectorConfigContext(ConnectorContext delegate, Map<String, String> conf) {
        super(delegate);
        this.conf = Objects.requireNonNull(conf, "conf");
    }

    @Override
    public Map<String, String> getConnectorConfig() {
        return conf;
    }
}
