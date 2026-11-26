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

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;

import java.util.HashMap;
import java.util.Map;

/**
 * Where the temporary probe plugins of {@link ConnectorPluginConfTest} hand back what the engine gave them.
 *
 * <p>It sits in {@code org.apache.doris.connector} deliberately. That prefix is parent-first for the
 * CONNECTOR family, so the plugin's child-first classloader delegates this class to the FE's own loader and
 * both sides share one Class — and therefore one static map. A probe provider cannot report through its own
 * types: those really are child-loaded (which is the point — the loader reads a plugin's declared API
 * version from the jar that defines its factory class), so anything it returns is uncastable on this side.
 *
 * <p>The {@link Connector} it hands back is likewise defined here rather than in the plugin jar, so the jar
 * needs to carry nothing but the provider itself.
 */
public final class ConfProbeSink {

    private static final Map<String, Map<String, String>> SEEN = new HashMap<>();

    private ConfProbeSink() {
    }

    /** Records the connector config a provider was handed, and gives it a connector to return. */
    public static Connector record(String type, Map<String, String> connectorConfig) {
        SEEN.put(type, new HashMap<>(connectorConfig));
        return new ProbeConnector();
    }

    /** What the provider of {@code type} was handed, or null if it was never asked to create anything. */
    public static Map<String, String> seen(String type) {
        return SEEN.get(type);
    }

    public static void reset() {
        SEEN.clear();
    }

    private static final class ProbeConnector implements Connector {
        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return null;
        }

        @Override
        public void close() {
        }
    }
}
