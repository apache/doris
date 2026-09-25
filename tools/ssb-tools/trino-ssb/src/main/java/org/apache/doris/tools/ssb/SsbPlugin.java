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

package org.apache.doris.tools.ssb;

import io.trino.spi.Plugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public final class SsbPlugin implements Plugin {
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return List.of(new ConnectorFactory() {
            @Override
            public String getName() {
                return "ssb";
            }

            @Override
            public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
                if (!config.isEmpty()) {
                    throw new IllegalArgumentException("SSB has no connector properties: " + config.keySet());
                }
                try {
                    // Only the operator-installed binary next to this JAR is executable. Catalog
                    // properties cannot select a command, a path, or arbitrary dbgen arguments.
                    Path pluginJar = Path.of(SsbPlugin.class.getProtectionDomain()
                            .getCodeSource().getLocation().toURI());
                    return new SsbConnector(pluginJar.getParent().resolve("dbgen"));
                } catch (URISyntaxException e) {
                    throw new IllegalStateException("Cannot locate the SSB plugin directory", e);
                }
            }
        });
    }
}
