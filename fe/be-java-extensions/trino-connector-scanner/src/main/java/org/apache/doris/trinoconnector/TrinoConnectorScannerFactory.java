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

package org.apache.doris.trinoconnector;

import org.apache.doris.jni.spi.JniScanner;
import org.apache.doris.jni.spi.JniScannerFactory;

import java.util.Map;

/** Builds {@link TrinoConnectorJniScanner}. BE reaches it as {@code (trino-connector, reader)}. */
public class TrinoConnectorScannerFactory implements JniScannerFactory {

    /**
     * Named for what it does inside the plugin rather than for the connector, because a factory
     * name has to be unique within its plugin whatever its kind - see the table in BE's
     * jni_plugin_registry.h.
     */
    public static final String NAME = "reader";

    /**
     * Where the Trino connectors themselves are installed. BE sends its {@code
     * trino_connector_plugin_dir} config under this key on every scan; it used to call a static
     * setter on this plugin's classes directly, which stopped being possible once those classes
     * became private to the plugin's own classloader.
     */
    static final String PLUGIN_DIR = "trino_connector_plugin_dir";

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * The directory is applied here rather than in the scanner because the connectors are loaded
     * once for the whole process, before the first scan opens - see
     * {@link TrinoConnectorPluginLoader}. A scan that arrives without the parameter leaves the
     * previously configured directory alone, which is what an operator changing the config and
     * restarting BE gets anyway.
     */
    @Override
    public JniScanner create(int batchSize, Map<String, String> params) {
        String pluginsDir = params.get(PLUGIN_DIR);
        if (pluginsDir != null && !pluginsDir.isEmpty()) {
            TrinoConnectorPluginLoader.setPluginsDir(pluginsDir);
        }
        return new TrinoConnectorJniScanner(batchSize, params);
    }
}
