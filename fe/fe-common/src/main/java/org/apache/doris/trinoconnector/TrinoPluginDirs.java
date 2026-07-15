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

/**
 * The default Trino plugin dir, shared by every Java component that must agree on it: FE
 * {@code Config.trino_connector_plugin_dir}, the FE trino-connector plugin
 * ({@code TrinoBootstrap}) and the BE scanner ({@code TrinoConnectorPluginLoader}).
 *
 * <p>All three decide "did the user set {@code trino_connector_plugin_dir} explicitly?" by
 * comparing the configured value against this default, and fall back to a legacy dir when it is
 * untouched. They must therefore read the same value: drift would make an untouched config look
 * like an explicit override, skip the legacy fallback, and silently strand the plugins of every
 * deployment that upgraded without moving them.
 *
 * <p>Lives in this package rather than {@code org.apache.doris.common} because connectors are
 * barred from importing fe-core internals (tools/check-connector-imports.sh). This package is
 * already the FE/BE-shared Trino package both consumers depend on — {@code TrinoColumnMetadata}
 * next door is loaded by the FE plugin at runtime, so nothing about this is exotic.
 *
 * <p>BE {@code config.cpp} holds the one copy no Java code can reach; that one rides on the
 * cross-referencing comments alone.
 */
public class TrinoPluginDirs {
    public static final String DEFAULT_PLUGIN_SUBDIR = "/plugins/trino_plugins";

    // Suppress default constructor for noninstantiability
    private TrinoPluginDirs() {
        throw new AssertionError();
    }
}
