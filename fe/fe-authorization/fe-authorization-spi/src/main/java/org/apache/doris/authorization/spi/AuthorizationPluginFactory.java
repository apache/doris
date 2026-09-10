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

package org.apache.doris.authorization.spi;

import org.apache.doris.extension.spi.Plugin;
import org.apache.doris.extension.spi.PluginFactory;

import java.util.Map;

/**
 * Creates an {@link AuthorizationPlugin}. This is what a plugin jar publishes for the engine to discover.
 *
 * <p>A plugin is created once and kept: unlike an authentication attempt, an authorization decision happens
 * many times within a single statement, and a source that caches policies has to be the same instance
 * throughout. The engine builds a new one only when what configures it changes.</p>
 *
 * <p>Extending {@link PluginFactory} is what lets a jar in {@code plugins/authorization/} be discovered by
 * the same loader the other plugin families use, and therefore be held to the same plugin API version
 * contract before any of its code runs.</p>
 */
public interface AuthorizationPluginFactory extends PluginFactory {

    /**
     * The name this source is selected by in configuration, and the name its plugin reports.
     *
     * @return plugin name, e.g. {@code "ranger-doris"}
     */
    @Override
    String name();

    /** One line about what this source is, for logs and diagnostics. */
    @Override
    default String description() {
        return "";
    }

    /**
     * @param properties configuration for this instance of the source, as configured
     * @param context what the engine will answer if this plugin asks; see {@link AuthorizationContext}
     */
    AuthorizationPlugin create(Map<String, String> properties, AuthorizationContext context);

    /**
     * Never called for an authorization source: an authorization plugin cannot be built without the context
     * it asks the engine questions through, so the engine only ever calls
     * {@link #create(Map, AuthorizationContext)}. Present because {@link PluginFactory} declares it, which is
     * what makes this factory discoverable by the shared plugin loader.
     */
    @Override
    default Plugin create() {
        throw new UnsupportedOperationException("AuthorizationPluginFactory does not support no-arg create();"
                + " an authorization source is built with create(Map, AuthorizationContext)");
    }
}
