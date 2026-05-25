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

package org.apache.doris.authentication.plugin.password;

import org.apache.doris.authentication.spi.AuthenticationPlugin;
import org.apache.doris.authentication.spi.AuthenticationPluginFactory;

/**
 * Factory for creating {@link PasswordAuthenticationPlugin} instances.
 *
 * <p>This factory is discovered via Java ServiceLoader mechanism.
 * Configuration file: META-INF/services/org.apache.doris.authentication.spi.AuthenticationPluginFactory
 */
public class PasswordAuthenticationPluginFactory implements AuthenticationPluginFactory {

    private static final String PLUGIN_NAME = "password";

    @Override
    public String name() {
        return PLUGIN_NAME;
    }

    @Override
    public AuthenticationPlugin create() {
        // One plugin instance per AuthenticationIntegration.
        return new PasswordAuthenticationPlugin();
    }

}
