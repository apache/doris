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

package org.apache.doris.authentication.spi;

import org.apache.doris.extension.spi.PluginDescriptor;
import org.apache.doris.extension.spi.PluginFactory;

/**
 * Factory interface for creating authentication plugins.
 *
 * <p>Factories are discovered via {@link java.util.ServiceLoader} so that
 * plugin implementations can be provided without changing handler code.
 *
 * <p>ServiceLoader configuration:
 * <pre>
 * # META-INF/services/org.apache.doris.authentication.spi.AuthenticationPluginFactory
 * org.apache.doris.authentication.plugins.ldap.LdapPluginFactory
 * org.apache.doris.authentication.plugins.oidc.OidcPluginFactory
 * org.apache.doris.authentication.plugins.password.PasswordPluginFactory
 * org.apache.company.custom.CustomAuthPluginFactory  # third-party plugin
 * </pre>
 *
 * <p>Design principles:
 * <ul>
 *   <li>Plugin name is a string (not enum) for extensibility</li>
 *   <li>Factory can return singleton or new instance each time</li>
 *   <li>Third parties can develop custom plugins without modifying Doris code</li>
 * </ul>
 */
public interface AuthenticationPluginFactory extends PluginFactory {

    /**
     * Get the plugin name this factory creates.
     * Must match the plugin's name() method.
     *
     * @return plugin name (e.g., "ldap", "oidc", "password")
     */
    @Override
    String name();

    /**
     * Create a plugin instance.
     * Can be singleton or new instance each time.
     *
     * @return plugin instance
     */
    AuthenticationPlugin create();

    @Override
    default AuthenticationPlugin create(PluginDescriptor descriptor) {
        return create();
    }
}
