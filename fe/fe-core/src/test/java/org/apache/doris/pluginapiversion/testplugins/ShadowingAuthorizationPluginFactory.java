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

import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.authorization.spi.AuthorizationContext;
import org.apache.doris.authorization.spi.AuthorizationPlugin;
import org.apache.doris.authorization.spi.AuthorizationPluginFactory;

import java.util.Map;

/**
 * An authorization source that claims a name another source on the FE's class path already answers to, so
 * that a test can check what happens when somebody drops such a jar into the plugin directory.
 *
 * <p>Not hypothetical: an allow-everything plugin under the name of a real source would be the cheapest way
 * to turn a filesystem write into read access to everything that source governs.
 */
public class ShadowingAuthorizationPluginFactory
        implements AuthorizationPluginFactory, AuthorizationPlugin {

    /**
     * Deliberately the name of a source registered through the class-path channel.
     *
     * <p>Doris no longer ships one: the Ranger sources are installed from {@code plugins/authorization/}
     * like any third-party plugin, and here {@code ranger-doris} is on the class path only because the
     * ranger plugin module is a test dependency of fe-core. That still exercises the guard for what does
     * reach the class-path channel in production - a jar an operator put in {@code fe/lib}.
     */
    public static final String SHADOWED_NAME = "ranger-doris";

    @Override
    public String name() {
        return SHADOWED_NAME;
    }

    @Override
    public AuthorizationPlugin create(Map<String, String> properties, AuthorizationContext context) {
        return this;
    }

    @Override
    public void checkPrivilege(AuthorizedSubject subject, AuthorizedResource resource,
            AccessRequirement requirement, AccessContext context) {
        // Allows everything, which is exactly what must never become reachable this way.
    }
}
