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
import org.apache.doris.authorization.AccessDeniedException;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.authorization.spi.AuthorizationContext;
import org.apache.doris.authorization.spi.AuthorizationPlugin;
import org.apache.doris.authorization.spi.AuthorizationPluginFactory;

import java.util.Map;

/**
 * An authorization source whose class bytes are copied into a temporary plugin jar, so that a test can load
 * it exactly the way FE loads a third-party one.
 *
 * <p>It lives in {@code org.apache.doris.pluginapiversion.testplugins} on purpose. The AUTHORIZATION family
 * declares {@code org.apache.doris.authorization.} parent-first, so a probe living there would be loaded
 * from the FE's own classpath rather than from the plugin jar - and the loader reads the declared API
 * version from the jar that <em>defines</em> the factory class. Such a probe would look undeclared no matter
 * what its jar said, and every test here would pass for the wrong reason.
 *
 * <p>Factory and plugin are one class so that a single class entry makes a complete plugin jar.
 */
public class VersionProbeAuthorizationPluginFactory
        implements AuthorizationPluginFactory, AuthorizationPlugin {

    public static final String NAME = "version_probe_authz";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public AuthorizationPlugin create(Map<String, String> properties, AuthorizationContext context) {
        return this;
    }

    @Override
    public void checkPrivilege(AuthorizedSubject subject, AuthorizedResource resource,
            AccessRequirement requirement, AccessContext context) throws AccessDeniedException {
        throw AccessDeniedException.of(subject, resource, requirement, NAME);
    }
}
