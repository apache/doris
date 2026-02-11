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

package org.apache.doris.authentication.handler;

import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.Identity;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Role mapper - maps external groups to internal roles.
 *
 * <p>Maps external groups (from LDAP, OIDC, etc.) to internal Doris roles.
 * Role mapping configuration can be stored in integration properties.
 *
 * <p>Property format for role mapping:
 * <pre>
 * role.mapping.&lt;external_group&gt; = &lt;internal_role&gt;
 * </pre>
 *
 * <p>Example:
 * <pre>
 * role.mapping.cn=admins,ou=groups,dc=example,dc=com = admin
 * role.mapping.cn=developers,ou=groups,dc=example,dc=com = developer
 * </pre>
 */
public class RoleMapper {

    /** Property prefix for role mappings */
    public static final String ROLE_MAPPING_PREFIX = "role.mapping.";

    /**
     * Map external groups to internal roles.
     *
     * @param identity authenticated identity (contains external groups)
     * @param integration authentication integration (contains role mapping in properties)
     * @return set of internal role names
     */
    public Set<String> mapRoles(Identity identity, AuthenticationIntegration integration) {
        if (identity == null || integration == null) {
            return Collections.emptySet();
        }

        Set<String> roles = new HashSet<>();
        for (String externalGroup : identity.getExternalGroups()) {
            if (externalGroup == null) {
                continue;
            }
            // Look for mapping in integration properties
            integration.getProperty(ROLE_MAPPING_PREFIX + externalGroup)
                    .filter(s -> !s.isEmpty())
                    .ifPresent(roles::add);
        }
        return roles;
    }
}
