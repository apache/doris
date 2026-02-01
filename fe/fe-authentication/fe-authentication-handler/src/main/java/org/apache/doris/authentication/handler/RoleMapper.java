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

import org.apache.doris.authentication.AuthenticationProfile;
import org.apache.doris.authentication.Identity;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Role mapper - maps external groups to internal roles.
 *
 * <p>Maps external groups (from LDAP, OIDC, etc.) to internal Doris roles
 * based on AuthenticationProfile's role mapping configuration.
 */
public class RoleMapper {

    /**
     * Map external groups to internal roles.
     *
     * @param identity authenticated identity (contains external groups)
     * @param profile authentication profile (contains role mapping)
     * @return set of internal role names
     */
    public Set<String> mapRoles(Identity identity, AuthenticationProfile profile) {
        if (identity == null || profile == null) {
            return Collections.emptySet();
        }
        Map<String, String> roleMapping = profile.getRoleMapping();
        if (roleMapping == null || roleMapping.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> roles = new HashSet<>();
        for (String externalGroup : identity.getExternalGroups()) {
            if (externalGroup == null) {
                continue;
            }
            String mapped = roleMapping.get(externalGroup);
            if (mapped != null && !mapped.isEmpty()) {
                roles.add(mapped);
            }
        }
        return roles;
    }
}
