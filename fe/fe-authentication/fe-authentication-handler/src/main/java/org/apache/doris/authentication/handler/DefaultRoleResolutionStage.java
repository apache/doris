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
import java.util.Objects;
import java.util.Set;

/**
 * Default role resolution stage using the role mapper.
 *
 * <p>This implementation delegates to RoleMapper for actual mapping.
 */
public class DefaultRoleResolutionStage implements RoleResolutionStage {

    private final RoleMapper roleMapper;

    public DefaultRoleResolutionStage(RoleMapper roleMapper) {
        this.roleMapper = Objects.requireNonNull(roleMapper, "roleMapper");
    }

    @Override
    public Set<String> resolveRoles(Identity identity, AuthenticationIntegration integration) {
        if (identity == null || integration == null) {
            return Collections.emptySet();
        }
        return roleMapper.mapRoles(identity, integration);
    }
}
