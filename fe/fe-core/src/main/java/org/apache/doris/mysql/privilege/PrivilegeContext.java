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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.UserIdentity;

import com.google.common.collect.ImmutableSet;

import java.util.Objects;
import java.util.Set;

public class PrivilegeContext {
    private final UserIdentity currentUser;
    private final Set<String> currentRoles;

    private PrivilegeContext(UserIdentity currentUser, Set<String> currentRoles) {
        this.currentUser = Objects.requireNonNull(currentUser, "require currentUser object");
        this.currentRoles = currentRoles == null ? null : ImmutableSet.copyOf(currentRoles);
    }

    public static PrivilegeContext of(UserIdentity currentUser) {
        return new PrivilegeContext(currentUser, null);
    }

    public static PrivilegeContext of(UserIdentity currentUser, Set<String> currentRoles) {
        return new PrivilegeContext(currentUser, currentRoles);
    }

    public UserIdentity getCurrentUser() {
        return currentUser;
    }

    public Set<String> getCurrentRoles() {
        return currentRoles;
    }
}
