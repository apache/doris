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
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class UserRoleManager implements Writable {
    private Map<UserIdentity, Set<String>> userToRoles = Maps.newConcurrentMap();
    private Map<String, Set<UserIdentity>> roleToUsers = Maps.newConcurrentMap();

    public void addUserRole(UserIdentity userIdentity, String roleName) {
        Set<String> roles = userToRoles.get(userIdentity);
        if (CollectionUtils.isEmpty(roles)) {
            roles = Sets.newHashSet();
        }
        roles.add(roleName);
        userToRoles.put(userIdentity, roles);
        Set<UserIdentity> userIdentities = roleToUsers.get(roleName);
        if (CollectionUtils.isEmpty(userIdentities)) {
            userIdentities = Sets.newHashSet();
        }
        userIdentities.add(userIdentity);
        roleToUsers.put(roleName, userIdentities);
    }

    public void dropUser(UserIdentity userIdentity) {
        Set<String> roles = userToRoles.remove(userIdentity);
        for (String roleName : roles) {
            Set<UserIdentity> userIdentities = roleToUsers.get(roleName);
            if (CollectionUtils.isEmpty(userIdentities)) {
                continue;
            }
            userIdentities.remove(userIdentity);
            if (CollectionUtils.isEmpty(userIdentities)) {
                roleToUsers.remove(roleName);
            }
        }
    }

    public void dropRole(String roleName) {
        Set<UserIdentity> remove = roleToUsers.remove(roleName);
        for (UserIdentity userIdentity : remove) {
            Set<String> roles = userToRoles.get(userIdentity);
            if (CollectionUtils.isEmpty(roles)) {
                continue;
            }
            roles.remove(roleName);
            if (CollectionUtils.isEmpty(roles)) {
                userToRoles.remove(userIdentity);
            }
        }
    }

    public Set<String> getRolesByUser(UserIdentity user) {
        return userToRoles.get(user);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(userToRoles.size());
        for (Entry<UserIdentity, Set<String>> entry : userToRoles.entrySet()) {
            entry.getKey().write(out);
            out.writeInt(entry.getValue().size());
            for (String role : entry.getValue()) {
                Text.writeString(out, role);
            }
        }
    }

    public static UserRoleManager read(DataInput in) throws IOException {
        UserRoleManager userRoleManager = new UserRoleManager();
        userRoleManager.readFields(in);
        return userRoleManager;
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            UserIdentity userIdentity = UserIdentity.read(in);
            int roleSize = in.readInt();
            Set<String> roles = Sets.newHashSet();
            for (int j = 0; j < roleSize; j++) {
                String roleName = Text.readString(in);
                roles.add(roleName);
                //init role to user map
                Set<UserIdentity> userIdentities = roleToUsers.get(roleName);
                if (CollectionUtils.isEmpty(userIdentities)) {
                    userIdentities = Sets.newHashSet();
                }
                userIdentities.add(userIdentity);
                roleToUsers.put(roleName, userIdentities);
            }
            userToRoles.put(userIdentity, roles);
        }
    }
}
