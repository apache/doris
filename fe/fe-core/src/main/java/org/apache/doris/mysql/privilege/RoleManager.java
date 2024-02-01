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

import org.apache.doris.analysis.ResourcePattern;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.analysis.WorkloadGroupPattern;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.MysqlDb;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.mysql.privilege.Auth.PrivLevel;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.workloadgroup.WorkloadGroupMgr;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RoleManager implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(RoleManager.class);
    //prefix of each user default role
    public static String DEFAULT_ROLE_PREFIX = "default_role_rbac_";

    // Concurrency control is delegated by Auth, so not concurrentMap
    @SerializedName(value = "roles")
    private Map<String, Role> roles = Maps.newHashMap();

    public RoleManager() {
        roles.put(Role.OPERATOR.getRoleName(), Role.OPERATOR);
        roles.put(Role.ADMIN.getRoleName(), Role.ADMIN);
    }

    public Role getRole(String name) {
        return roles.get(name);
    }

    public Role addOrMergeRole(Role newRole, boolean errOnExist) throws DdlException {
        Role existingRole = roles.get(newRole.getRoleName());
        if (existingRole != null) {
            if (errOnExist) {
                throw new DdlException("Role " + newRole + " already exists");
            }
            // merge
            existingRole.merge(newRole);
            return existingRole;
        } else {
            roles.put(newRole.getRoleName(), newRole);
            return newRole;
        }
    }

    public void dropRole(String qualifiedRole, boolean errOnNonExist) throws DdlException {
        if (!roles.containsKey(qualifiedRole)) {
            if (errOnNonExist) {
                throw new DdlException("Role " + qualifiedRole + " does not exist");
            }
            return;
        }

        // we just remove the role from this map and remain others unchanged(privs, etc..)
        roles.remove(qualifiedRole);
    }

    public Role revokePrivs(String name, TablePattern tblPattern, PrivBitSet privs,
            Map<ColPrivilegeKey, Set<String>> colPrivileges, boolean errOnNonExist)
            throws DdlException {
        Role existingRole = roles.get(name);
        if (existingRole == null) {
            if (errOnNonExist) {
                throw new DdlException("Role " + name + " does not exist");
            }
            return null;
        }
        existingRole.revokePrivs(tblPattern, privs, colPrivileges, errOnNonExist);
        return existingRole;
    }

    public Role revokePrivs(String role, ResourcePattern resourcePattern, PrivBitSet privs, boolean errOnNonExist)
            throws DdlException {
        Role existingRole = roles.get(role);
        if (existingRole == null) {
            if (errOnNonExist) {
                throw new DdlException("Role " + role + " does not exist");
            }
            return null;
        }
        existingRole.revokePrivs(resourcePattern, privs, errOnNonExist);
        return existingRole;
    }

    public Role revokePrivs(String role, WorkloadGroupPattern workloadGroupPattern, PrivBitSet privs,
            boolean errOnNonExist)
            throws DdlException {
        Role existingRole = roles.get(role);
        if (existingRole == null) {
            if (errOnNonExist) {
                throw new DdlException("Role " + role + " does not exist");
            }
            return null;
        }
        existingRole.revokePrivs(workloadGroupPattern, privs, errOnNonExist);
        return existingRole;
    }

    public void getRoleInfo(List<List<String>> results) {
        for (Role role : roles.values()) {
            if (ClusterNamespace.getNameFromFullName(role.getRoleName()).startsWith(DEFAULT_ROLE_PREFIX)) {
                if (ConnectContext.get() == null || !ConnectContext.get().getSessionVariable().showUserDefaultRole) {
                    continue;
                }
            }
            List<String> info = Lists.newArrayList();
            info.add(role.getRoleName());
            info.add(Joiner.on(", ").join(Env.getCurrentEnv().getAuth().getRoleUsers(role.getRoleName())));
            Map<PrivLevel, String> infoMap =
                    Stream.concat(
                            role.getTblPatternToPrivs().entrySet().stream()
                                    .collect(Collectors.groupingBy(entry -> entry.getKey().getPrivLevel())).entrySet()
                                    .stream(),
                            Stream.concat(role.getResourcePatternToPrivs().entrySet().stream()
                                            .collect(Collectors.groupingBy(entry -> entry.getKey().getPrivLevel()))
                                            .entrySet().stream(),
                                    role.getWorkloadGroupPatternToPrivs().entrySet().stream()
                                            .collect(Collectors.groupingBy(entry -> entry.getKey().getPrivLevel()))
                                            .entrySet().stream())
                    ).collect(Collectors.toMap(Entry::getKey, entry -> {
                                if (entry.getKey() == PrivLevel.GLOBAL) {
                                    return entry.getValue().stream().findFirst().map(priv -> priv.getValue().toString())
                                            .orElse(FeConstants.null_string);
                                } else {
                                    return entry.getValue().stream()
                                            .map(priv -> priv.getKey() + ": " + priv.getValue())
                                            .collect(Collectors.joining("; "));
                                }
                            }, (s1, s2) -> s1 + " " + s2
                    ));
            Stream.of(PrivLevel.GLOBAL, PrivLevel.CATALOG, PrivLevel.DATABASE, PrivLevel.TABLE, PrivLevel.RESOURCE)
                    .forEach(level -> {
                        String infoItem = infoMap.get(level);
                        if (Strings.isNullOrEmpty(infoItem)) {
                            infoItem = FeConstants.null_string;
                        }
                        info.add(infoItem);
                    });
            results.add(info);
        }
    }

    public Role createDefaultRole(UserIdentity userIdent) throws DdlException {
        String userDefaultRoleName = getUserDefaultRoleName(userIdent);
        if (roles.containsKey(userDefaultRoleName)) {
            return roles.get(userDefaultRoleName);
        }

        // grant read privs to database information_schema & mysql
        List<TablePattern> tablePatterns = Lists.newArrayList();
        TablePattern informationTblPattern = new TablePattern(Auth.DEFAULT_CATALOG, InfoSchemaDb.DATABASE_NAME, "*");
        try {
            informationTblPattern.analyze();
            tablePatterns.add(informationTblPattern);
        } catch (AnalysisException e) {
            LOG.warn("should not happen", e);
        }
        TablePattern mysqlTblPattern = new TablePattern(Auth.DEFAULT_CATALOG, MysqlDb.DATABASE_NAME, "*");
        try {
            mysqlTblPattern.analyze();
            tablePatterns.add(mysqlTblPattern);
        } catch (AnalysisException e) {
            LOG.warn("should not happen", e);
        }

        // grant read privs of default workload group
        WorkloadGroupPattern workloadGroupPattern = new WorkloadGroupPattern(WorkloadGroupMgr.DEFAULT_GROUP_NAME);
        try {
            workloadGroupPattern.analyze();
        } catch (AnalysisException e) {
            LOG.warn("should not happen", e);
        }
        Role role = new Role(userDefaultRoleName, tablePatterns, PrivBitSet.of(Privilege.SELECT_PRIV),
                workloadGroupPattern, PrivBitSet.of(Privilege.USAGE_PRIV));
        roles.put(role.getRoleName(), role);
        return role;
    }

    public Role removeDefaultRole(UserIdentity userIdent) {
        return roles.remove(getUserDefaultRoleName(userIdent));
    }

    public String getUserDefaultRoleName(UserIdentity userIdentity) {
        return userIdentity.toDefaultRoleName();
    }

    public Map<String, Role> getRoles() {
        return roles;
    }

    public void rectifyPrivs() {
        for (Map.Entry<String, Role> entry : roles.entrySet()) {
            entry.getValue().rectifyPrivs();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Roles: ");
        for (Role role : roles.values()) {
            sb.append(role).append("\n");
        }
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static RoleManager read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_116) {
            RoleManager roleManager = new RoleManager();
            roleManager.readFields(in);
            return roleManager;
        } else {
            String json = Text.readString(in);
            RoleManager rm = GsonUtils.GSON.fromJson(json, RoleManager.class);
            return rm;
        }
    }

    // should be removed after version 3.0
    private void removeClusterPrefix() {
        Map<String, Role> newRoles = Maps.newHashMap();
        for (Map.Entry<String, Role> entry : roles.entrySet()) {
            String roleName = ClusterNamespace.getNameFromFullName(entry.getKey());
            newRoles.put(roleName, entry.getValue());
        }
        roles = newRoles;
    }

    @Deprecated
    private void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            Role role = Role.read(in);
            roles.put(role.getRoleName(), role);
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        removeClusterPrefix();
    }
}
