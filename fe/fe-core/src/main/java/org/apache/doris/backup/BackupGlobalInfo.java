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

package org.apache.doris.backup;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.blockrule.SqlBlockRule;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.mysql.privilege.PasswordPolicy;
import org.apache.doris.mysql.privilege.Role;
import org.apache.doris.mysql.privilege.User;
import org.apache.doris.mysql.privilege.UserProperty;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.policy.Policy;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.resource.workloadgroup.WorkloadGroup;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BackupGlobalInfo implements Writable {
    // Users
    @SerializedName(value = "users")
    private List<User> userList = Lists.newArrayList();
    // roles
    @SerializedName(value = "roles")
    private List<Role> roleList = Lists.newArrayList();
    @SerializedName(value = "userProperties")
    private List<UserProperty> userProperties = Lists.newArrayList();
    @SerializedName(value = "roleToUsers")
    private Map<String, Set<UserIdentity>> roleToUsers = Maps.newHashMap();
    @SerializedName(value = "policyMap")
    private Map<UserIdentity, PasswordPolicy> policyMap = Maps.newHashMap();
    @SerializedName(value = "propertyMap")
    protected Map<String, UserProperty> propertyMap = Maps.newHashMap();
    // rowPolicy name -> policy
    @SerializedName(value = "rowPolicies")
    private List<Policy> rowPolicies = Lists.newArrayList();
    @SerializedName(value = "sqlBlockRules")
    private List<SqlBlockRule> sqlBlockRules = Lists.newArrayList();
    @SerializedName(value = "catalogs")
    private List<BackupCatalogMeta> catalogs = Lists.newArrayList();
    @SerializedName(value = "workloadGroups")
    private List<WorkloadGroup> workloadGroups = Lists.newArrayList();

    public BackupGlobalInfo() {
    }

    public List<User> getUserList() {
        return userList;
    }

    public List<Role> getRoleList() {
        return roleList;
    }

    public List<UserProperty> getUserProperties() {
        return userProperties;
    }

    public Map<String, Set<UserIdentity>>  getRoleToUsers() {
        return roleToUsers;
    }

    public Map<UserIdentity, PasswordPolicy>  getPolicyMap() {
        return policyMap;
    }

    public Set<UserIdentity> getUsersByRole(String roleName) {
        return roleToUsers.get(roleName);
    }

    public List<Policy>  getRowPolicies() {
        return rowPolicies;
    }

    public List<WorkloadGroup>  getWorkloadGroups() {
        return workloadGroups;
    }

    public List<BackupCatalogMeta>  getCatalogs() {
        return catalogs;
    }

    public  List<SqlBlockRule>  getSqlBlockRules() {
        return sqlBlockRules;
    }

    public void init(boolean backupPriv, boolean backupCatalog, boolean backupWorkloadGroup) {

        if (backupPriv) {
            Env.getCurrentEnv().getAuth().getAuthInfoCopied(userList, roleList, userProperties, roleToUsers,
                    propertyMap, policyMap);
            rowPolicies = Env.getCurrentEnv().getPolicyMgr().getCopiedPoliciesByType(PolicyTypeEnum.ROW);
            sqlBlockRules = Env.getCurrentEnv().getSqlBlockRuleMgr().getAllRulesCopied();
        }

        if (backupCatalog) {
            catalogs = Env.getCurrentEnv().getCatalogMgr().getAllCatalogsCopied();
        }

        if (backupWorkloadGroup) {
            workloadGroups = Env.getCurrentEnv().getWorkloadGroupMgr().getAllWorkloadGroupsCopied();
        }
    }

    public static BackupGlobalInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, BackupGlobalInfo.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

}
