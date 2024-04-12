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

package org.apache.doris.persist;

import org.apache.doris.analysis.PasswordOptions;
import org.apache.doris.analysis.ResourcePattern;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.analysis.WorkloadGroupPattern;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.mysql.privilege.ColPrivilegeKey;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PrivInfo implements Writable, GsonPostProcessable {
    @SerializedName(value = "userIdent")
    private UserIdentity userIdent;
    @SerializedName(value = "tblPattern")
    private TablePattern tblPattern;
    @SerializedName(value = "resourcePattern")
    private ResourcePattern resourcePattern;
    @SerializedName(value = "workloadGroupPattern")
    private WorkloadGroupPattern workloadGroupPattern;
    @SerializedName(value = "privs")
    private PrivBitSet privs;
    @SerializedName(value = "passwd")
    private byte[] passwd;
    @SerializedName(value = "role")
    private String role;
    @SerializedName(value = "comment")
    private String comment;
    @SerializedName(value = "colPrivileges")
    private Map<ColPrivilegeKey, Set<String>> colPrivileges;
    @SerializedName(value = "passwordOptions")
    private PasswordOptions passwordOptions;
    // Indicates that these roles are granted to a user
    @SerializedName(value = "roles")
    private List<String> roles;

    @SerializedName(value = "userId")
    private String userId;

    private PrivInfo() {

    }

    // For create user/set password/create role/drop role
    public PrivInfo(UserIdentity userIdent, PrivBitSet privs, byte[] passwd, String role,
            PasswordOptions passwordOptions) {
        this(userIdent, privs, passwd, role, passwordOptions, null, null);
    }

    public PrivInfo(UserIdentity userIdent, PrivBitSet privs, byte[] passwd, String role,
            PasswordOptions passwordOptions, String comment, String userId) {
        this.userIdent = userIdent;
        this.tblPattern = null;
        this.resourcePattern = null;
        this.privs = privs;
        this.passwd = passwd;
        this.role = role;
        this.passwordOptions = passwordOptions;
        this.comment = comment;
        this.userId = userId;
    }

    public PrivInfo(String role, String comment) {
        this.role = role;
        this.comment = comment;
    }

    // For grant/revoke
    public PrivInfo(UserIdentity userIdent, TablePattern tablePattern, PrivBitSet privs,
            byte[] passwd, String role, Map<ColPrivilegeKey, Set<String>> colPrivileges) {
        this.userIdent = userIdent;
        this.tblPattern = tablePattern;
        this.resourcePattern = null;
        this.workloadGroupPattern = null;
        this.privs = privs;
        this.passwd = passwd;
        this.role = role;
        this.colPrivileges = colPrivileges;
    }

    // For grant/revoke resource priv
    public PrivInfo(UserIdentity userIdent, ResourcePattern resourcePattern, PrivBitSet privs,
            byte[] passwd, String role) {
        this.userIdent = userIdent;
        this.tblPattern = null;
        this.workloadGroupPattern = null;
        this.resourcePattern = resourcePattern;
        this.privs = privs;
        this.passwd = passwd;
        this.role = role;
    }

    public PrivInfo(UserIdentity userIdent, WorkloadGroupPattern workloadGroupPattern, PrivBitSet privs,
            byte[] passwd, String role) {
        this.userIdent = userIdent;
        this.tblPattern = null;
        this.resourcePattern = null;
        this.workloadGroupPattern = workloadGroupPattern;
        this.privs = privs;
        this.passwd = passwd;
        this.role = role;
    }

    // For grant/revoke roles to/from userIdent
    public PrivInfo(UserIdentity userIdent, List<String> roles) {
        this.userIdent = userIdent;
        this.roles = roles;
    }

    public UserIdentity getUserIdent() {
        return userIdent;
    }

    public TablePattern getTblPattern() {
        return tblPattern;
    }

    public ResourcePattern getResourcePattern() {
        return resourcePattern;
    }

    public WorkloadGroupPattern getWorkloadGroupPattern() {
        return workloadGroupPattern;
    }

    public PrivBitSet getPrivs() {
        return privs;
    }

    public byte[] getPasswd() {
        return passwd;
    }

    public String getRole() {
        return role;
    }

    public String getComment() {
        return comment;
    }

    public String getUserId() {
        return userId;
    }

    public PasswordOptions getPasswordOptions() {
        return passwordOptions == null ? PasswordOptions.UNSET_OPTION : passwordOptions;
    }

    public List<String> getRoles() {
        return roles;
    }

    public Map<ColPrivilegeKey, Set<String>> getColPrivileges() {
        return colPrivileges;
    }

    private void removeClusterPrefix() {
        if (userIdent != null) {
            userIdent.removeClusterPrefix();
        }
        if (roles != null) {
            for (int i = 0; i < roles.size(); i++) {
                roles.set(i, ClusterNamespace.getNameFromFullName(roles.get(i)));
            }
        }
        if (role != null) {
            role = ClusterNamespace.getNameFromFullName(role);
        }
    }

    public static PrivInfo read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_113) {
            PrivInfo info = new PrivInfo();
            info.readFields(in);
            return info;
        } else {
            return GsonUtils.GSON.fromJson(Text.readString(in), PrivInfo.class);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Deprecated
    private void readFields(DataInput in) throws IOException {
        if (in.readBoolean()) {
            userIdent = UserIdentity.read(in);
        }

        if (in.readBoolean()) {
            tblPattern = TablePattern.read(in);
        }

        if (in.readBoolean()) {
            resourcePattern = ResourcePattern.read(in);
        }

        if (in.readBoolean()) {
            privs = PrivBitSet.read(in);
        }

        if (in.readBoolean()) {
            int passwordLen = in.readInt();
            passwd = new byte[passwordLen];
            in.readFully(passwd);
        }

        if (in.readBoolean()) {
            role = Text.readString(in);
        }

        passwordOptions = PasswordOptions.UNSET_OPTION;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        removeClusterPrefix();
    }
}
