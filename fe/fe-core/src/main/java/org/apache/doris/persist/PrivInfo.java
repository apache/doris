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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PrivInfo implements Writable {
    @SerializedName(value = "userIdent")
    private UserIdentity userIdent;
    @SerializedName(value = "tblPattern")
    private TablePattern tblPattern;
    @SerializedName(value = "resourcePattern")
    private ResourcePattern resourcePattern;
    @SerializedName(value = "privs")
    private PrivBitSet privs;
    @SerializedName(value = "passwd")
    private byte[] passwd;
    @SerializedName(value = "role")
    private String role;
    @SerializedName(value = "passwordOptions")
    private PasswordOptions passwordOptions;

    private PrivInfo() {

    }

    // For create user/set password/create role/drop role
    public PrivInfo(UserIdentity userIdent, PrivBitSet privs, byte[] passwd, String role,
            PasswordOptions passwordOptions) {
        this.userIdent = userIdent;
        this.tblPattern = null;
        this.resourcePattern = null;
        this.privs = privs;
        this.passwd = passwd;
        this.role = role;
        this.passwordOptions = passwordOptions;
    }

    // For grant/revoke
    public PrivInfo(UserIdentity userIdent, TablePattern tablePattern, PrivBitSet privs,
            byte[] passwd, String role) {
        this.userIdent = userIdent;
        this.tblPattern = tablePattern;
        this.resourcePattern = null;
        this.privs = privs;
        this.passwd = passwd;
        this.role = role;
    }

    // For grant/revoke resource priv
    public PrivInfo(UserIdentity userIdent, ResourcePattern resourcePattern, PrivBitSet privs,
            byte[] passwd, String role) {
        this.userIdent = userIdent;
        this.tblPattern = null;
        this.resourcePattern = resourcePattern;
        this.privs = privs;
        this.passwd = passwd;
        this.role = role;
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

    public PrivBitSet getPrivs() {
        return privs;
    }

    public byte[] getPasswd() {
        return passwd;
    }

    public String getRole() {
        return role;
    }

    public PasswordOptions getPasswordOptions() {
        return passwordOptions == null ? PasswordOptions.UNSET_OPTION : passwordOptions;
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
}
