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

import org.apache.doris.analysis.PasswordOptions;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.mysql.privilege.PasswordPolicy.ExpirePolicy;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class PasswordPolicyManager implements Writable {
    @SerializedName(value = "policyMap")
    private Map<UserIdentity, PasswordPolicy> policyMap = Maps.newConcurrentMap();

    public PasswordPolicyManager() {

    }

    private PasswordPolicy getOrCreatePolicy(UserIdentity userIdent) {
        PasswordPolicy passwordPolicy = policyMap.get(userIdent);
        if (passwordPolicy == null) {
            // When upgrading from old Doris version(before v1.2), there may be not entry in this manager.
            // create and return a default one.
            // The following logic is just to handle some concurrent issue.
            PasswordPolicy policy = PasswordPolicy.createDefault();
            passwordPolicy = policyMap.putIfAbsent(userIdent, policy);
            if (passwordPolicy == null) {
                passwordPolicy = policy;
            }
        }
        return passwordPolicy;
    }

    private boolean hasUser(UserIdentity userIdent) {
        return policyMap.containsKey(userIdent);
    }

    public void checkAccountLockedAndPasswordExpiration(UserIdentity curUser) throws AuthenticationException {
        if (!hasUser(curUser)) {
            return;
        }
        PasswordPolicy policy = getOrCreatePolicy(curUser);
        policy.checkAccountLockedAndPasswordExpiration(curUser);
    }

    public boolean onFailedLogin(UserIdentity curUser) {
        if (!hasUser(curUser)) {
            return false;
        }
        PasswordPolicy passwordPolicy = getOrCreatePolicy(curUser);
        return passwordPolicy.onFailedLogin();
    }

    public boolean checkPasswordHistory(UserIdentity curUser, byte[] password) {
        if (!hasUser(curUser)) {
            return true;
        }
        PasswordPolicy passwordPolicy = getOrCreatePolicy(curUser);
        return passwordPolicy.checkPasswordHistory(password);
    }

    public void updatePolicy(UserIdentity curUser, byte[] password, PasswordOptions passwordOptions) {
        PasswordPolicy passwordPolicy = getOrCreatePolicy(curUser);
        passwordPolicy.update(password, passwordOptions);
    }

    public void updatePassword(UserIdentity curUser, byte[] password) {
        PasswordPolicy passwordPolicy = getOrCreatePolicy(curUser);
        passwordPolicy.updatePassword(password);

        // Compatible with setting the password expiration time and changing the password again
        ExpirePolicy expirePolicy = passwordPolicy.getExpirePolicy();
        if (expirePolicy.passwordCreateTime != 0) {
            expirePolicy.setPasswordCreateTime();
        }
    }

    public List<List<String>> getPolicyInfo(UserIdentity userIdent) {
        if (!hasUser(userIdent)) {
            return Lists.newArrayList();
        }
        PasswordPolicy passwordPolicy = getOrCreatePolicy(userIdent);
        return passwordPolicy.getInfo();
    }

    public void unlockUser(UserIdentity userIdent) {
        if (!hasUser(userIdent)) {
            return;
        }
        PasswordPolicy passwordPolicy = getOrCreatePolicy(userIdent);
        passwordPolicy.unlockAccount();
    }

    public void dropUser(UserIdentity userIdent) {
        policyMap.remove(userIdent);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static PasswordPolicyManager read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), PasswordPolicyManager.class);
    }

}
