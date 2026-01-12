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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.PassVar;
import org.apache.doris.analysis.PasswordOptions;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.Role;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;

/**
 * CreateUserInfo
 */
public class CreateUserInfo {
    private static final Logger LOG = LogManager.getLogger(CreateUserInfo.class);
    private boolean ifNotExist;
    private String role;
    private PasswordOptions passwordOptions;
    private String comment;
    private UserIdentity userIdent;
    private PassVar passVar;
    private String userId;

    /**
     * CreateUserInfo
     */
    public CreateUserInfo(UserDesc userDesc) {
        this(false, userDesc, null, null, "");
    }

    /**
     * CreateUserInfo
     */
    public CreateUserInfo(boolean ifNotExist, UserDesc userDesc,
            String role, PasswordOptions passwordOptions, String comment) {
        this.ifNotExist = ifNotExist;
        this.userIdent = userDesc.getUserIdent();
        this.passVar = userDesc.getPassVar();
        this.role = role;
        this.passwordOptions = passwordOptions;
        this.comment = comment;

        String uId = Env.getCurrentEnv().getAuth().getUserId(ClusterNamespace
                .getNameFromFullName(this.userIdent.getUser()));
        LOG.debug("create user command userIdent {}, userName {}, userId {}",
                this.userIdent, ClusterNamespace.getNameFromFullName(this.userIdent.getUser()), uId);
        // avoid this case "jack@'192.1'" and "jack@'192.2'", jack's uid different
        if (Strings.isNullOrEmpty(uId)) {
            userId = UUID.randomUUID().toString();
        } else {
            userId = uId;
        }

        if (this.passwordOptions == null) {
            this.passwordOptions = PasswordOptions.UNSET_OPTION;
        }
    }

    /**
     * validate
     */
    public void validate() throws AnalysisException {
        userIdent.analyze();

        if (userIdent.isRootUser()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR, "Can not create root user");
        }

        // convert plain password to hashed password
        passVar.analyze();

        if (role != null) {
            if (role.equalsIgnoreCase("SUPERUSER")) {
                // for forward compatibility
                role = Role.ADMIN_ROLE;
            }
            FeNameFormat.checkRoleName(role, true /* can be admin */, "Can not granted user to role");
        }
        passwordOptions.analyze();

        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
        }
    }

    public boolean isIfNotExist() {
        return ifNotExist;
    }

    public String getRole() {
        return role;
    }

    public PasswordOptions getPasswordOptions() {
        return passwordOptions;
    }

    public String getComment() {
        return comment;
    }

    public UserIdentity getUserIdent() {
        return userIdent;
    }

    public String getUserId() {
        return userId;
    }

    public byte[] getPassword() {
        return passVar.getScrambled();
    }

    public PassVar getPassVar() {
        return passVar;
    }
}
