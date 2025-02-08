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

import org.apache.doris.analysis.CreateUserStmt;
import org.apache.doris.analysis.PasswordOptions;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.mysql.authenticate.AuthenticateType;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.Role;
import org.apache.doris.qe.ConnectContext;

/**
 * CreateUserInfo
 */
public class CreateUserInfo {
    private boolean ifNotExist;
    private String role;
    private PasswordOptions passwordOptions;
    private String comment;
    private UserDesc userDesc;

    public CreateUserInfo(boolean ifNotExist, UserDesc userDesc,
            String role, PasswordOptions passwordOptions, String comment) {
        this.ifNotExist = ifNotExist;
        this.userDesc = userDesc;
        this.role = role;
        this.passwordOptions = passwordOptions;
        this.comment = comment;
    }

    /**
     * validate
     */
    public void validate() throws AnalysisException {
        if (Config.access_controller_type.equalsIgnoreCase("ranger-doris")
                && AuthenticateType.getAuthTypeConfig() == AuthenticateType.LDAP) {
            throw new AnalysisException("Create user is prohibited when Ranger and LDAP are enabled at same time.");
        }

        userDesc.getUserIdent().analyze();

        if (userDesc.getUserIdent().isRootUser()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR, "Can not create root user");
        }

        // convert plain password to hashed password
        userDesc.getPassVar().analyze();

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

    public CreateUserStmt translateToLegacyStmt() {
        CreateUserStmt createUserStmt = new CreateUserStmt(ifNotExist, userDesc, role, passwordOptions, comment);
        return createUserStmt;
    }
}
