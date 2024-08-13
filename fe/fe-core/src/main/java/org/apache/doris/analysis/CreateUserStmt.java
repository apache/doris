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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.authenticate.AuthenticateType;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.Role;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;

/*
 * We support the following create user stmts:
 * 1. create user user@'ip' [identified by 'password']
 *      specify the user name at a certain ip(wildcard is accepted), with optional password.
 *      the user@ip must not exist in system
 *
 * 2. create user user@['domain'] [identified by 'password']
 *      specify the user name at a certain domain, with optional password.
 *      the user@['domain'] must not exist in system
 *      the daemon thread will resolve this domain to user@'ip' format
 *
 * 3. create user user@xx [identified by 'password'] role role_name
 *      not only create the specified user, but also grant all privs of the specified role to the user.
 */
public class CreateUserStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(CreateUserStmt.class);

    private boolean ifNotExist;
    private UserIdentity userIdent;
    private PassVar passVar;
    private String role;
    private PasswordOptions passwordOptions;
    private String comment;

    private String userId;

    public CreateUserStmt() {
    }

    public CreateUserStmt(UserDesc userDesc) {
        userIdent = userDesc.getUserIdent();
        String uId = Env.getCurrentEnv().getAuth().getUserId(ClusterNamespace.getNameFromFullName(userIdent.getUser()));
        LOG.debug("create user stmt userIdent {}, userName {}, userId {}",
                userIdent, ClusterNamespace.getNameFromFullName(userIdent.getUser()), uId);
        // avoid this case "jack@'192.1'" and "jack@'192.2'", jack's uid different
        if (Strings.isNullOrEmpty(uId)) {
            userId = UUID.randomUUID().toString();
        } else {
            userId = uId;
        }
        passVar = userDesc.getPassVar();
        if (this.passwordOptions == null) {
            this.passwordOptions = PasswordOptions.UNSET_OPTION;
        }
    }

    public CreateUserStmt(boolean ifNotExist, UserDesc userDesc, String role) {
        this(ifNotExist, userDesc, role, null, null);
    }

    public CreateUserStmt(boolean ifNotExist, UserDesc userDesc, String role, PasswordOptions passwordOptions,
            String comment) {
        this.ifNotExist = ifNotExist;
        userIdent = userDesc.getUserIdent();
        String uId = Env.getCurrentEnv().getAuth().getUserId(ClusterNamespace.getNameFromFullName(userIdent.getUser()));
        LOG.debug("create user stmt by role userIdent {}, userName {}, userId {}",
                userIdent, ClusterNamespace.getNameFromFullName(userIdent.getUser()), uId);
        // avoid this case "jack@'192.1'" and "jack@'192.2'", jack's uid different
        if (Strings.isNullOrEmpty(uId)) {
            userId = UUID.randomUUID().toString();
        } else {
            userId = uId;
        }
        passVar = userDesc.getPassVar();
        this.role = role;
        this.passwordOptions = passwordOptions;
        if (this.passwordOptions == null) {
            this.passwordOptions = PasswordOptions.UNSET_OPTION;
        }
        this.comment = Strings.nullToEmpty(comment);
    }

    public boolean isIfNotExist() {
        return ifNotExist;
    }

    public String getUserId() {
        return userId;
    }

    public String getQualifiedRole() {
        return role;
    }

    public byte[] getPassword() {
        return passVar.getScrambled();
    }

    public UserIdentity getUserIdent() {
        return userIdent;
    }

    @Override
    public boolean needAuditEncryption() {
        return true;
    }

    public PasswordOptions getPasswordOptions() {
        return passwordOptions;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        if (Config.access_controller_type.equalsIgnoreCase("ranger-doris")
                && AuthenticateType.getAuthTypeConfig() == AuthenticateType.LDAP) {
            throw new AnalysisException("Create user is prohibited when Ranger and LDAP are enabled at same time.");
        }

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

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE USER ").append(userIdent);
        if (!Strings.isNullOrEmpty(passVar.getText())) {
            if (passVar.isPlain()) {
                sb.append(" IDENTIFIED BY '").append("*XXX").append("'");
            } else {
                sb.append(" IDENTIFIED BY PASSWORD '").append(passVar.getText()).append("'");
            }
        }

        if (!Strings.isNullOrEmpty(role)) {
            sb.append(" DEFAULT ROLE '").append(role).append("'");
        }
        if (passwordOptions != null) {
            sb.append(passwordOptions.toSql());
        }
        if (!StringUtils.isEmpty(comment)) {
            sb.append(" COMMENT \"").append(comment).append("\"");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }
}
