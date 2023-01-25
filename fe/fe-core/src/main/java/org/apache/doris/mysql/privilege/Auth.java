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

import org.apache.doris.analysis.AlterUserStmt;
import org.apache.doris.analysis.AlterUserStmt.OpType;
import org.apache.doris.analysis.CreateRoleStmt;
import org.apache.doris.analysis.CreateUserStmt;
import org.apache.doris.analysis.DropRoleStmt;
import org.apache.doris.analysis.DropUserStmt;
import org.apache.doris.analysis.GrantStmt;
import org.apache.doris.analysis.PasswordOptions;
import org.apache.doris.analysis.ResourcePattern;
import org.apache.doris.analysis.RevokeStmt;
import org.apache.doris.analysis.SetLdapPassVar;
import org.apache.doris.analysis.SetPassVar;
import org.apache.doris.analysis.SetUserPropertyStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AuthorizationInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.LdapConfig;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.ldap.LdapManager;
import org.apache.doris.ldap.LdapPrivsChecker;
import org.apache.doris.load.DppConfig;
import org.apache.doris.persist.AlterUserOperationLog;
import org.apache.doris.persist.LdapInfo;
import org.apache.doris.persist.PrivInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.thrift.TFetchResourceResult;
import org.apache.doris.thrift.TPrivilegeStatus;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Auth implements Writable {
    private static final Logger LOG = LogManager.getLogger(Auth.class);

    // root user's role is operator.
    // each Palo system has only one root user.
    public static final String ROOT_USER = "root";
    public static final String ADMIN_USER = "admin";
    // unknown user does not have any privilege, this is just to be compatible with old version.
    public static final String UNKNOWN_USER = "unknown";
    public static final String DEFAULT_CATALOG = InternalCatalog.INTERNAL_CATALOG_NAME;

    private RoleManager roleManager = new RoleManager();
    private UserManager userManager = new UserManager();
    private UserRoleManager userRoleManager = new UserRoleManager();
    private UserPropertyMgr propertyMgr = new UserPropertyMgr();

    private LdapInfo ldapInfo = new LdapInfo();

    private LdapManager ldapManager = new LdapManager();

    private PasswordPolicyManager passwdPolicyManager = new PasswordPolicyManager();

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    public enum PrivLevel {
        GLOBAL, CATALOG, DATABASE, TABLE, RESOURCE
    }

    public Auth() {
        initUser();
    }

    public LdapInfo getLdapInfo() {
        return ldapInfo;
    }

    public void setLdapInfo(LdapInfo ldapInfo) {
        this.ldapInfo = ldapInfo;
    }

    public LdapManager getLdapManager() {
        return ldapManager;
    }

    public PasswordPolicyManager getPasswdPolicyManager() {
        return passwdPolicyManager;
    }

    public boolean doesRoleExist(String qualifiedRole) {
        return roleManager.getRole(qualifiedRole) != null;
    }

    public void mergeRolesNoCheckName(List<String> roles, Role savedRole) {
        readLock();
        try {
            for (String roleName : roles) {
                if (doesRoleExist(roleName)) {
                    Role role = roleManager.getRole(roleName);
                    savedRole.mergeNotCheck(role);
                }
            }
        } finally {
            readUnlock();
        }
    }

    /*
     * check password, if matched, save the userIdentity in matched entry.
     * the following auth checking should use userIdentity saved in currentUser.
     */
    public void checkPassword(String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString,
            List<UserIdentity> currentUser) throws AuthenticationException {
        if ((remoteUser.equals(ROOT_USER) || remoteUser.equals(ADMIN_USER)) && remoteHost.equals("127.0.0.1")) {
            // root and admin user is allowed to login from 127.0.0.1, in case user forget password.
            if (remoteUser.equals(ROOT_USER)) {
                currentUser.add(UserIdentity.ROOT);
            } else {
                currentUser.add(UserIdentity.ADMIN);
            }
            return;
        }
        readLock();
        try {
            userManager.checkPassword(remoteUser, remoteHost, remotePasswd, randomString, currentUser);
        } finally {
            readUnlock();
        }
    }

    // For unit test only, wrapper of "void checkPlainPassword"
    public boolean checkPlainPasswordForTest(String remoteUser, String remoteHost, String remotePasswd,
            List<UserIdentity> currentUser) {
        try {
            checkPlainPassword(remoteUser, remoteHost, remotePasswd, currentUser);
            return true;
        } catch (AuthenticationException e) {
            return false;
        }
    }

    public void checkPlainPassword(String remoteUser, String remoteHost, String remotePasswd,
            List<UserIdentity> currentUser) throws AuthenticationException {
        // Check the LDAP password when the user exists in the LDAP service.
        if (ldapManager.doesUserExist(remoteUser)) {
            if (!ldapManager.checkUserPasswd(remoteUser, remotePasswd, remoteHost, currentUser)) {
                throw new AuthenticationException(ErrorCode.ERR_ACCESS_DENIED_ERROR, remoteUser + "@" + remoteHost,
                        Strings.isNullOrEmpty(remotePasswd) ? "NO" : "YES");
            }
            return;
        }
        readLock();
        try {
            userManager.checkPlainPassword(remoteUser, remoteHost, remotePasswd, currentUser);
        } finally {
            readUnlock();
        }
    }

    public boolean checkGlobalPriv(ConnectContext ctx, PrivPredicate wanted) {
        return checkGlobalPriv(ctx.getCurrentUserIdentity(), wanted);
    }

    public boolean checkGlobalPriv(UserIdentity currentUser, PrivPredicate wanted) {
        if (isLdapAuthEnabled() && LdapPrivsChecker.hasGlobalPrivFromLdap(currentUser, wanted)) {
            return true;
        }
        readLock();
        try {
            Set<String> roles = userRoleManager.getRolesByUser(currentUser);
            for (String roleName : roles) {
                if (roleManager.getRole(roleName).checkGlobalPriv(wanted)) {
                    return true;
                }
            }
            return false;
        } finally {
            readUnlock();
        }

    }

    public boolean checkCtlPriv(ConnectContext ctx, String ctl, PrivPredicate wanted) {
        return checkCtlPriv(ctx.getCurrentUserIdentity(), ctl, wanted);
    }

    public boolean checkCtlPriv(UserIdentity currentUser, String ctl, PrivPredicate wanted) {
        if (wanted.getPrivs().containsNodePriv()) {
            LOG.debug("should not check NODE priv in catalog level. user: {}, catalog: {}",
                    currentUser, ctl);
            return false;
        }
        //todo ldap （todo before change to rbac）
        readLock();
        try {
            Set<String> roles = userRoleManager.getRolesByUser(currentUser);
            for (String roleName : roles) {
                if (roleManager.getRole(roleName).checkCtlPriv(ctl, wanted)) {
                    return true;
                }
            }
            return false;
        } finally {
            readUnlock();
        }

    }

    public boolean checkDbPriv(ConnectContext ctx, String qualifiedDb, PrivPredicate wanted) {
        return checkDbPriv(ctx.getCurrentUserIdentity(), qualifiedDb, wanted);
    }

    public boolean checkDbPriv(UserIdentity currentUser, String db, PrivPredicate wanted) {
        return checkDbPriv(currentUser, DEFAULT_CATALOG, db, wanted);
    }

    public boolean checkDbPriv(ConnectContext ctx, String ctl, String db, PrivPredicate wanted) {
        return checkDbPriv(ctx.getCurrentUserIdentity(), ctl, db, wanted);
    }

    /*
     * Check if 'user'@'host' on 'db' has 'wanted' priv.
     * If the given db is null, which means it will no check if database name is matched.
     */
    public boolean checkDbPriv(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted) {
        if (isLdapAuthEnabled() && (LdapPrivsChecker.hasDbPrivFromLdap(currentUser, wanted) || LdapPrivsChecker
                .hasPrivsOfDb(currentUser, db))) {
            return true;
        }
        if (wanted.getPrivs().containsNodePriv()) {
            LOG.debug("should not check NODE priv in Database level. user: {}, db: {}",
                    currentUser, db);
            return false;
        }
        readLock();
        try {
            Set<String> roles = userRoleManager.getRolesByUser(currentUser);
            for (String roleName : roles) {
                if (roleManager.getRole(roleName).checkDbPriv(ctl, db, wanted)) {
                    return true;
                }
            }
            return false;
        } finally {
            readUnlock();
        }

    }

    public boolean checkTblPriv(ConnectContext ctx, String qualifiedCtl,
            String qualifiedDb, String tbl, PrivPredicate wanted) {
        return checkTblPriv(ctx.getCurrentUserIdentity(), qualifiedCtl, qualifiedDb, tbl, wanted);
    }

    public boolean checkTblPriv(ConnectContext ctx, String qualifiedDb, String tbl, PrivPredicate wanted) {
        return checkTblPriv(ctx, DEFAULT_CATALOG, qualifiedDb, tbl, wanted);
    }

    public boolean checkTblPriv(ConnectContext ctx, TableName tableName, PrivPredicate wanted) {
        Preconditions.checkState(tableName.isFullyQualified());
        return checkTblPriv(ctx, tableName.getDb(), tableName.getTbl(), wanted);
    }

    public boolean checkTblPriv(UserIdentity currentUser, String db, String tbl, PrivPredicate wanted) {
        return checkTblPriv(currentUser, DEFAULT_CATALOG, db, tbl, wanted);
    }

    public boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl, PrivPredicate wanted) {
        if (isLdapAuthEnabled() && LdapPrivsChecker.hasTblPrivFromLdap(currentUser, db, tbl, wanted)) {
            return true;
        }
        if (wanted.getPrivs().containsNodePriv()) {
            LOG.debug("should check NODE priv in GLOBAL level. user: {}, db: {}, tbl: {}", currentUser, db, tbl);
            return false;
        }
        readLock();
        try {
            Set<String> roles = userRoleManager.getRolesByUser(currentUser);
            for (String roleName : roles) {
                if (roleManager.getRole(roleName).checkTblPriv(ctl, db, tbl, wanted)) {
                    return true;
                }
            }
            return false;
        } finally {
            readUnlock();
        }
    }

    public boolean checkResourcePriv(ConnectContext ctx, String resourceName, PrivPredicate wanted) {
        return checkResourcePriv(ctx.getCurrentUserIdentity(), resourceName, wanted);
    }

    public boolean checkResourcePriv(UserIdentity currentUser, String resourceName, PrivPredicate wanted) {
        if (isLdapAuthEnabled() && LdapPrivsChecker.hasResourcePrivFromLdap(currentUser, resourceName, wanted)) {
            return true;
        }
        readLock();
        try {
            Set<String> roles = userRoleManager.getRolesByUser(currentUser);
            for (String roleName : roles) {
                if (roleManager.getRole(roleName).checkResourcePriv(resourceName, wanted)) {
                    return true;
                }
            }
            return false;
        } finally {
            readUnlock();
        }
    }

    public boolean checkPrivByAuthInfo(ConnectContext ctx, AuthorizationInfo authInfo, PrivPredicate wanted) {
        if (authInfo == null) {
            return false;
        }
        if (authInfo.getDbName() == null) {
            return false;
        }
        if (authInfo.getTableNameList() == null || authInfo.getTableNameList().isEmpty()) {
            return checkDbPriv(ctx, authInfo.getDbName(), wanted);
        }
        for (String tblName : authInfo.getTableNameList()) {
            if (!checkTblPriv(ConnectContext.get(), authInfo.getDbName(), tblName, wanted)) {
                return false;
            }
        }
        return true;
    }

    /*
     * Check if current user has certain privilege.
     * This method will check the given privilege levels
     */
    public boolean checkHasPriv(ConnectContext ctx, PrivPredicate priv, PrivLevel... levels) {
        if (isLdapAuthEnabled() && checkHasPrivLdap(ctx.getCurrentUserIdentity(), priv, levels)) {
            return true;
        }
        Set<String> roles = userRoleManager.getRolesByUser(ctx.getCurrentUserIdentity());
        for (String roleName : roles) {
            if (roleManager.getRole(roleName).checkHasPriv(priv, levels)) {
                return true;
            }
        }
        return false;
    }

    public boolean checkHasPrivLdap(UserIdentity currentUser, PrivPredicate priv, PrivLevel... levels) {
        for (PrivLevel privLevel : levels) {
            switch (privLevel) {
                case GLOBAL:
                    if (LdapPrivsChecker.hasGlobalPrivFromLdap(currentUser, priv)) {
                        return true;
                    }
                    break;
                case DATABASE:
                    if (LdapPrivsChecker.hasDbPrivFromLdap(currentUser, priv)) {
                        return true;
                    }
                    break;
                case TABLE:
                    if (LdapPrivsChecker.hasTblPrivFromLdap(currentUser, priv)) {
                        return true;
                    }
                    break;
                default:
                    break;
            }
        }
        return false;

    }

    // Check if LDAP authentication is enabled.
    private boolean isLdapAuthEnabled() {
        return LdapConfig.ldap_authentication_enabled;
    }

    // for test only
    public void clear() {

    }

    // create user
    public void createUser(CreateUserStmt stmt) throws DdlException {
        createUserInternal(stmt.getUserIdent(), stmt.getQualifiedRole(),
                stmt.getPassword(), stmt.isIfNotExist(), stmt.getPasswordOptions(), false);
    }

    public void replayCreateUser(PrivInfo privInfo) {
        try {
            createUserInternal(privInfo.getUserIdent(), privInfo.getRole(), privInfo.getPasswd(), false,
                    privInfo.getPasswordOptions(), true);
        } catch (DdlException e) {
            LOG.error("should not happen", e);
        }
    }

    private void createUserInternal(UserIdentity userIdent, String roleName, byte[] password,
            boolean ignoreIfExists, PasswordOptions passwordOptions, boolean isReplay) throws DdlException {
        writeLock();
        try {
            // 1. check if role exist
            Role role = null;
            if (roleName != null) {
                role = roleManager.getRole(roleName);
                if (role == null) {
                    throw new DdlException("Role: " + roleName + " does not exist");
                }
            }

            // 2. check if user already exist
            if (doesUserExist(userIdent)) {
                if (ignoreIfExists) {
                    LOG.info("user exists, ignored to create user: {}, is replay: {}", userIdent, isReplay);
                    return;
                }
                throw new DdlException("User " + userIdent + " already exist");
            }
            // 3. set password and create user
            setPasswordInternal(userIdent, password, null, false /* err on non exist */, false /* set by resolver */,
                    true /* is replay */);
            //4.create defaultRole
            Role defaultRole = roleManager.createDefaultRole(userIdent);
            // 5.create user role
            userRoleManager.addUserRole(userIdent, defaultRole.getRoleName());
            if (role != null) {
                userRoleManager.addUserRole(userIdent, roleName);
            }
            // other user properties
            propertyMgr.addUserResource(userIdent.getQualifiedUser(), false /* not system user */);

            // 5. update password policy
            passwdPolicyManager.updatePolicy(userIdent, password, passwordOptions);

            if (!isReplay) {
                PrivInfo privInfo = new PrivInfo(userIdent, null, password, roleName, passwordOptions);
                Env.getCurrentEnv().getEditLog().logCreateUser(privInfo);
            }
            LOG.info("finished to create user: {}, is replay: {}", userIdent, isReplay);
        } finally {
            writeUnlock();
        }
    }

    // drop user
    public void dropUser(DropUserStmt stmt) throws DdlException {
        dropUserInternal(stmt.getUserIdentity(), stmt.isSetIfExists(), false);
    }

    public void replayDropUser(UserIdentity userIdent) throws DdlException {
        dropUserInternal(userIdent, false, true);
    }

    private void dropUserInternal(UserIdentity userIdent, boolean ignoreIfNonExists, boolean isReplay)
            throws DdlException {
        writeLock();
        try {
            // check if user exists
            if (!doesUserExist(userIdent)) {
                if (ignoreIfNonExists) {
                    LOG.info("user non exists, ignored to drop user: {}, is replay: {}",
                            userIdent.getQualifiedUser(), isReplay);
                    return;
                }
                throw new DdlException(String.format("User `%s`@`%s` does not exist.",
                        userIdent.getQualifiedUser(), userIdent.getHost()));
            }

            // drop default role
            roleManager.removeDefaultRole(userIdent);
            //drop user role
            userRoleManager.dropUser(userIdent);
            if (userManager.getUserByName(userIdent.getQualifiedUser()).size() == 0) {
                propertyMgr.dropUser(userIdent);
            } else if (userIdent.isDomain()) {
                propertyMgr.removeDomainFromUser(userIdent);
            }
            passwdPolicyManager.dropUser(userIdent);
            userManager.removeUser(userIdent);
            if (!isReplay) {
                Env.getCurrentEnv().getEditLog().logNewDropUser(userIdent);
            }
            LOG.info("finished to drop user: {}, is replay: {}", userIdent.getQualifiedUser(), isReplay);
        } finally {
            writeUnlock();
        }
    }

    // grant
    public void grant(GrantStmt stmt) throws DdlException {
        PrivBitSet privs = PrivBitSet.of(stmt.getPrivileges());
        if (stmt.getTblPattern() != null) {
            grantInternal(stmt.getUserIdent(), stmt.getQualifiedRole(), stmt.getTblPattern(), privs,
                    true /* err on non exist */, false /* not replay */);
        } else {
            grantInternal(stmt.getUserIdent(), stmt.getQualifiedRole(), stmt.getResourcePattern(), privs,
                    true /* err on non exist */, false /* not replay */);
        }
    }

    public void replayGrant(PrivInfo privInfo) {
        try {
            if (privInfo.getTblPattern() != null) {
                grantInternal(privInfo.getUserIdent(), privInfo.getRole(),
                        privInfo.getTblPattern(), privInfo.getPrivs(),
                        true /* err on non exist */, true /* is replay */);
            } else {
                grantInternal(privInfo.getUserIdent(), privInfo.getRole(),
                        privInfo.getResourcePattern(), privInfo.getPrivs(),
                        true /* err on non exist */, true /* is replay */);
            }
        } catch (DdlException e) {
            LOG.error("should not happen", e);
        }
    }

    // grant for TablePattern
    //if no role,role is default role of userIdent
    private void grantInternal(UserIdentity userIdent, String role, TablePattern tblPattern,
            PrivBitSet privs, boolean errOnNonExist, boolean isReplay)
            throws DdlException {
        writeLock();
        try {
            if (role == null) {
                role = roleManager.getUserDefaultRoleName(userIdent);
            }
            Role newRole = new Role(role, tblPattern, privs);
            roleManager.addRole(newRole, false /* err on exist */);
            if (!isReplay) {
                PrivInfo info = new PrivInfo(userIdent, tblPattern, privs, null, role);
                Env.getCurrentEnv().getEditLog().logGrantPriv(info);
            }
            LOG.info("finished to grant privilege. is replay: {}", isReplay);
        } finally {
            writeUnlock();
        }
    }

    // grant for ResourcePattern
    private void grantInternal(UserIdentity userIdent, String role, ResourcePattern resourcePattern, PrivBitSet privs,
            boolean errOnNonExist, boolean isReplay) throws DdlException {
        writeLock();
        try {
            if (role == null) {
                role = roleManager.getUserDefaultRoleName(userIdent);
            }

            // grant privs to role, role must exist
            Role newRole = new Role(role, resourcePattern, privs);
            roleManager.addRole(newRole, false /* err on exist */);

            if (!isReplay) {
                PrivInfo info = new PrivInfo(userIdent, resourcePattern, privs, null, role);
                Env.getCurrentEnv().getEditLog().logGrantPriv(info);
            }
            LOG.info("finished to grant resource privilege. is replay: {}", isReplay);
        } finally {
            writeUnlock();
        }
    }


    // return true if user ident exist
    private boolean doesUserExist(UserIdentity userIdent) {
        if (userIdent.isDomain()) {
            return propertyMgr.doesUserExist(userIdent);
        } else {
            return userManager.userIdentityExist(userIdent);
        }
    }

    // Check whether the user exists. If the user exists, return UserIdentity, otherwise return null.
    public UserIdentity getCurrentUserIdentity(UserIdentity userIdent) {
        readLock();
        try {
            if (userManager.userIdentityExist(userIdent)) {
                return userIdent;
            }
            return null;
        } finally {
            readUnlock();
        }
    }

    // revoke
    public void revoke(RevokeStmt stmt) throws DdlException {
        PrivBitSet privs = PrivBitSet.of(stmt.getPrivileges());
        if (stmt.getTblPattern() != null) {
            revokeInternal(stmt.getUserIdent(), stmt.getQualifiedRole(), stmt.getTblPattern(), privs,
                    true /* err on non exist */, false /* is replay */);
        } else {
            revokeInternal(stmt.getUserIdent(), stmt.getQualifiedRole(), stmt.getResourcePattern(), privs,
                    true /* err on non exist */, false /* is replay */);
        }
    }

    public void replayRevoke(PrivInfo info) {
        try {
            if (info.getTblPattern() != null) {
                revokeInternal(info.getUserIdent(), info.getRole(), info.getTblPattern(), info.getPrivs(),
                        true /* err on non exist */, true /* is replay */);
            } else {
                revokeInternal(info.getUserIdent(), info.getRole(), info.getResourcePattern(), info.getPrivs(),
                        true /* err on non exist */, true /* is replay */);
            }
        } catch (DdlException e) {
            LOG.error("should not happened", e);
        }
    }

    private void revokeInternal(UserIdentity userIdent, String role, TablePattern tblPattern,
            PrivBitSet privs, boolean errOnNonExist, boolean isReplay) throws DdlException {
        writeLock();
        try {
            if (role == null) {
                role = roleManager.getUserDefaultRoleName(userIdent);
            }
            // revoke privs from role
            roleManager.revokePrivs(role, tblPattern, privs, errOnNonExist);

            if (!isReplay) {
                PrivInfo info = new PrivInfo(userIdent, tblPattern, privs, null, role);
                Env.getCurrentEnv().getEditLog().logRevokePriv(info);
            }
            LOG.info("finished to revoke privilege. is replay: {}", isReplay);
        } finally {
            writeUnlock();
        }
    }

    private void revokeInternal(UserIdentity userIdent, String role, ResourcePattern resourcePattern,
            PrivBitSet privs, boolean errOnNonExist, boolean isReplay) throws DdlException {
        writeLock();
        try {
            if (role == null) {
                role = roleManager.getUserDefaultRoleName(userIdent);
            }

            // revoke privs from role
            roleManager.revokePrivs(role, resourcePattern, privs, errOnNonExist);

            if (!isReplay) {
                PrivInfo info = new PrivInfo(userIdent, resourcePattern, privs, null, role);
                Env.getCurrentEnv().getEditLog().logRevokePriv(info);
            }
            LOG.info("finished to revoke privilege. is replay: {}", isReplay);
        } finally {
            writeUnlock();
        }
    }

    // set password
    public void setPassword(SetPassVar stmt) throws DdlException {
        setPasswordInternal(stmt.getUserIdent(), stmt.getPassword(), null, true /* err on non exist */,
                false /* set by resolver */, false);
    }

    public void replaySetPassword(PrivInfo info) {
        try {
            setPasswordInternal(info.getUserIdent(), info.getPasswd(), null, true /* err on non exist */,
                    false /* set by resolver */, true);
        } catch (DdlException e) {
            LOG.error("should not happened", e);
        }
    }

    public void setPasswordInternal(UserIdentity userIdent, byte[] password, UserIdentity domainUserIdent,
            boolean errOnNonExist, boolean setByResolver, boolean isReplay) throws DdlException {
        Preconditions.checkArgument(!setByResolver || domainUserIdent != null, setByResolver + ", " + domainUserIdent);
        writeLock();
        try {
            if (!isReplay) {
                if (!passwdPolicyManager.checkPasswordHistory(userIdent, password)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CREDENTIALS_CONTRADICT_TO_HISTORY,
                            userIdent.getQualifiedUser(), userIdent.getHost());
                }
            }
            if (userIdent.isDomain()) {
                // throw exception if this user already contains this domain
                propertyMgr.setPasswordForDomain(userIdent, password,
                        true /* err on exist */, errOnNonExist /* err on non exist */);
            } else {
                try {
                    userManager.createUser(userIdent, password, domainUserIdent, setByResolver);
                } catch (AnalysisException e) {
                    throw new DdlException(e.getMessage());
                }
            }
            if (password != null) {
                // save password to password history
                passwdPolicyManager.updatePassword(userIdent, password);
            }

            if (!isReplay) {
                PrivInfo info = new PrivInfo(userIdent, null, password, null, null);
                Env.getCurrentEnv().getEditLog().logSetPassword(info);
            }
        } finally {
            writeUnlock();
        }
        LOG.info("finished to set password for {}. is replay: {}", userIdent, isReplay);
    }

    // set ldap admin password.
    public void setLdapPassword(SetLdapPassVar stmt) {
        ldapInfo = new LdapInfo(stmt.getLdapPassword());
        Env.getCurrentEnv().getEditLog().logSetLdapPassword(ldapInfo);
        LOG.info("finished to set ldap password.");
    }

    public void replaySetLdapPassword(LdapInfo info) {
        ldapInfo = info;
        LOG.debug("finish replaying ldap admin password.");
    }

    // create role
    public void createRole(CreateRoleStmt stmt) throws DdlException {
        createRoleInternal(stmt.getQualifiedRole(), stmt.isSetIfNotExists(), false);
    }

    public void replayCreateRole(PrivInfo info) {
        try {
            createRoleInternal(info.getRole(), false, true);
        } catch (DdlException e) {
            LOG.error("should not happened", e);
        }
    }

    private void createRoleInternal(String role, boolean ignoreIfExists, boolean isReplay) throws DdlException {
        Role emptyPrivsRole = new Role(role);
        writeLock();
        try {
            if (ignoreIfExists && roleManager.getRole(role) != null) {
                LOG.info("role exists, ignored to create role: {}, is replay: {}", role, isReplay);
                return;
            }

            roleManager.addRole(emptyPrivsRole, true /* err on exist */);

            if (!isReplay) {
                PrivInfo info = new PrivInfo(null, null, null, role, null);
                Env.getCurrentEnv().getEditLog().logCreateRole(info);
            }
        } finally {
            writeUnlock();
        }
        LOG.info("finished to create role: {}, is replay: {}", role, isReplay);
    }

    // drop role
    public void dropRole(DropRoleStmt stmt) throws DdlException {
        dropRoleInternal(stmt.getQualifiedRole(), stmt.isSetIfExists(), false);
    }

    public void replayDropRole(PrivInfo info) {
        try {
            dropRoleInternal(info.getRole(), false, true);
        } catch (DdlException e) {
            LOG.error("should not happened", e);
        }
    }

    private void dropRoleInternal(String role, boolean ignoreIfNonExists, boolean isReplay) throws DdlException {
        writeLock();
        try {
            if (ignoreIfNonExists && roleManager.getRole(role) == null) {
                LOG.info("role non exists, ignored to drop role: {}, is replay: {}", role, isReplay);
                return;
            }

            roleManager.dropRole(role, true /* err on non exist */);
            userRoleManager.dropRole(role);
            if (!isReplay) {
                PrivInfo info = new PrivInfo(null, null, null, role, null);
                Env.getCurrentEnv().getEditLog().logDropRole(info);
            }
        } finally {
            writeUnlock();
        }
        LOG.info("finished to drop role: {}, is replay: {}", role, isReplay);
    }

    // update user property
    public void updateUserProperty(SetUserPropertyStmt stmt) throws UserException {
        List<Pair<String, String>> properties = stmt.getPropertyPairList();
        updateUserPropertyInternal(stmt.getUser(), properties, false /* is replay */);
    }

    public void replayUpdateUserProperty(UserPropertyInfo propInfo) throws UserException {
        updateUserPropertyInternal(propInfo.getUser(), propInfo.getProperties(), true /* is replay */);
    }

    public void updateUserPropertyInternal(String user, List<Pair<String, String>> properties, boolean isReplay)
            throws UserException {
        writeLock();
        try {
            propertyMgr.updateUserProperty(user, properties);
            if (!isReplay) {
                UserPropertyInfo propertyInfo = new UserPropertyInfo(user, properties);
                Env.getCurrentEnv().getEditLog().logUpdateUserProperty(propertyInfo);
            }
            LOG.info("finished to set properties for user: {}", user);
        } finally {
            writeUnlock();
        }
    }

    public long getMaxConn(String qualifiedUser) {
        readLock();
        try {
            return propertyMgr.getMaxConn(qualifiedUser);
        } finally {
            readUnlock();
        }
    }

    public long getQueryTimeout(String qualifiedUser) {
        readLock();
        try {
            return propertyMgr.getQueryTimeout(qualifiedUser);
        } finally {
            readUnlock();
        }
    }

    public long getMaxQueryInstances(String qualifiedUser) {
        readLock();
        try {
            return propertyMgr.getMaxQueryInstances(qualifiedUser);
        } finally {
            readUnlock();
        }
    }

    public String[] getSqlBlockRules(String qualifiedUser) {
        readLock();
        try {
            return propertyMgr.getSqlBlockRules(qualifiedUser);
        } finally {
            readUnlock();
        }
    }

    public int getCpuResourceLimit(String qualifiedUser) {
        readLock();
        try {
            return propertyMgr.getCpuResourceLimit(qualifiedUser);
        } finally {
            readUnlock();
        }
    }

    public Set<Tag> getResourceTags(String qualifiedUser) {
        readLock();
        try {
            return propertyMgr.getResourceTags(qualifiedUser);
        } finally {
            readUnlock();
        }
    }

    public long getExecMemLimit(String qualifiedUser) {
        readLock();
        try {
            return propertyMgr.getExecMemLimit(qualifiedUser);
        } finally {
            readUnlock();
        }
    }

    public void getAllDomains(Set<String> allDomains) {
        readLock();
        try {
            propertyMgr.getAllDomains(allDomains);
        } finally {
            readUnlock();
        }
    }

    //todo implement
    // refresh all priv entries set by domain resolver.
    // 1. delete all priv entries in user priv table which are set by domain resolver previously.
    // 2. add priv entries by new resolving IPs
    public void refreshUserPrivEntriesByResovledIPs(Map<String, Set<String>> resolvedIPsMap) {
        //        writeLock();
        //        try {
        //            // 1. delete all previously set entries
        //            userPrivTable.clearEntriesSetByResolver();
        //            // 2. add new entries
        //            propertyMgr.addUserPrivEntriesByResolvedIPs(resolvedIPsMap);
        //        } finally {
        //            writeUnlock();
        //        }
        userManager.clearEntriesSetByResolver();
        propertyMgr.addUserPrivEntriesByResolvedIPs(resolvedIPsMap);
    }

    // return the auth info of specified user, or infos of all users, if user is not specified.
    // the returned columns are defined in AuthProcDir
    // the specified user identity should be the identity created by CREATE USER, same as result of
    // SELECT CURRENT_USER();
    public List<List<String>> getAuthInfo(UserIdentity specifiedUserIdent) {
        try {
            //merge all auth of roles
        } finally {
            readUnlock();
        }
        return null;
    }

    private void getUserAuthInfo(List<List<String>> userAuthInfos, UserIdentity userIdent) {
        //todo merge auth of roles
    }

    private Set<UserIdentity> getAllUserIdents(boolean includeEntrySetByResolver) {
        //        return userManager.getUsers();
        return null;
    }

    public List<List<String>> getUserProperties(String qualifiedUser) {
        readLock();
        try {
            return propertyMgr.fetchUserProperty(qualifiedUser);
        } catch (AnalysisException e) {
            return Lists.newArrayList();
        } finally {
            readUnlock();
        }
    }

    public void dropUserOfCluster(String clusterName, boolean isReplay) throws DdlException {
        writeLock();
        try {
            Set<UserIdentity> allUserIdents = getAllUserIdents(true);
            for (UserIdentity userIdent : allUserIdents) {
                if (userIdent.getQualifiedUser().startsWith(clusterName)) {
                    dropUserInternal(userIdent, false, isReplay);
                }
            }
        } finally {
            writeUnlock();
        }
    }

    public Pair<String, DppConfig> getLoadClusterInfo(String qualifiedUser, String cluster) throws DdlException {
        readLock();
        try {
            return propertyMgr.getLoadClusterInfo(qualifiedUser, cluster);
        } finally {
            readUnlock();
        }
    }

    // user can enter a cluster, if it has any privs of database or table in this cluster.
    public boolean checkCanEnterCluster(ConnectContext ctx, String clusterName) {
        readLock();
        try {
            // TODO: 2023/1/16 roles 

            return false;
        } finally {
            readUnlock();
        }
    }

    private void initUser() {
        try {
            UserIdentity rootUser = new UserIdentity(ROOT_USER, "%");
            rootUser.setIsAnalyzed();
            createUserInternal(rootUser, Role.OPERATOR_ROLE, new byte[0],
                    false /* ignore if exists */, PasswordOptions.UNSET_OPTION, true /* is replay */);
            UserIdentity adminUser = new UserIdentity(ADMIN_USER, "%");
            adminUser.setIsAnalyzed();
            createUserInternal(adminUser, Role.ADMIN_ROLE, new byte[0],
                    false /* ignore if exists */, PasswordOptions.UNSET_OPTION, true /* is replay */);
        } catch (DdlException e) {
            LOG.error("should not happened", e);
        }
    }

    public TFetchResourceResult toResourceThrift() {
        readLock();
        try {
            return propertyMgr.toResourceThrift();
        } finally {
            readUnlock();
        }
    }

    public List<List<String>> getRoleInfo() {
        readLock();
        try {
            List<List<String>> results = Lists.newArrayList();
            roleManager.getRoleInfo(results);
            return results;
        } finally {
            readUnlock();
        }
    }

    // Used for creating table_privileges table in information_schema.
    public void getTablePrivStatus(List<TPrivilegeStatus> tblPrivResult, UserIdentity currentUser) {
        readLock();
        try {
            // TODO: merge roles
        } finally {
            readUnlock();
        }
    }

    // Used for creating schema_privileges table in information_schema.
    public void getSchemaPrivStatus(List<TPrivilegeStatus> dbPrivResult, UserIdentity currentUser) {
        readLock();
        try {
            // TODO: merge roles
        } finally {
            readUnlock();
        }
    }

    // Used for creating user_privileges table in information_schema.
    public void getGlobalPrivStatus(List<TPrivilegeStatus> userPrivResult, UserIdentity currentUser) {
        readLock();
        try {
            // TODO: merge roles
        } finally {
            readUnlock();
        }
    }

    public List<List<String>> getPasswdPolicyInfo(UserIdentity userIdent) {
        return passwdPolicyManager.getPolicyInfo(userIdent);
    }

    public void alterUser(AlterUserStmt stmt) throws DdlException {
        alterUserInternal(stmt.isIfExist(), stmt.getOpType(), stmt.getUserIdent(), stmt.getPassword(), stmt.getRole(),
                stmt.getPasswordOptions(), false);
    }

    public void replayAlterUser(AlterUserOperationLog log) {
        try {
            alterUserInternal(true, log.getOp(), log.getUserIdent(), log.getPassword(), log.getRole(),
                    log.getPasswordOptions(), true);
        } catch (DdlException e) {
            LOG.error("should not happen", e);
        }
    }

    private void alterUserInternal(boolean ifExists, OpType opType, UserIdentity userIdent, byte[] password,
            String role, PasswordOptions passwordOptions, boolean isReplay) throws DdlException {
        writeLock();
        try {
            if (!doesUserExist(userIdent)) {
                if (ifExists) {
                    return;
                }
                throw new DdlException("User " + userIdent + " does not exist");
            }
            switch (opType) {
                case SET_PASSWORD:
                    setPasswordInternal(userIdent, password, null, false, false, isReplay);
                    break;
                case SET_ROLE:
                    setRoleToUser(userIdent, role);
                    break;
                case SET_PASSWORD_POLICY:
                    passwdPolicyManager.updatePolicy(userIdent, null, passwordOptions);
                    break;
                case UNLOCK_ACCOUNT:
                    passwdPolicyManager.unlockUser(userIdent);
                    break;
                default:
                    throw new DdlException("Unknown alter user operation type: " + opType.name());
            }
            if (opType != OpType.SET_PASSWORD && !isReplay) {
                // For SET_PASSWORD:
                //      the edit log is wrote in "setPasswordInternal"
                AlterUserOperationLog log = new AlterUserOperationLog(opType, userIdent, password, role,
                        passwordOptions);
                Env.getCurrentEnv().getEditLog().logAlterUser(log);
            }
        } finally {
            writeUnlock();
        }
    }

    //    todo need think if alter user can change default role
    private void setRoleToUser(UserIdentity userIdent, String role) throws DdlException {
    }

    public static Auth read(DataInput in) throws IOException {
        Auth auth = new Auth();
        auth.readFields(in);
        return auth;
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }

    public void readFields(DataInput in) throws IOException {

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        return sb.toString();
    }
}
