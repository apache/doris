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
import org.apache.doris.analysis.RefreshLdapStmt;
import org.apache.doris.analysis.ResourcePattern;
import org.apache.doris.analysis.RevokeStmt;
import org.apache.doris.analysis.SetLdapPassVar;
import org.apache.doris.analysis.SetPassVar;
import org.apache.doris.analysis.SetUserPropertyStmt;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.analysis.WorkloadGroupPattern;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.AuthorizationException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.LdapConfig;
import org.apache.doris.common.Pair;
import org.apache.doris.common.PatternMatcherException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.ldap.LdapManager;
import org.apache.doris.ldap.LdapUserInfo;
import org.apache.doris.mysql.MysqlPassword;
import org.apache.doris.persist.AlterUserOperationLog;
import org.apache.doris.persist.LdapInfo;
import org.apache.doris.persist.PrivInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.thrift.TPrivilegeStatus;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class Auth implements Writable {
    private static final Logger LOG = LogManager.getLogger(Auth.class);

    // root user's role is operator.
    // each Doris system has only one root user.
    public static final String ROOT_USER = "root";
    public static final String ADMIN_USER = "admin";
    // unknown user does not have any privilege, this is just to be compatible with old version.
    public static final String UNKNOWN_USER = "unknown";
    public static final String DEFAULT_CATALOG = InternalCatalog.INTERNAL_CATALOG_NAME;

    //There is no concurrency control logic inside roleManager,userManager,userRoleManage and rpropertyMgr,
    // and it is completely managed by Auth.
    // Therefore, their methods cannot be directly called outside, and should be called indirectly through Auth.
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
        GLOBAL, CATALOG, DATABASE, TABLE, RESOURCE, WORKLOAD_GROUP
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

    public void mergeRolesNoCheckName(List<String> roles, Role savedRole) throws DdlException {
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

    public Role getRoleByName(String roleName) {
        return roleManager.getRole(roleName);
    }

    /*
     * check password, if matched, save the userIdentity in matched entry.
     * the following auth checking should use userIdentity saved in currentUser.
     */
    public void checkPassword(String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString,
            List<UserIdentity> currentUser) throws AuthenticationException {
        if ((ROOT_USER.equals(remoteUser) || ADMIN_USER.equals(remoteUser)) && Config.skip_localhost_auth_check
                && "127.0.0.1".equals(remoteHost)) {
            // in case user forget password.
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

    public Set<String> getRolesByUser(UserIdentity user, boolean showUserDefaultRole) {
        readLock();
        try {
            return userRoleManager.getRolesByUser(user, showUserDefaultRole);
        } finally {
            readUnlock();
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
        } else {
            readLock();
            try {
                userManager.checkPlainPassword(remoteUser, remoteHost, remotePasswd, currentUser);
            } finally {
                readUnlock();
            }
        }
    }

    public Set<Role> getRolesByUserWithLdap(UserIdentity userIdentity) {
        Set<Role> roles = Sets.newHashSet();
        Set<String> roleNames = userRoleManager.getRolesByUser(userIdentity);
        for (String roleName : roleNames) {
            roles.add(roleManager.getRole(roleName));
        }
        if (isLdapAuthEnabled()) {
            Set<Role> ldapRoles = ldapManager.getUserRoles(userIdentity.getQualifiedUser());
            if (!CollectionUtils.isEmpty(ldapRoles)) {
                roles.addAll(ldapRoles);
            }
        }
        return roles;
    }

    public List<UserIdentity> getUserIdentityForLdap(String remoteUser, String remoteHost) {
        return userManager.getUserIdentityUncheckPasswd(remoteUser, remoteHost);
    }

    // ==== Global ====
    public boolean checkGlobalPriv(UserIdentity currentUser, PrivPredicate wanted) {
        readLock();
        try {
            Set<Role> roles = getRolesByUserWithLdap(currentUser);
            for (Role role : roles) {
                if (role.checkGlobalPriv(wanted)) {
                    return true;
                }
            }
            return false;
        } finally {
            readUnlock();
        }
    }

    // ==== Catalog ====
    public boolean checkCtlPriv(UserIdentity currentUser, String ctl, PrivPredicate wanted) {
        if (wanted.getPrivs().containsNodePriv()) {
            LOG.debug("should not check NODE priv in catalog level. user: {}, catalog: {}",
                    currentUser, ctl);
            return false;
        }
        readLock();
        try {
            Set<Role> roles = getRolesByUserWithLdap(currentUser);
            for (Role role : roles) {
                if (role.checkCtlPriv(ctl, wanted)) {
                    return true;
                }
            }
            return false;
        } finally {
            readUnlock();
        }
    }

    // ==== Database ====
    public boolean checkDbPriv(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted) {
        if (wanted.getPrivs().containsNodePriv()) {
            LOG.debug("should not check NODE priv in Database level. user: {}, db: {}",
                    currentUser, db);
            return false;
        }
        readLock();
        try {
            Set<Role> roles = getRolesByUserWithLdap(currentUser);
            for (Role role : roles) {
                if (role.checkDbPriv(ctl, db, wanted)) {
                    return true;
                }
            }
            return false;
        } finally {
            readUnlock();
        }

    }

    // ==== Table ====
    public boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl, PrivPredicate wanted) {
        if (wanted.getPrivs().containsNodePriv()) {
            LOG.debug("should check NODE priv in GLOBAL level. user: {}, db: {}, tbl: {}", currentUser, db, tbl);
            return false;
        }
        readLock();
        try {
            Set<Role> roles = getRolesByUserWithLdap(currentUser);
            for (Role role : roles) {
                if (role.checkTblPriv(ctl, db, tbl, wanted)) {
                    return true;
                }
            }
            return false;
        } finally {
            readUnlock();
        }
    }

    // ==== Column ====
    // The reason why this method throws an exception instead of returning a boolean is to
    // indicate which col does not have permission
    public void checkColsPriv(UserIdentity currentUser, String ctl, String db, String tbl, Set<String> cols,
            PrivPredicate wanted) throws AuthorizationException {
        Set<Role> roles = getRolesByUserWithLdap(currentUser);
        for (String col : cols) {
            if (!checkColPriv(ctl, db, tbl, col, wanted, roles)) {
                throw new AuthorizationException(String.format(
                        "Permission denied: user [%s] does not have privilege for [%s] command on [%s].[%s].[%s].[%s]",
                        currentUser, wanted, ctl, db, tbl, col));
            }
        }
    }

    private boolean checkColPriv(String ctl, String db, String tbl,
            String col, PrivPredicate wanted, Set<Role> roles) {
        for (Role role : roles) {
            if (role.checkColPriv(ctl, db, tbl, col, wanted)) {
                return true;
            }
        }
        return false;
    }

    // ==== Resource ====
    public boolean checkResourcePriv(UserIdentity currentUser, String resourceName, PrivPredicate wanted) {
        readLock();
        try {
            Set<Role> roles = getRolesByUserWithLdap(currentUser);
            for (Role role : roles) {
                if (role.checkResourcePriv(resourceName, wanted)) {
                    return true;
                }
            }
            return false;
        } finally {
            readUnlock();
        }
    }

    // ==== Workload Group ====
    public boolean checkWorkloadGroupPriv(UserIdentity currentUser, String workloadGroupName, PrivPredicate wanted) {
        readLock();
        try {
            Set<Role> roles = getRolesByUserWithLdap(currentUser);
            for (Role role : roles) {
                if (role.checkWorkloadGroupPriv(workloadGroupName, wanted)) {
                    return true;
                }
            }
            return false;
        } finally {
            readUnlock();
        }
    }

    // ==== Other ====
    /*
     * Check if current user has certain privilege.
     * This method will check the given privilege levels
     */
    public boolean checkHasPriv(ConnectContext ctx, PrivPredicate priv, PrivLevel... levels) {
        readLock();
        try {
            Set<Role> roles = getRolesByUserWithLdap(ctx.getCurrentUserIdentity());
            for (Role role : roles) {
                if (role.checkHasPriv(priv, levels)) {
                    return true;
                }
            }
            return false;
        } finally {
            readUnlock();
        }
    }

    // Check if LDAP authentication is enabled.
    private boolean isLdapAuthEnabled() {
        return LdapConfig.ldap_authentication_enabled;
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
            // check if role exist
            Role role = null;
            if (roleName != null) {
                role = roleManager.getRole(roleName);
                if (role == null) {
                    throw new DdlException("Role: " + roleName + " does not exist");
                }
            }

            // check if user already exist
            if (doesUserExist(userIdent)) {
                if (ignoreIfExists) {
                    LOG.info("user exists, ignored to create user: {}, is replay: {}", userIdent, isReplay);
                    return;
                }
                throw new DdlException("User " + userIdent + " already exist");
            }

            // create user
            try {
                // we should not throw AnalysisException at here,so transfer it
                userManager.createUser(userIdent, password, null, false);
            } catch (PatternMatcherException e) {
                throw new DdlException("create user failed,", e);
            }
            if (password != null) {
                // save password to password history
                passwdPolicyManager.updatePassword(userIdent, password);
            }
            //4.create defaultRole
            Role defaultRole = roleManager.createDefaultRole(userIdent);
            // 5.create user role
            userRoleManager.addUserRole(userIdent, defaultRole.getRoleName());
            if (role != null) {
                userRoleManager.addUserRole(userIdent, roleName);
            }
            // other user properties
            propertyMgr.addUserResource(userIdent.getQualifiedUser());

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
            passwdPolicyManager.dropUser(userIdent);
            userManager.removeUser(userIdent);
            if (CollectionUtils.isEmpty(userManager.getUserByName(userIdent.getQualifiedUser()))) {
                propertyMgr.dropUser(userIdent);
            }

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
        if (stmt.getTblPattern() != null) {
            PrivBitSet privs = PrivBitSet.of(stmt.getPrivileges());
            grantInternal(stmt.getUserIdent(), stmt.getQualifiedRole(), stmt.getTblPattern(), privs,
                    stmt.getColPrivileges(), true /* err on non exist */, false /* not replay */);
        } else if (stmt.getResourcePattern() != null) {
            PrivBitSet privs = PrivBitSet.of(stmt.getPrivileges());
            grantInternal(stmt.getUserIdent(), stmt.getQualifiedRole(), stmt.getResourcePattern(), privs,
                    true /* err on non exist */, false /* not replay */);
        } else if (stmt.getWorkloadGroupPattern() != null) {
            PrivBitSet privs = PrivBitSet.of(stmt.getPrivileges());
            grantInternal(stmt.getUserIdent(), stmt.getQualifiedRole(), stmt.getWorkloadGroupPattern(), privs,
                    true /* err on non exist */, false /* not replay */);
        } else {
            grantInternal(stmt.getUserIdent(), stmt.getRoles(), false);
        }
    }

    public void replayGrant(PrivInfo privInfo) {
        try {
            if (privInfo.getTblPattern() != null) {
                grantInternal(privInfo.getUserIdent(), privInfo.getRole(),
                        privInfo.getTblPattern(), privInfo.getPrivs(), privInfo.getColPrivileges(),
                        true /* err on non exist */, true /* is replay */);
            } else if (privInfo.getResourcePattern() != null) {
                grantInternal(privInfo.getUserIdent(), privInfo.getRole(),
                        privInfo.getResourcePattern(), privInfo.getPrivs(),
                        true /* err on non exist */, true /* is replay */);
            } else if (privInfo.getWorkloadGroupPattern() != null) {
                grantInternal(privInfo.getUserIdent(), privInfo.getRole(),
                        privInfo.getWorkloadGroupPattern(), privInfo.getPrivs(),
                        true /* err on non exist */, true /* is replay */);
            } else {
                grantInternal(privInfo.getUserIdent(), privInfo.getRoles(), true);
            }
        } catch (DdlException e) {
            LOG.error("should not happen", e);
        }
    }

    // grant for TablePattern
    //if no role,role is default role of userIdent
    private void grantInternal(UserIdentity userIdent, String role, TablePattern tblPattern, PrivBitSet privs,
            Map<ColPrivilegeKey, Set<String>> colPrivileges, boolean errOnNonExist, boolean isReplay)
            throws DdlException {
        writeLock();
        try {
            if (role == null) {
                if (!doesUserExist(userIdent)) {
                    throw new DdlException("user " + userIdent + " does not exist");
                }
                role = roleManager.getUserDefaultRoleName(userIdent);
            }
            Role newRole = new Role(role, tblPattern, privs, colPrivileges);
            roleManager.addOrMergeRole(newRole, false /* err on exist */);
            if (!isReplay) {
                PrivInfo info = new PrivInfo(userIdent, tblPattern, privs, null, role, colPrivileges);
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

            Role newRole = new Role(role, resourcePattern, privs);
            roleManager.addOrMergeRole(newRole, false /* err on exist */);

            if (!isReplay) {
                PrivInfo info = new PrivInfo(userIdent, resourcePattern, privs, null, role);
                Env.getCurrentEnv().getEditLog().logGrantPriv(info);
            }
            LOG.info("finished to grant resource privilege. is replay: {}", isReplay);
        } finally {
            writeUnlock();
        }
    }

    private void grantInternal(UserIdentity userIdent, String role, WorkloadGroupPattern workloadGroupPattern,
            PrivBitSet privs, boolean errOnNonExist, boolean isReplay) throws DdlException {
        writeLock();
        try {
            if (role == null) {
                role = roleManager.getUserDefaultRoleName(userIdent);
            }

            Role newRole = new Role(role, workloadGroupPattern, privs);
            roleManager.addOrMergeRole(newRole, false /* err on exist */);

            if (!isReplay) {
                PrivInfo info = new PrivInfo(userIdent, workloadGroupPattern, privs, null, role);
                Env.getCurrentEnv().getEditLog().logGrantPriv(info);
            }
            LOG.info("finished to grant workload group privilege. is replay: {}", isReplay);
        } finally {
            writeUnlock();
        }
    }

    // grant for roles
    private void grantInternal(UserIdentity userIdent, List<String> roles, boolean isReplay) throws DdlException {
        writeLock();
        try {
            if (userManager.getUserByUserIdentity(userIdent) == null) {
                throw new DdlException("user: " + userIdent + " does not exist");
            }
            //roles must exist
            for (String roleName : roles) {
                if (roleManager.getRole(roleName) == null) {
                    throw new DdlException("role:" + roleName + " does not exist");
                }
            }
            userRoleManager.addUserRoles(userIdent, roles);
            if (!isReplay) {
                PrivInfo info = new PrivInfo(userIdent, roles);
                Env.getCurrentEnv().getEditLog().logGrantPriv(info);
            }
            LOG.info("finished to grant role privilege. is replay: {}", isReplay);
        } finally {
            writeUnlock();
        }
    }


    // return true if user ident exist
    public boolean doesUserExist(UserIdentity userIdent) {
        return userManager.userIdentityExist(userIdent, false);
    }

    // Check whether the user exists. If the user exists, return UserIdentity, otherwise return null.
    public UserIdentity getCurrentUserIdentity(UserIdentity userIdent) {
        readLock();
        try {
            if (doesUserExist(userIdent)) {
                return userIdent;
            }
            return null;
        } finally {
            readUnlock();
        }
    }

    // revoke
    public void revoke(RevokeStmt stmt) throws DdlException {
        if (stmt.getTblPattern() != null) {
            PrivBitSet privs = PrivBitSet.of(stmt.getPrivileges());
            revokeInternal(stmt.getUserIdent(), stmt.getQualifiedRole(), stmt.getTblPattern(), privs,
                    stmt.getColPrivileges(), true /* err on non exist */, false /* is replay */);
        } else if (stmt.getResourcePattern() != null) {
            PrivBitSet privs = PrivBitSet.of(stmt.getPrivileges());
            revokeInternal(stmt.getUserIdent(), stmt.getQualifiedRole(), stmt.getResourcePattern(), privs,
                    true /* err on non exist */, false /* is replay */);
        } else if (stmt.getWorkloadGroupPattern() != null) {
            PrivBitSet privs = PrivBitSet.of(stmt.getPrivileges());
            revokeInternal(stmt.getUserIdent(), stmt.getQualifiedRole(), stmt.getWorkloadGroupPattern(), privs,
                    true /* err on non exist */, false /* is replay */);
        } else {
            revokeInternal(stmt.getUserIdent(), stmt.getRoles(), false);
        }
    }

    public void replayRevoke(PrivInfo info) {
        try {
            if (info.getTblPattern() != null) {
                revokeInternal(info.getUserIdent(), info.getRole(), info.getTblPattern(), info.getPrivs(),
                        info.getColPrivileges(), true /* err on non exist */, true /* is replay */);
            } else if (info.getResourcePattern() != null) {
                revokeInternal(info.getUserIdent(), info.getRole(), info.getResourcePattern(), info.getPrivs(),
                        true /* err on non exist */, true /* is replay */);
            } else if (info.getWorkloadGroupPattern() != null) {
                revokeInternal(info.getUserIdent(), info.getRole(), info.getWorkloadGroupPattern(), info.getPrivs(),
                        true /* err on non exist */, true /* is replay */);
            } else {
                revokeInternal(info.getUserIdent(), info.getRoles(), true /* is replay */);
            }
        } catch (DdlException e) {
            LOG.error("should not happened", e);
        }
    }

    private void revokeInternal(UserIdentity userIdent, String role, TablePattern tblPattern,
            PrivBitSet privs, Map<ColPrivilegeKey, Set<String>> colPrivileges, boolean errOnNonExist, boolean isReplay)
            throws DdlException {
        writeLock();
        try {
            if (role == null) {
                role = roleManager.getUserDefaultRoleName(userIdent);
            }
            // revoke privs from role
            roleManager.revokePrivs(role, tblPattern, privs, colPrivileges, errOnNonExist);

            if (!isReplay) {
                PrivInfo info = new PrivInfo(userIdent, tblPattern, privs, null, role, colPrivileges);
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

    private void revokeInternal(UserIdentity userIdent, String role, WorkloadGroupPattern workloadGroupPattern,
            PrivBitSet privs, boolean errOnNonExist, boolean isReplay) throws DdlException {
        writeLock();
        try {
            if (role == null) {
                role = roleManager.getUserDefaultRoleName(userIdent);
            }

            // revoke privs from role
            roleManager.revokePrivs(role, workloadGroupPattern, privs, errOnNonExist);

            if (!isReplay) {
                PrivInfo info = new PrivInfo(userIdent, workloadGroupPattern, privs, null, role);
                Env.getCurrentEnv().getEditLog().logRevokePriv(info);
            }
            LOG.info("finished to revoke privilege. is replay: {}", isReplay);
        } finally {
            writeUnlock();
        }
    }

    // revoke for roles
    private void revokeInternal(UserIdentity userIdent, List<String> roles, boolean isReplay) throws DdlException {
        writeLock();
        try {
            if (userManager.getUserByUserIdentity(userIdent) == null) {
                throw new DdlException("user: " + userIdent + " does not exist");
            }
            //roles must exist
            for (String roleName : roles) {
                if (roleManager.getRole(roleName) == null) {
                    throw new DdlException("role:" + roleName + " does not exist");
                }
            }
            userRoleManager.removeUserRoles(userIdent, roles);
            if (!isReplay) {
                PrivInfo info = new PrivInfo(userIdent, roles);
                Env.getCurrentEnv().getEditLog().logRevokePriv(info);
            }
            LOG.info("finished to revoke role privilege. is replay: {}", isReplay);
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
            userManager.setPassword(userIdent, password, errOnNonExist);
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

    public void refreshLdap(RefreshLdapStmt refreshLdapStmt) {
        ldapManager.refresh(refreshLdapStmt.getIsAll(), refreshLdapStmt.getUser());
    }

    // create role
    public void createRole(CreateRoleStmt stmt) throws DdlException {
        createRoleInternal(stmt.getRole(), stmt.isSetIfNotExists(), false);
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

            roleManager.addOrMergeRole(emptyPrivsRole, true /* err on exist */);

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
        dropRoleInternal(stmt.getRole(), stmt.isSetIfExists(), false);
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

    public Set<UserIdentity> getRoleUsers(String roleName) {
        readLock();
        try {
            return userRoleManager.getUsersByRole(roleName);
        } finally {
            readUnlock();
        }
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
            propertyMgr.updateUserProperty(user, properties, isReplay);
            if (!isReplay) {
                UserPropertyInfo propertyInfo = new UserPropertyInfo(user, properties);
                Env.getCurrentEnv().getEditLog().logUpdateUserProperty(propertyInfo);
            }
            LOG.info("finished to set properties for user: {}", user);
        } catch (DdlException e) {
            if (isReplay && e.getMessage().contains("Unknown user property")) {
                LOG.warn("ReplayUpdateUserProperty failed, maybe FE rolled back version, " + e.getMessage());
            } else {
                throw e;
            }
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

    public int getQueryTimeout(String qualifiedUser) {
        readLock();
        try {
            return propertyMgr.getQueryTimeout(qualifiedUser);
        } finally {
            readUnlock();
        }
    }

    public int getInsertTimeout(String qualifiedUser) {
        readLock();
        try {
            return propertyMgr.getInsertTimeout(qualifiedUser);
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

    public int getParallelFragmentExecInstanceNum(String qualifiedUser) {
        readLock();
        try {
            return propertyMgr.getParallelFragmentExecInstanceNum(qualifiedUser);
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

    public String getWorkloadGroup(String qualifiedUser) {
        readLock();
        try {
            return propertyMgr.getWorkloadGroup(qualifiedUser);
        } finally {
            readUnlock();
        }
    }

    public Pair<Boolean, String> isWorkloadGroupInUse(String groupName) {
        readLock();
        try {
            return propertyMgr.isWorkloadGroupInUse(groupName);
        } finally {
            readUnlock();
        }
    }

    public void getAllDomains(Set<String> allDomains) {
        readLock();
        try {
            userManager.getAllDomains(allDomains);
        } finally {
            readUnlock();
        }
    }

    // refresh all user set by domain resolver.
    public void refreshUserPrivEntriesByResovledIPs(Map<String, Set<String>> resolvedIPsMap) {
        writeLock();
        try {
            // 1. delete all user
            userManager.clearEntriesSetByResolver();
            // 2. add new user
            userManager.addUserPrivEntriesByResolvedIPs(resolvedIPsMap);
        } finally {
            writeUnlock();
        }
    }

    // return the auth info of specified user, or infos of all users, if user is not specified.
    // the returned columns are defined in AuthProcDir
    // the specified user identity should be the identity created by CREATE USER, same as result of
    // SELECT CURRENT_USER();
    public List<List<String>> getAuthInfo(UserIdentity specifiedUserIdent) {
        List<List<String>> userAuthInfos = Lists.newArrayList();
        readLock();
        try {
            if (specifiedUserIdent == null) {
                // get all users' auth info
                Map<String, List<User>> nameToUsers = userManager.getNameToUsers();
                for (List<User> users : nameToUsers.values()) {
                    for (User user : users) {
                        if (!user.isSetByDomainResolver()) {
                            getUserAuthInfo(userAuthInfos, user.getUserIdentity());
                        }
                    }
                }
            } else {
                getUserAuthInfo(userAuthInfos, specifiedUserIdent);
            }
        } finally {
            readUnlock();
        }
        return userAuthInfos;
    }

    private void getUserAuthInfo(List<List<String>> userAuthInfos, UserIdentity userIdent) {
        // AuthProcDir.TITLE_NAMES
        List<String> userAuthInfo = Lists.newArrayList();
        // ================= UserIdentity =======================
        userAuthInfo.add(userIdent.toString());
        if (isLdapAuthEnabled() && ldapManager.doesUserExist(userIdent.getQualifiedUser())) {
            LdapUserInfo ldapUserInfo = ldapManager.getUserInfo(userIdent.getQualifiedUser());
            // ============== Password ==============
            userAuthInfo.add(ldapUserInfo.isSetPasswd() ? "Yes" : "No");
            // ============== Roles ==============
            userAuthInfo.add(ldapUserInfo.getRoles().stream().map(role -> role.getRoleName())
                    .collect(Collectors.joining(",")));
        } else {
            User user = userManager.getUserByUserIdentity(userIdent);
            if (user == null) {
                userAuthInfo.add(FeConstants.null_string);
                userAuthInfo.add(FeConstants.null_string);
            } else {
                // ============== Password ==============
                userAuthInfo.add(user.hasPassword() ? "Yes" : "No");
                // ============== Roles ==============
                userAuthInfo.add(Joiner.on(",").join(userRoleManager
                        .getRolesByUser(userIdent, ConnectContext.get().getSessionVariable().showUserDefaultRole)));
            }
        }
        // ==============GlobalPrivs==============
        PrivBitSet globalPrivs = new PrivBitSet();
        List<PrivEntry> globalEntries = getUserGlobalPrivTable(userIdent).entries;
        if (!CollectionUtils.isEmpty(globalEntries)) {
            globalPrivs.or(globalEntries.get(0).privSet);
        }
        userAuthInfo.add(globalPrivs.isEmpty() ? FeConstants.null_string : globalPrivs.toString());
        // ============== CatalogPrivs ========================
        String ctlPrivs = getUserCtlPrivTable(userIdent).entries.stream()
                .map(entry -> String.format("%s: %s",
                        ((CatalogPrivEntry) entry).getOrigCtl(), entry.privSet))
                .collect(Collectors.joining("; "));
        if (Strings.isNullOrEmpty(ctlPrivs)) {
            ctlPrivs = FeConstants.null_string;
        }
        userAuthInfo.add(ctlPrivs);
        // ============== DatabasePrivs ==============
        List<String> dbPrivs = Lists.newArrayList();
        for (PrivEntry entry : getUserDbPrivTable(userIdent).entries) {
            DbPrivEntry dEntry = (DbPrivEntry) entry;
            PrivBitSet savedPrivs = dEntry.getPrivSet().copy();
            dbPrivs.add(String.format("%s.%s: %s", dEntry.getOrigCtl(), dEntry.getOrigDb(),
                    savedPrivs));
        }

        if (dbPrivs.isEmpty()) {
            userAuthInfo.add(FeConstants.null_string);
        } else {
            userAuthInfo.add(Joiner.on("; ").join(dbPrivs));
        }

        // tbl
        List<String> tblPrivs = Lists.newArrayList();
        for (PrivEntry entry : getUserTblPrivTable(userIdent).entries) {
            TablePrivEntry tEntry = (TablePrivEntry) entry;
            PrivBitSet savedPrivs = tEntry.getPrivSet().copy();
            tblPrivs.add(String.format("%s.%s.%s: %s", tEntry.getOrigCtl(), tEntry.getOrigDb(),
                    tEntry.getOrigTbl(), savedPrivs));
        }

        if (tblPrivs.isEmpty()) {
            userAuthInfo.add(FeConstants.null_string);
        } else {
            userAuthInfo.add(Joiner.on("; ").join(tblPrivs));
        }

        // col
        List<String> colPrivs = Lists.newArrayList();
        for (Entry<ColPrivilegeKey, Set<String>> entry : getUserColPrivMap(userIdent).entrySet()) {
            colPrivs.add(String.format("%s.%s.%s: %s%s", entry.getKey().getCtl(), entry.getKey().getDb(),
                    entry.getKey().getTbl(), entry.getKey().getPrivilege(), entry.getValue()));
        }

        if (colPrivs.isEmpty()) {
            userAuthInfo.add(FeConstants.null_string);
        } else {
            userAuthInfo.add(Joiner.on("; ").join(colPrivs));
        }

        // resource
        List<String> resourcePrivs = Lists.newArrayList();
        for (PrivEntry entry : getUserResourcePrivTable(userIdent).entries) {
            ResourcePrivEntry rEntry = (ResourcePrivEntry) entry;
            PrivBitSet savedPrivs = rEntry.getPrivSet().copy();
            resourcePrivs.add(rEntry.getOrigResource() + ": " + savedPrivs.toString());
        }

        if (resourcePrivs.isEmpty()) {
            userAuthInfo.add(FeConstants.null_string);
        } else {
            userAuthInfo.add(Joiner.on("; ").join(resourcePrivs));
        }

        // workload group
        List<String> workloadGroupPrivs = Lists.newArrayList();
        for (PrivEntry entry : getUserWorkloadGroupPrivTable(userIdent).entries) {
            WorkloadGroupPrivEntry workloadGroupPrivEntry = (WorkloadGroupPrivEntry) entry;
            PrivBitSet savedPrivs = workloadGroupPrivEntry.getPrivSet().copy();
            workloadGroupPrivs.add(workloadGroupPrivEntry.getOrigWorkloadGroupName() + ": " + savedPrivs);
        }

        if (workloadGroupPrivs.isEmpty()) {
            userAuthInfo.add(FeConstants.null_string);
        } else {
            userAuthInfo.add(Joiner.on("; ").join(workloadGroupPrivs));
        }

        userAuthInfos.add(userAuthInfo);
    }

    private GlobalPrivTable getUserGlobalPrivTable(UserIdentity userIdentity) {
        GlobalPrivTable table = new GlobalPrivTable();
        Set<Role> roles = getRolesByUserWithLdap(userIdentity);
        for (Role role : roles) {
            table.merge(role.getGlobalPrivTable());
        }
        return table;
    }

    private CatalogPrivTable getUserCtlPrivTable(UserIdentity userIdentity) {
        CatalogPrivTable table = new CatalogPrivTable();
        Set<Role> roles = getRolesByUserWithLdap(userIdentity);
        for (Role role : roles) {
            table.merge(role.getCatalogPrivTable());
        }
        return table;
    }

    private DbPrivTable getUserDbPrivTable(UserIdentity userIdentity) {
        DbPrivTable table = new DbPrivTable();
        Set<Role> roles = getRolesByUserWithLdap(userIdentity);
        for (Role role : roles) {
            table.merge(role.getDbPrivTable());
        }
        return table;
    }

    private TablePrivTable getUserTblPrivTable(UserIdentity userIdentity) {
        TablePrivTable table = new TablePrivTable();
        Set<Role> roles = getRolesByUserWithLdap(userIdentity);
        for (Role role : roles) {
            table.merge(role.getTablePrivTable());
        }
        return table;
    }

    private Map<ColPrivilegeKey, Set<String>> getUserColPrivMap(UserIdentity userIdentity) {
        Map<ColPrivilegeKey, Set<String>> colPrivMap = Maps.newHashMap();
        Set<Role> roles = getRolesByUserWithLdap(userIdentity);
        for (Role role : roles) {
            Role.mergeColPrivMap(colPrivMap, role.getColPrivMap());
        }
        return colPrivMap;
    }


    private ResourcePrivTable getUserResourcePrivTable(UserIdentity userIdentity) {
        ResourcePrivTable table = new ResourcePrivTable();
        Set<Role> roles = getRolesByUserWithLdap(userIdentity);
        for (Role role : roles) {
            table.merge(role.getResourcePrivTable());
        }
        return table;
    }

    private WorkloadGroupPrivTable getUserWorkloadGroupPrivTable(UserIdentity userIdentity) {
        WorkloadGroupPrivTable table = new WorkloadGroupPrivTable();
        Set<Role> roles = getRolesByUserWithLdap(userIdentity);
        for (Role role : roles) {
            table.merge(role.getWorkloadGroupPrivTable());
        }
        return table;
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

    public void setInitialRootPassword(String initialRootPassword) {
        // Skip set root password if `initial_root_password` set to empty string
        if (StringUtils.isEmpty(initialRootPassword)) {
            return;
        }
        byte[] scramble;
        try {
            scramble = MysqlPassword.checkPassword(initialRootPassword);
        } catch (AnalysisException e) {
            // Skip set root password if `initial_root_password` is not valid 2-staged SHA-1 encrypted
            LOG.warn("initial_root_password [{}] is not valid 2-staged SHA-1 encrypted, ignore it",
                    initialRootPassword);
            return;
        }
        UserIdentity rootUser = new UserIdentity(ROOT_USER, "%");
        rootUser.setIsAnalyzed();
        try {
            setPasswordInternal(rootUser, scramble, null, false, false, false);
        } catch (DdlException e) {
            LOG.warn("Fail to set initial root password, ignore it", e);
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
            Map<String, List<User>> nameToUsers = userManager.getNameToUsers();
            for (List<User> users : nameToUsers.values()) {
                for (User user : users) {
                    if (!user.isSetByDomainResolver()) {
                        TablePrivTable tablePrivTable = getUserTblPrivTable(user.getUserIdentity());
                        if (tablePrivTable.isEmpty()) {
                            continue;
                        }
                        for (PrivEntry entry : tablePrivTable.getEntries()) {
                            TablePrivEntry tablePrivEntry = (TablePrivEntry) entry;
                            String dbName = ClusterNamespace.getNameFromFullName(tablePrivEntry.getOrigDb());
                            String tblName = tablePrivEntry.getOrigTbl();
                            // Don't show privileges in information_schema
                            if (InfoSchemaDb.DATABASE_NAME.equals(dbName)
                                    || !checkTblPriv(currentUser, DEFAULT_CATALOG, tablePrivEntry.getOrigDb(), tblName,
                                    PrivPredicate.SHOW)) {
                                continue;
                            }

                            String grantee = new String("\'")
                                    .concat(ClusterNamespace
                                            .getNameFromFullName(user.getUserIdentity().getQualifiedUser()))
                                    .concat("\'@\'").concat(user.getUserIdentity().getHost()).concat("\'");
                            String isGrantable = tablePrivEntry.getPrivSet().get(2) ? "YES" : "NO"; // GRANT_PRIV
                            for (Privilege priv : tablePrivEntry.getPrivSet().toPrivilegeList()) {
                                if (!Privilege.privInDorisToMysql.containsKey(priv)) {
                                    continue;
                                }
                                TPrivilegeStatus status = new TPrivilegeStatus();
                                status.setTableName(tblName);
                                status.setPrivilegeType(Privilege.privInDorisToMysql.get(priv));
                                status.setGrantee(grantee);
                                status.setSchema(dbName);
                                status.setIsGrantable(isGrantable);
                                tblPrivResult.add(status);
                            }
                        }

                    }
                }
            }

        } finally {
            readUnlock();
        }
    }

    // Used for creating schema_privileges table in information_schema.
    public void getSchemaPrivStatus(List<TPrivilegeStatus> dbPrivResult, UserIdentity currentUser) {
        readLock();
        try {
            Map<String, List<User>> nameToUsers = userManager.getNameToUsers();
            for (List<User> users : nameToUsers.values()) {
                for (User user : users) {
                    if (!user.isSetByDomainResolver()) {
                        DbPrivTable dbPrivTable = getUserDbPrivTable(user.getUserIdentity());
                        if (dbPrivTable.isEmpty()) {
                            continue;
                        }
                        for (PrivEntry entry : dbPrivTable.getEntries()) {
                            DbPrivEntry dbPrivEntry = (DbPrivEntry) entry;
                            String origDb = dbPrivEntry.getOrigDb();
                            String dbName = ClusterNamespace.getNameFromFullName(dbPrivEntry.getOrigDb());
                            // Don't show privileges in information_schema
                            if (InfoSchemaDb.DATABASE_NAME.equals(dbName)
                                    || !checkDbPriv(currentUser, InternalCatalog.INTERNAL_CATALOG_NAME, origDb,
                                    PrivPredicate.SHOW)) {
                                continue;
                            }

                            String grantee = new String("\'")
                                    .concat(ClusterNamespace
                                            .getNameFromFullName(user.getUserIdentity().getQualifiedUser()))
                                    .concat("\'@\'").concat(user.getUserIdentity().getHost()).concat("\'");
                            String isGrantable = dbPrivEntry.getPrivSet().get(2) ? "YES" : "NO"; // GRANT_PRIV
                            for (Privilege priv : dbPrivEntry.getPrivSet().toPrivilegeList()) {
                                if (!Privilege.privInDorisToMysql.containsKey(priv)) {
                                    continue;
                                }
                                TPrivilegeStatus status = new TPrivilegeStatus();
                                status.setPrivilegeType(Privilege.privInDorisToMysql.get(priv));
                                status.setGrantee(grantee);
                                status.setSchema(dbName);
                                status.setIsGrantable(isGrantable);
                                dbPrivResult.add(status);
                            }
                        }

                    }
                }
            }

        } finally {
            readUnlock();
        }
    }

    // Used for creating user_privileges table in information_schema.
    public void getGlobalPrivStatus(List<TPrivilegeStatus> userPrivResult, UserIdentity currentUser) {
        readLock();
        try {
            if (!checkGlobalPriv(currentUser, PrivPredicate.SHOW)) {
                return;
            }
            Map<String, List<User>> nameToUsers = userManager.getNameToUsers();
            for (List<User> users : nameToUsers.values()) {
                for (User user : users) {
                    if (!user.isSetByDomainResolver()) {
                        GlobalPrivTable userGlobalPrivTable = getUserGlobalPrivTable(user.getUserIdentity());
                        if (userGlobalPrivTable.isEmpty()) {
                            continue;
                        }
                        PrivEntry privEntry = userGlobalPrivTable.entries.get(0);
                        if (privEntry.getPrivSet().isEmpty()) {
                            continue;
                        }
                        String grantee = new String("\'")
                                .concat(ClusterNamespace.getNameFromFullName(user.getUserIdentity().getQualifiedUser()))
                                .concat("\'@\'").concat(user.getUserIdentity().getHost()).concat("\'");
                        String isGrantable = privEntry.getPrivSet().get(2) ? "YES" : "NO"; // GRANT_PRIV
                        for (Privilege globalPriv : privEntry.getPrivSet().toPrivilegeList()) {
                            if (globalPriv == Privilege.ADMIN_PRIV) {
                                // ADMIN_PRIV includes all privileges of table and resource.
                                for (String priv : Privilege.privInDorisToMysql.values()) {
                                    TPrivilegeStatus status = new TPrivilegeStatus();
                                    status.setPrivilegeType(priv);
                                    status.setGrantee(grantee);
                                    status.setIsGrantable("YES");
                                    userPrivResult.add(status);
                                }
                                break;
                            }
                            if (!Privilege.privInDorisToMysql.containsKey(globalPriv)) {
                                continue;
                            }
                            TPrivilegeStatus status = new TPrivilegeStatus();
                            status.setPrivilegeType(Privilege.privInDorisToMysql.get(globalPriv));
                            status.setGrantee(grantee);
                            status.setIsGrantable(isGrantable);
                            userPrivResult.add(status);
                        }
                    }
                }
            }
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

    //tmp for current user can only has one role
    private void setRoleToUser(UserIdentity userIdent, String role) throws DdlException {
        // 1. check if role exist
        Role newRole = roleManager.getRole(role);
        if (newRole == null) {
            throw new DdlException("Role " + role + " does not exist");
        }
        userRoleManager.dropUser(userIdent);
        userRoleManager.addUserRole(userIdent, role);
        userRoleManager.addUserRole(userIdent, roleManager.getUserDefaultRoleName(userIdent));
    }

    public Set<String> getAllUser() {
        return userManager.getNameToUsers().keySet();
    }

    /**
     * This is a bug that if created a normal user and grant it with ADMIN_PRIV/RESOURCE_PRIV/NODE_PRIV
     * before v1.2, and then upgrade to v1.2, these privileges will be set in catalog level, but it should be
     * in global level.
     * This method will rectify this bug. And it's logic is same with userPrivTable.degradeToInternalCatalogPriv(),
     * but userPrivTable.degradeToInternalCatalogPriv() only handle the info in images, not in edit log.
     * This rectifyPrivs() will be called after image and edit log replayed.
     * So it will rectify the bug in both images and edit log.
     */
    public void rectifyPrivs() {
        roleManager.rectifyPrivs();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // role manager must be first, because role should be exist before any user
        roleManager.write(out);
        userManager.write(out);
        userRoleManager.write(out);
        propertyMgr.write(out);
        ldapInfo.write(out);
        passwdPolicyManager.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        roleManager = RoleManager.read(in);
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_116) {
            userManager = UserManager.read(in);
            userRoleManager = UserRoleManager.read(in);
            propertyMgr = UserPropertyMgr.read(in);
        } else {
            // new Auth will fill userManager,roleManager,and userRoleManager,roleManager will be reset when read,
            // so we need reset userManager and userRoleManager to avoid data inconsistency
            userManager = new UserManager();
            userRoleManager = new UserRoleManager();
            UserPrivTable userPrivTable = (UserPrivTable) PrivTable.read(in);
            CatalogPrivTable catalogPrivTable;
            if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_111) {
                catalogPrivTable = (CatalogPrivTable) PrivTable.read(in);
            } else {
                catalogPrivTable = userPrivTable.degradeToInternalCatalogPriv();
                LOG.info("Load auth from meta version < {}, degrade UserPrivTable to CatalogPrivTable",
                        FeMetaVersion.VERSION_111);
            }
            DbPrivTable dbPrivTable = (DbPrivTable) PrivTable.read(in);
            TablePrivTable tablePrivTable = (TablePrivTable) PrivTable.read(in);
            ResourcePrivTable resourcePrivTable = (ResourcePrivTable) PrivTable.read(in);
            propertyMgr = UserPropertyMgr.read(in);
            try {
                upgradeToVersion116(userPrivTable, catalogPrivTable, dbPrivTable, tablePrivTable, resourcePrivTable);
            } catch (Exception e) {
                // will not generate exception
                LOG.warn("upgrade failed,", e);
            }
        }
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_106) {
            ldapInfo = LdapInfo.read(in);
        }

        if (userManager.getNameToUsers().isEmpty()) {
            // init root and admin user
            initUser();
        }
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_113) {
            passwdPolicyManager = PasswordPolicyManager.read(in);
        } else {
            passwdPolicyManager = new PasswordPolicyManager();
        }
    }

    private void upgradeToVersion116(UserPrivTable userPrivTable, CatalogPrivTable catalogPrivTable,
            DbPrivTable dbPrivTable, TablePrivTable tablePrivTable, ResourcePrivTable resourcePrivTable)
            throws AnalysisException, DdlException, PatternMatcherException {
        //OPERATOR and Admin role not save users,if not inituser,root will do not have admin role
        initUser();
        for (Entry<String, UserProperty> entry : propertyMgr.propertyMap.entrySet()) {
            for (Entry<String, byte[]> userEntry : entry.getValue().getWhiteList().getPasswordMap().entrySet()) {
                //create user
                User user = userManager
                        .createUser(UserIdentity.createAnalyzedUserIdentWithDomain(entry.getKey(), userEntry.getKey()),
                                userEntry.getValue(), null, false);
                //create default role
                Role defaultRole = roleManager.createDefaultRole(user.getUserIdentity());
                userRoleManager
                        .addUserRole(user.getUserIdentity(), defaultRole.getRoleName());
            }
        }
        List<PrivEntry> userPrivTableEntries = userPrivTable.getEntries();
        for (PrivEntry privEntry : userPrivTableEntries) {
            GlobalPrivEntry globalPrivEntry = (GlobalPrivEntry) privEntry;
            //may repeat with created user from propertyMgr,but no influence
            User user = userManager
                    .createUser(globalPrivEntry.userIdentity, globalPrivEntry.password, globalPrivEntry.domainUserIdent,
                            globalPrivEntry.isSetByDomainResolver);
            //create default role
            Role defaultRole = roleManager.createDefaultRole(user.getUserIdentity());
            userRoleManager
                    .addUserRole(user.getUserIdentity(), defaultRole.getRoleName());
            if (globalPrivEntry.privSet.isEmpty()) {
                continue;
            }
            //grant global auth
            if (globalPrivEntry.privSet.containsResourcePriv()) {
                roleManager.addOrMergeRole(new Role(roleManager.getUserDefaultRoleName(user.getUserIdentity()),
                        ResourcePattern.ALL, PrivBitSet.of(Privilege.USAGE_PRIV)), false);
            }
            PrivBitSet copy = globalPrivEntry.privSet.copy();
            copy.unset(Privilege.USAGE_PRIV.getIdx());
            if (!copy.isEmpty()) {
                roleManager.addOrMergeRole(new Role(roleManager.getUserDefaultRoleName(user.getUserIdentity()),
                        TablePattern.ALL, copy), false);
            }
        }

        Map<String, Role> roles = roleManager.getRoles();
        for (Role role : roles.values()) {
            Set<UserIdentity> users = role.getUsers();
            for (UserIdentity userIdentity : users) {
                userRoleManager.addUserRole(userIdentity, role.getRoleName());
            }
        }

        List<PrivEntry> catalogPrivTableEntries = catalogPrivTable.getEntries();
        for (PrivEntry privEntry : catalogPrivTableEntries) {
            CatalogPrivEntry catalogPrivEntry = (CatalogPrivEntry) privEntry;
            TablePattern tablePattern = new TablePattern(ClusterNamespace.getNameFromFullName(catalogPrivEntry.origCtl),
                    "*", "*");
            tablePattern.analyze();
            Role newRole = new Role(roleManager.getUserDefaultRoleName(catalogPrivEntry.userIdentity),
                    tablePattern, catalogPrivEntry.privSet);
            roleManager.addOrMergeRole(newRole, false);
        }

        List<PrivEntry> dbPrivTableEntries = dbPrivTable.getEntries();
        for (PrivEntry privEntry : dbPrivTableEntries) {
            DbPrivEntry dbPrivEntry = (DbPrivEntry) privEntry;
            TablePattern tablePattern = new TablePattern(ClusterNamespace.getNameFromFullName(dbPrivEntry.origCtl),
                    ClusterNamespace.getNameFromFullName(dbPrivEntry.origDb), "*");
            tablePattern.analyze();
            Role newRole = new Role(roleManager.getUserDefaultRoleName(dbPrivEntry.userIdentity),
                    tablePattern, dbPrivEntry.privSet);
            roleManager.addOrMergeRole(newRole, false);
        }

        List<PrivEntry> tblPrivTableEntries = tablePrivTable.getEntries();
        for (PrivEntry privEntry : tblPrivTableEntries) {
            TablePrivEntry tblPrivEntry = (TablePrivEntry) privEntry;
            TablePattern tablePattern = new TablePattern(ClusterNamespace.getNameFromFullName(tblPrivEntry.origCtl),
                    ClusterNamespace.getNameFromFullName(tblPrivEntry.origDb),
                    ClusterNamespace.getNameFromFullName(tblPrivEntry.getOrigTbl()));
            tablePattern.analyze();
            Role newRole = new Role(roleManager.getUserDefaultRoleName(tblPrivEntry.userIdentity),
                    tablePattern, tblPrivEntry.privSet);
            roleManager.addOrMergeRole(newRole, false);
        }

        List<PrivEntry> resourcePrivTableEntries = resourcePrivTable.getEntries();
        for (PrivEntry privEntry : resourcePrivTableEntries) {
            ResourcePrivEntry resourcePrivEntry = (ResourcePrivEntry) privEntry;
            ResourcePattern resourcePattern = new ResourcePattern(
                    ClusterNamespace.getNameFromFullName(resourcePrivEntry.origResource));
            resourcePattern.analyze();
            Role newRole = new Role(roleManager.getUserDefaultRoleName(resourcePrivEntry.userIdentity),
                    resourcePattern, resourcePrivEntry.privSet);
            roleManager.addOrMergeRole(newRole, false);
        }

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(userManager).append("\n");
        sb.append(userRoleManager).append("\n");
        sb.append(roleManager).append("\n");
        sb.append(propertyMgr).append("\n");
        sb.append(ldapInfo).append("\n");
        return sb.toString();
    }
}
