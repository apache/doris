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
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class PaloAuth implements Writable {
    private static final Logger LOG = LogManager.getLogger(PaloAuth.class);

    // root user's role is operator.
    // each Palo system has only one root user.
    public static final String ROOT_USER = "root";
    public static final String ADMIN_USER = "admin";
    // unknown user does not have any privilege, this is just to be compatible with old version.
    public static final String UNKNOWN_USER = "unknown";
    private static final String DEFAULT_CATALOG = InternalCatalog.INTERNAL_CATALOG_NAME;

    private UserPrivTable userPrivTable = new UserPrivTable();
    private CatalogPrivTable catalogPrivTable = new CatalogPrivTable();
    private DbPrivTable dbPrivTable = new DbPrivTable();
    private TablePrivTable tablePrivTable = new TablePrivTable();
    private ResourcePrivTable resourcePrivTable = new ResourcePrivTable();

    private RoleManager roleManager = new RoleManager();
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

    public PaloAuth() {
        initUser();
    }

    public UserPrivTable getUserPrivTable() {
        return userPrivTable;
    }

    public DbPrivTable getDbPrivTable() {
        return dbPrivTable;
    }

    public TablePrivTable getTablePrivTable() {
        return tablePrivTable;
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

    private GlobalPrivEntry grantGlobalPrivs(UserIdentity userIdentity, boolean errOnExist, boolean errOnNonExist,
            PrivBitSet privs) throws DdlException {
        if (errOnExist && errOnNonExist) {
            throw new DdlException("Can only specified errOnExist or errOnNonExist");
        }
        GlobalPrivEntry entry;
        try {
            // password set here will not overwrite the password of existing entry, no need to worry.
            entry = GlobalPrivEntry.create(userIdentity.getHost(), userIdentity.getQualifiedUser(),
                    userIdentity.isDomain(), new byte[0] /* no use */, privs);
            entry.setSetByDomainResolver(false);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        userPrivTable.addEntry(entry, errOnExist, errOnNonExist);
        return entry;
    }

    private void revokeGlobalPrivs(UserIdentity userIdentity, PrivBitSet privs, boolean errOnNonExist)
            throws DdlException {
        GlobalPrivEntry entry;
        try {
            entry = GlobalPrivEntry.create(userIdentity.getHost(), userIdentity.getQualifiedUser(),
                    userIdentity.isDomain(), new byte[0] /* no use */, privs);
            entry.setSetByDomainResolver(false);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        userPrivTable.revoke(entry, errOnNonExist,
                false /* not delete entry if priv is empty, because global priv entry has password */);
    }

    private void grantCatalogPrivs(UserIdentity userIdentity, String ctl,
            boolean errOnExist, boolean errOnNonExist, PrivBitSet privs) throws DdlException {
        CatalogPrivEntry entry;
        try {
            entry = CatalogPrivEntry.create(userIdentity.getQualifiedUser(), userIdentity.getHost(),
                    ctl, userIdentity.isDomain(), privs);
            entry.setSetByDomainResolver(false);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        catalogPrivTable.addEntry(entry, errOnExist, errOnNonExist);
    }

    private void revokeCatalogPrivs(UserIdentity userIdentity, String ctl,
            PrivBitSet privs, boolean errOnNonExist) throws DdlException {
        CatalogPrivEntry entry;
        try {
            entry = CatalogPrivEntry.create(userIdentity.getQualifiedUser(), userIdentity.getHost(),
                    ctl, userIdentity.isDomain(), privs);
            entry.setSetByDomainResolver(false);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        catalogPrivTable.revoke(entry, errOnNonExist, true /* delete entry when empty */);
    }

    private void grantDbPrivs(UserIdentity userIdentity, String ctl, String db,
            boolean errOnExist, boolean errOnNonExist, PrivBitSet privs) throws DdlException {
        DbPrivEntry entry;
        try {
            entry = DbPrivEntry.create(userIdentity.getQualifiedUser(), userIdentity.getHost(),
                    ctl, db, userIdentity.isDomain(), privs);
            entry.setSetByDomainResolver(false);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        dbPrivTable.addEntry(entry, errOnExist, errOnNonExist);
    }

    private void revokeDbPrivs(UserIdentity userIdentity, String ctl, String db,
            PrivBitSet privs, boolean errOnNonExist) throws DdlException {
        DbPrivEntry entry;
        try {
            entry = DbPrivEntry.create(userIdentity.getQualifiedUser(), userIdentity.getHost(),
                    ctl, db, userIdentity.isDomain(), privs);
            entry.setSetByDomainResolver(false);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        dbPrivTable.revoke(entry, errOnNonExist, true /* delete entry when empty */);
    }

    private void grantTblPrivs(UserIdentity userIdentity, String ctl, String db, String tbl,
            boolean errOnExist, boolean errOnNonExist, PrivBitSet privs) throws DdlException {
        TablePrivEntry entry;
        try {
            entry = TablePrivEntry.create(userIdentity.getQualifiedUser(), userIdentity.getHost(),
                    ctl, db, tbl, userIdentity.isDomain(), privs);
            entry.setSetByDomainResolver(false);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        tablePrivTable.addEntry(entry, errOnExist, errOnNonExist);
    }

    private void revokeTblPrivs(UserIdentity userIdentity, String ctl, String db, String tbl,
            PrivBitSet privs, boolean errOnNonExist) throws DdlException {
        TablePrivEntry entry;
        try {
            entry = TablePrivEntry.create(userIdentity.getQualifiedUser(), userIdentity.getHost(),
                    ctl, db, tbl, userIdentity.isDomain(), privs);
            entry.setSetByDomainResolver(false);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        tablePrivTable.revoke(entry, errOnNonExist, true /* delete entry when empty */);
    }

    private void grantResourcePrivs(UserIdentity userIdentity, String resourceName, boolean errOnExist,
                                    boolean errOnNonExist, PrivBitSet privs) throws DdlException {
        ResourcePrivEntry entry;
        try {
            entry = ResourcePrivEntry.create(userIdentity.getHost(), resourceName, userIdentity.getQualifiedUser(),
                                             userIdentity.isDomain(), privs);
            entry.setSetByDomainResolver(false);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        resourcePrivTable.addEntry(entry, errOnExist, errOnNonExist);
    }

    private void revokeResourcePrivs(UserIdentity userIdentity, String resourceName, PrivBitSet privs,
                                     boolean errOnNonExist) throws DdlException {
        ResourcePrivEntry entry;
        try {
            entry = ResourcePrivEntry.create(userIdentity.getHost(), resourceName, userIdentity.getQualifiedUser(),
                                             userIdentity.isDomain(), privs);
            entry.setSetByDomainResolver(false);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        resourcePrivTable.revoke(entry, errOnNonExist, true /* delete entry when empty */);
    }

    public boolean doesRoleExist(String qualifiedRole) {
        return roleManager.getRole(qualifiedRole) != null;
    }

    public void mergeRolesNoCheckName(List<String> roles, PaloRole savedRole) {
        readLock();
        try {
            for (String roleName : roles) {
                if (doesRoleExist(roleName)) {
                    PaloRole role = roleManager.getRole(roleName);
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
            userPrivTable.checkPassword(remoteUser, remoteHost, remotePasswd, randomString, currentUser);
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
            userPrivTable.checkPlainPassword(remoteUser, remoteHost, remotePasswd, currentUser);
        } finally {
            readUnlock();
        }
    }

    public boolean checkGlobalPriv(ConnectContext ctx, PrivPredicate wanted) {
        return checkGlobalPriv(ctx.getCurrentUserIdentity(), wanted);
    }

    public boolean checkGlobalPriv(UserIdentity currentUser, PrivPredicate wanted) {
        PrivBitSet savedPrivs = PrivBitSet.of();
        if (checkGlobalInternal(currentUser, wanted, savedPrivs)) {
            return true;
        }

        LOG.debug("failed to get wanted privs: {}, granted: {}", wanted, savedPrivs);
        return false;
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

        PrivBitSet savedPrivs = PrivBitSet.of();
        if (checkGlobalInternal(currentUser, wanted, savedPrivs)
                || checkCatalogInternal(currentUser, ctl, wanted, savedPrivs)) {
            return true;
        }

        // if user has any privs of databases or tables in this catalog, and the wanted priv is SHOW, return true
        if (ctl != null && wanted == PrivPredicate.SHOW && checkAnyPrivWithinCatalog(currentUser, ctl)) {
            return true;
        }

        LOG.debug("failed to get wanted privs: {}, granted: {}", wanted, savedPrivs);
        return false;
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
        if (wanted.getPrivs().containsNodePriv()) {
            LOG.debug("should not check NODE priv in Database level. user: {}, db: {}",
                    currentUser, db);
            return false;
        }

        PrivBitSet savedPrivs = PrivBitSet.of();
        if (checkGlobalInternal(currentUser, wanted, savedPrivs)
                || checkCatalogInternal(currentUser, ctl, wanted, savedPrivs)
                || checkDbInternal(currentUser, ctl, db, wanted, savedPrivs)) {
            return true;
        }

        // if user has any privs of table in this db, and the wanted priv is SHOW, return true
        if (ctl != null && db != null && wanted == PrivPredicate.SHOW && checkAnyPrivWithinDb(currentUser, ctl, db)) {
            return true;
        }

        LOG.debug("failed to get wanted privs: {}, granted: {}", wanted, savedPrivs);
        return false;
    }

    /*
     * User may not have privs on a catalog, but have privs of databases or tables in this catalog.
     * So we have to check if user has any privs of databases or tables in this catalog.
     * if so, the catalog should be visible to this user.
     */
    private boolean checkAnyPrivWithinCatalog(UserIdentity currentUser, String ctl) {
        readLock();
        try {
            return dbPrivTable.hasPrivsOfCatalog(currentUser, ctl)
                    || tablePrivTable.hasPrivsOfCatalog(currentUser, ctl);
        } finally {
            readUnlock();
        }
    }

    /*
     * User may not have privs on a database, but have privs of tables in this database.
     * So we have to check if user has any privs of tables in this database.
     * if so, the database should be visible to this user.
     */
    private boolean checkAnyPrivWithinDb(UserIdentity currentUser, String ctl, String db) {
        readLock();
        try {
            return (isLdapAuthEnabled() && LdapPrivsChecker.hasPrivsOfDb(currentUser, db))
                    || tablePrivTable.hasPrivsOfDb(currentUser, ctl, db);
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

    public boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl, PrivPredicate wanted) {
        if (wanted.getPrivs().containsNodePriv()) {
            LOG.debug("should check NODE priv in GLOBAL level. user: {}, db: {}, tbl: {}", currentUser, db, tbl);
            return false;
        }

        PrivBitSet savedPrivs = PrivBitSet.of();
        if (checkGlobalInternal(currentUser, wanted, savedPrivs)
                || checkCatalogInternal(currentUser, ctl, wanted, savedPrivs)
                || checkDbInternal(currentUser, ctl, db, wanted, savedPrivs)
                || checkTblInternal(currentUser, ctl, db, tbl, wanted, savedPrivs)) {
            return true;
        }

        LOG.debug("failed to get wanted privs: {}, granted: {}", wanted, savedPrivs);
        return false;
    }

    public boolean checkTblPriv(UserIdentity currentUser, String db, String tbl, PrivPredicate wanted) {
        return checkTblPriv(currentUser, DEFAULT_CATALOG, db, tbl, wanted);
    }

    public boolean checkResourcePriv(ConnectContext ctx, String resourceName, PrivPredicate wanted) {
        return checkResourcePriv(ctx.getCurrentUserIdentity(), resourceName, wanted);
    }

    public boolean checkResourcePriv(UserIdentity currentUser, String resourceName, PrivPredicate wanted) {
        PrivBitSet savedPrivs = PrivBitSet.of();
        if (checkGlobalInternal(currentUser, wanted, savedPrivs)
                || checkResourceInternal(currentUser, resourceName, wanted, savedPrivs)) {
            return true;
        }

        LOG.debug("failed to get wanted privs: {}, granted: {}", wanted, savedPrivs);
        return false;
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
            if (!Env.getCurrentEnv().getAuth()
                    .checkTblPriv(ConnectContext.get(), authInfo.getDbName(), tblName, wanted)) {
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
        return checkHasPrivInternal(ctx.getCurrentUserIdentity(),
                ctx.getRemoteIP(), ctx.getQualifiedUser(), priv, levels);
    }

    private boolean checkHasPrivInternal(UserIdentity currentUser, String host, String user, PrivPredicate priv,
                                         PrivLevel... levels) {
        for (PrivLevel privLevel : levels) {
            switch (privLevel) {
                case GLOBAL:
                    if ((isLdapAuthEnabled() && LdapPrivsChecker.hasGlobalPrivFromLdap(currentUser, priv))
                            || userPrivTable.hasPriv(host, user, priv)) {
                        return true;
                    }
                    break;
                case DATABASE:
                    if ((isLdapAuthEnabled() && LdapPrivsChecker.hasDbPrivFromLdap(currentUser, priv))
                            || dbPrivTable.hasPriv(host, user, priv)) {
                        return true;
                    }
                    break;
                case TABLE:
                    if ((isLdapAuthEnabled() && LdapPrivsChecker.hasTblPrivFromLdap(currentUser, priv))
                            || tablePrivTable.hasPriv(host, user, priv)) {
                        return true;
                    }
                    break;
                default:
                    break;
            }
        }
        return false;
    }

    private boolean checkGlobalInternal(UserIdentity currentUser, PrivPredicate wanted, PrivBitSet savedPrivs) {
        if (isLdapAuthEnabled() && LdapPrivsChecker.hasGlobalPrivFromLdap(currentUser, wanted)) {
            return true;
        }

        readLock();
        try {
            userPrivTable.getPrivs(currentUser, savedPrivs);
            if (PaloPrivilege.satisfy(savedPrivs, wanted)) {
                return true;
            }
            return false;
        } finally {
            readUnlock();
        }
    }

    private boolean checkCatalogInternal(UserIdentity currentUser, String ctl,
                                         PrivPredicate wanted, PrivBitSet savedPrivs) {
        // TODO(gaoxin): check privileges by ldap.
        readLock();
        try {
            catalogPrivTable.getPrivs(currentUser, ctl, savedPrivs);
            if (PaloPrivilege.satisfy(savedPrivs, wanted)) {
                return true;
            }
        } finally {
            readUnlock();
        }
        return false;
    }

    private boolean checkDbInternal(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted,
                                    PrivBitSet savedPrivs) {
        if (isLdapAuthEnabled() && LdapPrivsChecker.hasDbPrivFromLdap(currentUser, db, wanted)) {
            return true;
        }

        readLock();
        try {
            dbPrivTable.getPrivs(currentUser, ctl, db, savedPrivs);
            if (PaloPrivilege.satisfy(savedPrivs, wanted)) {
                return true;
            }
        } finally {
            readUnlock();
        }
        return false;
    }

    private boolean checkTblInternal(UserIdentity currentUser, String ctl, String db, String tbl,
                                     PrivPredicate wanted, PrivBitSet savedPrivs) {
        if (isLdapAuthEnabled() && LdapPrivsChecker.hasTblPrivFromLdap(currentUser, db, tbl, wanted)) {
            return true;
        }

        readLock();
        try {
            tablePrivTable.getPrivs(currentUser, ctl, db, tbl, savedPrivs);
            if (PaloPrivilege.satisfy(savedPrivs, wanted)) {
                return true;
            }
            return false;
        } finally {
            readUnlock();
        }
    }

    private boolean checkResourceInternal(UserIdentity currentUser, String resourceName,
                                          PrivPredicate wanted, PrivBitSet savedPrivs) {
        if (isLdapAuthEnabled() && LdapPrivsChecker.hasResourcePrivFromLdap(currentUser, resourceName, wanted)) {
            return true;
        }

        readLock();
        try {
            resourcePrivTable.getPrivs(currentUser, resourceName, savedPrivs);
            if (PaloPrivilege.satisfy(savedPrivs, wanted)) {
                return true;
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

    // for test only
    public void clear() {
        userPrivTable.clear();
        dbPrivTable.clear();
        tablePrivTable.clear();
        resourcePrivTable.clear();
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

    /*
     * Do following steps:
     * 1. Check does specified role exist. If not, throw exception.
     * 2. Check does user already exist. If yes && ignoreIfExists, just return. Otherwise, throw exception.
     * 3. set password for specified user.
     * 4. grant privs of role to user, if role is specified.
     */
    private void createUserInternal(UserIdentity userIdent, String roleName, byte[] password,
            boolean ignoreIfExists, PasswordOptions passwordOptions, boolean isReplay) throws DdlException {
        writeLock();
        try {
            // 1. check if role exist
            PaloRole role = null;
            if (roleName != null) {
                role = roleManager.getRole(roleName);
                if (role == null) {
                    throw new DdlException("Role: " + roleName + " does not exist");
                }
            }

            // 2. check if user already exist
            if (userPrivTable.doesUserExist(userIdent)) {
                if (ignoreIfExists) {
                    LOG.info("user exists, ignored to create user: {}, is replay: {}", userIdent, isReplay);
                    return;
                }
                throw new DdlException("User " + userIdent + " already exist");
            }

            // 3. set password
            setPasswordInternal(userIdent, password, null, false /* err on non exist */, false /* set by resolver */,
                    true /* is replay */);
            try {
                // 4. grant privs of role to user
                grantPrivsByRole(userIdent, role);

                // other user properties
                propertyMgr.addUserResource(userIdent.getQualifiedUser(), false /* not system user */);

                if (!userIdent.getQualifiedUser().equals(ROOT_USER) && !userIdent.getQualifiedUser()
                        .equals(ADMIN_USER)) {
                    // grant read privs to database information_schema
                    TablePattern tblPattern = new TablePattern(DEFAULT_CATALOG, InfoSchemaDb.DATABASE_NAME, "*");
                    try {
                        tblPattern.analyze(ClusterNamespace.getClusterNameFromFullName(userIdent.getQualifiedUser()));
                    } catch (AnalysisException e) {
                        LOG.warn("should not happen", e);
                    }
                    grantInternal(userIdent, null /* role */, tblPattern, PrivBitSet.of(PaloPrivilege.SELECT_PRIV),
                            false /* err on non exist */, true /* is replay */);
                }
            } catch (Throwable t) {
                // This is a temp protection to avoid bug such as described in
                // https://github.com/apache/doris/issues/11235
                // Normally, all operations in try..catch block should not fail
                // Why add try..catch block after "setPasswordInternal"?
                // Because after calling "setPasswordInternal()", the in-memory state has been changed,
                // so we should make sure the following operations not throw any exception, if it throws,
                // exit the process because there is no way to rollback in-memory state.
                LOG.error("got unexpected exception when creating user. exit", t);
                System.exit(-1);
            }

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

    private void grantPrivsByRole(UserIdentity userIdent, PaloRole role) throws DdlException {
        if (role == null) {
            return;
        }
        writeLock();
        try {
            for (Map.Entry<TablePattern, PrivBitSet> entry : role.getTblPatternToPrivs().entrySet()) {
                // use PrivBitSet copy to avoid same object being changed synchronously
                grantInternal(userIdent, null /* role */, entry.getKey(), entry.getValue().copy(),
                        false /* err on non exist */, true /* is replay */);
            }
            for (Map.Entry<ResourcePattern, PrivBitSet> entry : role.getResourcePatternToPrivs().entrySet()) {
                // use PrivBitSet copy to avoid same object being changed synchronously
                grantInternal(userIdent, null /* role */, entry.getKey(), entry.getValue().copy(),
                        false /* err on non exist */, true /* is replay */);
            }
            // add user to this role
            role.addUser(userIdent);
        } finally {
            writeUnlock();
        }
    }

    private void revokePrivsByRole(UserIdentity userIdent, PaloRole role) throws DdlException {
        if (role == null) {
            return;
        }
        writeLock();
        try {
            for (Map.Entry<TablePattern, PrivBitSet> entry : role.getTblPatternToPrivs().entrySet()) {
                revokeInternal(userIdent, null, entry.getKey(), entry.getValue().copy(), false, true);
            }

            for (Map.Entry<ResourcePattern, PrivBitSet> entry : role.getResourcePatternToPrivs().entrySet()) {
                revokeInternal(userIdent, null, entry.getKey(), entry.getValue().copy(),
                        false, true);
            }
            // drop user from this role
            role.dropUser(userIdent);
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

    public void replayOldDropUser(String userName) throws DdlException {
        UserIdentity userIdentity = new UserIdentity(userName, "%");
        userIdentity.setIsAnalyzed();
        dropUserInternal(userIdentity, false /* ignore if non exists */, true /* is replay */);
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

            // we don't check if user exists
            userPrivTable.dropUser(userIdent);
            catalogPrivTable.dropUser(userIdent);
            dbPrivTable.dropUser(userIdent);
            tablePrivTable.dropUser(userIdent);
            resourcePrivTable.dropUser(userIdent);
            // drop user in roles if exist
            roleManager.dropUser(userIdent);

            if (!userPrivTable.doesUsernameExist(userIdent.getQualifiedUser())) {
                // if user name does not exist in userPrivTable, which means all userIdent with this name
                // has been remove, then we can drop this user from property manager
                propertyMgr.dropUser(userIdent);
            } else if (userIdent.isDomain()) {
                // if there still has entry with this user name, we can not drop user from property map,
                // but we need to remove the specified domain from this user.
                propertyMgr.removeDomainFromUser(userIdent);
            }
            passwdPolicyManager.dropUser(userIdent);

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
    private void grantInternal(UserIdentity userIdent, String role, TablePattern tblPattern,
            PrivBitSet privs, boolean errOnNonExist, boolean isReplay)
            throws DdlException {
        writeLock();
        try {
            if (role != null) {
                // grant privs to role, role must exist
                PaloRole newRole = new PaloRole(role, tblPattern, privs);
                PaloRole existingRole = roleManager.addRole(newRole, false /* err on exist */);

                // update users' privs of this role
                for (UserIdentity user : existingRole.getUsers()) {
                    for (Map.Entry<TablePattern, PrivBitSet> entry : existingRole.getTblPatternToPrivs().entrySet()) {
                        // copy the PrivBitSet
                        grantPrivs(user, entry.getKey(), entry.getValue().copy(), errOnNonExist);
                    }
                }
            } else {
                grantPrivs(userIdent, tblPattern, privs, errOnNonExist);
            }

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
            if (role != null) {
                // grant privs to role, role must exist
                PaloRole newRole = new PaloRole(role, resourcePattern, privs);
                PaloRole existingRole = roleManager.addRole(newRole, false /* err on exist */);

                // update users' privs of this role
                for (UserIdentity user : existingRole.getUsers()) {
                    for (Map.Entry<ResourcePattern, PrivBitSet> entry
                            : existingRole.getResourcePatternToPrivs().entrySet()) {
                        // copy the PrivBitSet
                        grantPrivs(user, entry.getKey(), entry.getValue().copy(), errOnNonExist);
                    }
                }
            } else {
                grantPrivs(userIdent, resourcePattern, privs, errOnNonExist);
            }

            if (!isReplay) {
                PrivInfo info = new PrivInfo(userIdent, resourcePattern, privs, null, role);
                Env.getCurrentEnv().getEditLog().logGrantPriv(info);
            }
            LOG.info("finished to grant resource privilege. is replay: {}", isReplay);
        } finally {
            writeUnlock();
        }
    }

    public void grantPrivs(UserIdentity userIdent, TablePattern tblPattern, PrivBitSet privs,
            boolean errOnNonExist) throws DdlException {
        LOG.debug("grant {} on {} to {}, err on non exist: {}", privs, tblPattern, userIdent, errOnNonExist);

        writeLock();
        try {
            // check if user identity already exist
            if (errOnNonExist && !doesUserExist(userIdent)) {
                throw new DdlException("user " + userIdent + " does not exist");
            }

            // grant privs to user
            switch (tblPattern.getPrivLevel()) {
                case GLOBAL:
                    grantGlobalPrivs(userIdent,
                                     false /* err on exist */,
                                     errOnNonExist,
                                     privs);
                    break;
                case CATALOG:
                    grantCatalogPrivs(userIdent, tblPattern.getQualifiedCtl(),
                                      false /* err on exist */,
                                      false /* err on non exist */,
                                      privs);
                    break;
                case DATABASE:
                    grantDbPrivs(userIdent, tblPattern.getQualifiedCtl(),
                                 tblPattern.getQualifiedDb(),
                                 false /* err on exist */,
                                 false /* err on non exist */,
                                 privs);
                    break;
                case TABLE:
                    grantTblPrivs(userIdent, tblPattern.getQualifiedCtl(),
                                  tblPattern.getQualifiedDb(),
                                  tblPattern.getTbl(),
                                  false /* err on exist */,
                                  false /* err on non exist */,
                                  privs);
                    break;
                default:
                    Preconditions.checkNotNull(null, tblPattern.getPrivLevel());
            }
        } finally {
            writeUnlock();
        }
    }

    public void grantPrivs(UserIdentity userIdent, ResourcePattern resourcePattern, PrivBitSet privs,
                           boolean errOnNonExist) throws DdlException {
        LOG.debug("grant {} on resource {} to {}, err on non exist: {}",
                privs, resourcePattern, userIdent, errOnNonExist);

        writeLock();
        try {
            // check if user identity already exist
            if (errOnNonExist && !doesUserExist(userIdent)) {
                throw new DdlException("user " + userIdent + " does not exist");
            }

            // grant privs to user
            switch (resourcePattern.getPrivLevel()) {
                case GLOBAL:
                    grantGlobalPrivs(userIdent, false, errOnNonExist, privs);
                    break;
                case RESOURCE:
                    grantResourcePrivs(userIdent, resourcePattern.getResourceName(), false, false, privs);
                    break;
                default:
                    Preconditions.checkNotNull(null, resourcePattern.getPrivLevel());
            }
        } finally {
            writeUnlock();
        }
    }

    // return true if user ident exist
    private boolean doesUserExist(UserIdentity userIdent) {
        if (userIdent.isDomain()) {
            return propertyMgr.doesUserExist(userIdent);
        } else {
            return userPrivTable.doesUserExist(userIdent);
        }
    }

    // Check whether the user exists. If the user exists, return UserIdentity, otherwise return null.
    public UserIdentity getCurrentUserIdentity(UserIdentity userIdent) {
        readLock();
        try {
            return userPrivTable.getCurrentUserIdentity(userIdent);
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
            if (role != null) {
                // revoke privs from role
                PaloRole existingRole = roleManager.revokePrivs(role, tblPattern, privs, errOnNonExist);
                if (existingRole != null) {
                    // revoke privs from users of this role
                    for (UserIdentity user : existingRole.getUsers()) {
                        revokePrivs(user, tblPattern, privs, false /* err on non exist */);
                    }
                }
            } else {
                revokePrivs(userIdent, tblPattern, privs, errOnNonExist);
            }

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
            if (role != null) {
                // revoke privs from role
                PaloRole existingRole = roleManager.revokePrivs(role, resourcePattern, privs, errOnNonExist);
                if (existingRole != null) {
                    // revoke privs from users of this role
                    for (UserIdentity user : existingRole.getUsers()) {
                        revokePrivs(user, resourcePattern, privs, false /* err on non exist */);
                    }
                }
            } else {
                revokePrivs(userIdent, resourcePattern, privs, errOnNonExist);
            }

            if (!isReplay) {
                PrivInfo info = new PrivInfo(userIdent, resourcePattern, privs, null, role);
                Env.getCurrentEnv().getEditLog().logRevokePriv(info);
            }
            LOG.info("finished to revoke privilege. is replay: {}", isReplay);
        } finally {
            writeUnlock();
        }
    }

    public void revokePrivs(UserIdentity userIdent, TablePattern tblPattern, PrivBitSet privs,
            boolean errOnNonExist) throws DdlException {
        writeLock();
        try {
            switch (tblPattern.getPrivLevel()) {
                case GLOBAL:
                    revokeGlobalPrivs(userIdent, privs, errOnNonExist);
                    break;
                case CATALOG:
                    revokeCatalogPrivs(userIdent, tblPattern.getQualifiedCtl(), privs, errOnNonExist);
                    break;
                case DATABASE:
                    revokeDbPrivs(userIdent, tblPattern.getQualifiedCtl(),
                            tblPattern.getQualifiedDb(), privs, errOnNonExist);
                    break;
                case TABLE:
                    revokeTblPrivs(userIdent, tblPattern.getQualifiedCtl(), tblPattern.getQualifiedDb(),
                            tblPattern.getTbl(), privs, errOnNonExist);
                    break;
                default:
                    Preconditions.checkNotNull(null, tblPattern.getPrivLevel());
            }
        } finally {
            writeUnlock();
        }
    }

    public void revokePrivs(UserIdentity userIdent, ResourcePattern resourcePattern, PrivBitSet privs,
                            boolean errOnNonExist) throws DdlException {
        writeLock();
        try {
            switch (resourcePattern.getPrivLevel()) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
                case GLOBAL:
                    revokeGlobalPrivs(userIdent, privs, errOnNonExist);
                    break;
                case RESOURCE:
                    revokeResourcePrivs(userIdent, resourcePattern.getResourceName(), privs, errOnNonExist);
                    break;
            }
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
                GlobalPrivEntry passwdEntry;
                try {
                    passwdEntry = GlobalPrivEntry.create(userIdent.getHost(), userIdent.getQualifiedUser(),
                            userIdent.isDomain(), password, PrivBitSet.of());
                    passwdEntry.setSetByDomainResolver(setByResolver);
                    if (setByResolver) {
                        Preconditions.checkNotNull(domainUserIdent);
                        passwdEntry.setDomainUserIdent(domainUserIdent);
                    }
                } catch (AnalysisException e) {
                    throw new DdlException(e.getMessage());
                }
                userPrivTable.setPassword(passwdEntry, errOnNonExist);
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
        PaloRole emptyPrivsRole = new PaloRole(role);
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

    // refresh all priv entries set by domain resolver.
    // 1. delete all priv entries in user priv table which are set by domain resolver previously.
    // 2. add priv entries by new resolving IPs
    public void refreshUserPrivEntriesByResovledIPs(Map<String, Set<String>> resolvedIPsMap) {
        writeLock();
        try {
            // 1. delete all previously set entries
            userPrivTable.clearEntriesSetByResolver();
            // 2. add new entries
            propertyMgr.addUserPrivEntriesByResolvedIPs(resolvedIPsMap);
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
                Set<UserIdentity> userIdents = getAllUserIdents(false /* include entry set by resolver */);
                for (UserIdentity userIdent : userIdents) {
                    getUserAuthInfo(userAuthInfos, userIdent);
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
        List<String> userAuthInfo = Lists.newArrayList();

        // global
        // ldap global privs.
        PrivBitSet ldapGlobalPrivs = LdapPrivsChecker.getGlobalPrivFromLdap(userIdent);
        for (PrivEntry entry : userPrivTable.entries) {
            if (!entry.match(userIdent, true /* exact match */)) {
                continue;
            }
            GlobalPrivEntry gEntry = (GlobalPrivEntry) entry;
            userAuthInfo.add(userIdent.toString());
            if (userIdent.isDomain()) {
                // for domain user ident, password is saved in property manager
                userAuthInfo.add(propertyMgr.doesUserHasPassword(userIdent) ? "No" : "Yes");
            } else {
                userAuthInfo.add((gEntry.getPassword() == null || gEntry.getPassword().length == 0) ? "No" : "Yes");
            }
            PrivBitSet savedPrivs = gEntry.getPrivSet().copy();
            savedPrivs.or(ldapGlobalPrivs);
            userAuthInfo.add(savedPrivs.toString() + " (" + gEntry.isSetByDomainResolver() + ")");
            break;
        }

        if (userAuthInfo.isEmpty()) {
            userAuthInfo.add(userIdent.toString());
            if (LdapPrivsChecker.hasLdapPrivs(userIdent)) {
                userAuthInfo.add("No");
                userAuthInfo.add(ldapGlobalPrivs.toString() + " (false)");
            } else if (!userIdent.isDomain()) {
                // If this is not a domain user identity, it must have global priv entry.
                // TODO(cmy): I don't know why previous comment said:
                // This may happen when we grant non global privs to a non exist user via GRANT stmt.
                LOG.warn("user identity does not have global priv entry: {}", userIdent);
                userAuthInfo.add(FeConstants.null_string);
                userAuthInfo.add(FeConstants.null_string);
            } else {
                // this is a domain user identity and fall in here, which means this user identity does not
                // have global priv, we need to check user property to see if it has password.
                userAuthInfo.add(propertyMgr.doesUserHasPassword(userIdent) ? "No" : "Yes");
                userAuthInfo.add(FeConstants.null_string);
            }
        }

        // catalog
        String ctlPrivs = catalogPrivTable.entries.stream()
                .filter(entry -> entry.match(userIdent, true))
                .map(entry -> String.format("%s: %s (%b)",
                        ((CatalogPrivEntry) entry).getOrigCtl(), entry.privSet, entry.isSetByDomainResolver()))
                .collect(Collectors.joining("; "));
        if (Strings.isNullOrEmpty(ctlPrivs)) {
            ctlPrivs = FeConstants.null_string;
        }
        userAuthInfo.add(ctlPrivs);

        // db
        List<String> dbPrivs = Lists.newArrayList();
        Set<String> addedDbs = Sets.newHashSet();
        for (PrivEntry entry : dbPrivTable.entries) {
            if (!entry.match(userIdent, true /* exact match */)) {
                continue;
            }
            DbPrivEntry dEntry = (DbPrivEntry) entry;
            /**
             * Doris and Ldap may have different privs on one database.
             * Merge these privs and add.
             */
            PrivBitSet savedPrivs = dEntry.getPrivSet().copy();
            savedPrivs.or(LdapPrivsChecker.getDbPrivFromLdap(userIdent, dEntry.getOrigDb()));
            addedDbs.add(dEntry.getOrigDb());
            dbPrivs.add(String.format("%s.%s: %s (%b)", dEntry.getOrigCtl(), dEntry.getOrigDb(),
                    savedPrivs, dEntry.isSetByDomainResolver()));
        }
        // Add privs from ldap groups that have not been added in Doris.
        if (LdapPrivsChecker.hasLdapPrivs(userIdent)) {
            Map<TablePattern, PrivBitSet> ldapDbPrivs = LdapPrivsChecker.getLdapAllDbPrivs(userIdent);
            for (Map.Entry<TablePattern, PrivBitSet> entry : ldapDbPrivs.entrySet()) {
                if (!addedDbs.contains(entry.getKey().getQualifiedDb())) {
                    dbPrivs.add(String.format("%s.%s: %s (%b)", entry.getKey().getQualifiedCtl(),
                            entry.getKey().getQualifiedDb(), entry.getValue(), false));
                }
            }
        }

        if (dbPrivs.isEmpty()) {
            userAuthInfo.add(FeConstants.null_string);
        } else {
            userAuthInfo.add(Joiner.on("; ").join(dbPrivs));
        }

        // tbl
        List<String> tblPrivs = Lists.newArrayList();
        Set<String> addedtbls = Sets.newHashSet();
        for (PrivEntry entry : tablePrivTable.entries) {
            if (!entry.match(userIdent, true /* exact match */)) {
                continue;
            }
            TablePrivEntry tEntry = (TablePrivEntry) entry;
            /**
             * Doris and Ldap may have different privs on one table.
             * Merge these privs and add.
             */
            PrivBitSet savedPrivs = tEntry.getPrivSet().copy();
            savedPrivs.or(LdapPrivsChecker.getTblPrivFromLdap(userIdent, tEntry.getOrigDb(), tEntry.getOrigTbl()));
            addedtbls.add(tEntry.getOrigDb().concat(".").concat(tEntry.getOrigTbl()));
            tblPrivs.add(String.format("%s.%s.%s: %s (%b)", tEntry.getOrigCtl(), tEntry.getOrigDb(),
                    tEntry.getOrigTbl(), savedPrivs, tEntry.isSetByDomainResolver()));
        }
        // Add privs from ldap groups that have not been added in Doris.
        if (LdapPrivsChecker.hasLdapPrivs(userIdent)) {
            Map<TablePattern, PrivBitSet> ldapTblPrivs = LdapPrivsChecker.getLdapAllTblPrivs(userIdent);
            for (Map.Entry<TablePattern, PrivBitSet> entry : ldapTblPrivs.entrySet()) {
                if (!addedtbls.contains(entry.getKey().getQualifiedDb().concat(".").concat(entry.getKey().getTbl()))) {
                    tblPrivs.add(String.format("%s: %s (%b)", entry.getKey(), entry.getValue(), false));
                }
            }
        }

        if (tblPrivs.isEmpty()) {
            userAuthInfo.add(FeConstants.null_string);
        } else {
            userAuthInfo.add(Joiner.on("; ").join(tblPrivs));
        }

        // resource
        List<String> resourcePrivs = Lists.newArrayList();
        Set<String> addedResources = Sets.newHashSet();
        for (PrivEntry entry : resourcePrivTable.entries) {
            if (!entry.match(userIdent, true /* exact match */)) {
                continue;
            }
            ResourcePrivEntry rEntry = (ResourcePrivEntry) entry;
            /**
             * Doris and Ldap may have different privs on one resource.
             * Merge these privs and add.
             */
            PrivBitSet savedPrivs = rEntry.getPrivSet().copy();
            savedPrivs.or(LdapPrivsChecker.getResourcePrivFromLdap(userIdent, rEntry.getOrigResource()));
            addedResources.add(rEntry.getOrigResource());
            resourcePrivs.add(rEntry.getOrigResource() + ": " + savedPrivs.toString()
                                      + " (" + entry.isSetByDomainResolver() + ")");
        }
        // Add privs from ldap groups that have not been added in Doris.
        if (LdapPrivsChecker.hasLdapPrivs(userIdent)) {
            Map<ResourcePattern, PrivBitSet> ldapResourcePrivs = LdapPrivsChecker.getLdapAllResourcePrivs(userIdent);
            for (Map.Entry<ResourcePattern, PrivBitSet> entry : ldapResourcePrivs.entrySet()) {
                if (!addedResources.contains(entry.getKey().getResourceName())) {
                    tblPrivs.add(entry.getKey().getResourceName().concat(": ").concat(entry.getValue().toString())
                            .concat(" (false)"));
                }
            }
        }

        if (resourcePrivs.isEmpty()) {
            userAuthInfo.add(FeConstants.null_string);
        } else {
            userAuthInfo.add(Joiner.on("; ").join(resourcePrivs));
        }

        userAuthInfos.add(userAuthInfo);
    }

    private Set<UserIdentity> getAllUserIdents(boolean includeEntrySetByResolver) {
        Set<UserIdentity> userIdents = Sets.newHashSet();
        for (PrivEntry entry : userPrivTable.entries) {
            if (!includeEntrySetByResolver && entry.isSetByDomainResolver()) {
                continue;
            }
            userIdents.add(entry.getUserIdent());
        }
        for (PrivEntry entry : dbPrivTable.entries) {
            if (!includeEntrySetByResolver && entry.isSetByDomainResolver()) {
                continue;
            }
            userIdents.add(entry.getUserIdent());
        }
        for (PrivEntry entry : tablePrivTable.entries) {
            if (!includeEntrySetByResolver && entry.isSetByDomainResolver()) {
                continue;
            }
            userIdents.add(entry.getUserIdent());
        }
        for (PrivEntry entry : resourcePrivTable.entries) {
            if (!includeEntrySetByResolver && entry.isSetByDomainResolver()) {
                continue;
            }
            userIdents.add(entry.getUserIdent());
        }
        return userIdents;
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
            if (checkGlobalPriv(ctx, PrivPredicate.ALL)) {
                return true;
            }

            if (dbPrivTable.hasClusterPriv(ctx, clusterName)) {
                return true;
            }

            if (tablePrivTable.hasClusterPriv(ctx, clusterName)) {
                return true;
            }

            return false;
        } finally {
            readUnlock();
        }
    }

    private void initUser() {
        try {
            UserIdentity rootUser = new UserIdentity(ROOT_USER, "%");
            rootUser.setIsAnalyzed();
            createUserInternal(rootUser, PaloRole.OPERATOR_ROLE, new byte[0],
                    false /* ignore if exists */, PasswordOptions.UNSET_OPTION, true /* is replay */);
            UserIdentity adminUser = new UserIdentity(ADMIN_USER, "%");
            adminUser.setIsAnalyzed();
            createUserInternal(adminUser, PaloRole.ADMIN_ROLE, new byte[0],
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
            for (PrivEntry entry : tablePrivTable.getEntries()) {
                TablePrivEntry tblPrivEntry = (TablePrivEntry) entry;
                String dbName = ClusterNamespace.getNameFromFullName(tblPrivEntry.getOrigDb());
                String tblName = tblPrivEntry.getOrigTbl();

                if (dbName.equals("information_schema" /* Don't show privileges in information_schema */)
                        || !checkTblPriv(currentUser, tblPrivEntry.getOrigDb(), tblName, PrivPredicate.SHOW)) {
                    continue;
                }

                String grantee = new String("\'")
                        .concat(ClusterNamespace.getNameFromFullName(tblPrivEntry.getOrigUser()))
                        .concat("\'@\'").concat(tblPrivEntry.getOrigHost()).concat("\'");
                String isGrantable = tblPrivEntry.getPrivSet().get(2) ? "YES" : "NO"; // GRANT_PRIV
                for (PaloPrivilege paloPriv : tblPrivEntry.getPrivSet().toPrivilegeList()) {
                    if (!PaloPrivilege.privInPaloToMysql.containsKey(paloPriv)) {
                        continue;
                    }
                    TPrivilegeStatus status = new TPrivilegeStatus();
                    status.setTableName(tblName);
                    status.setPrivilegeType(PaloPrivilege.privInPaloToMysql.get(paloPriv));
                    status.setGrantee(grantee);
                    status.setSchema(dbName);
                    status.setIsGrantable(isGrantable);
                    tblPrivResult.add(status);
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
            for (PrivEntry entry : dbPrivTable.getEntries()) {
                DbPrivEntry dbPrivEntry = (DbPrivEntry) entry;
                String origDb = dbPrivEntry.getOrigDb();
                String dbName = ClusterNamespace.getNameFromFullName(dbPrivEntry.getOrigDb());

                if (dbName.equals("information_schema" /* Don't show privileges in information_schema */)
                        || !checkDbPriv(currentUser, origDb, PrivPredicate.SHOW)) {
                    continue;
                }

                String grantee = new String("\'")
                        .concat(ClusterNamespace.getNameFromFullName(dbPrivEntry.getOrigUser()))
                        .concat("\'@\'").concat(dbPrivEntry.getOrigHost()).concat("\'");
                String isGrantable = dbPrivEntry.getPrivSet().get(2) ? "YES" : "NO"; // GRANT_PRIV
                for (PaloPrivilege paloPriv : dbPrivEntry.getPrivSet().toPrivilegeList()) {
                    if (!PaloPrivilege.privInPaloToMysql.containsKey(paloPriv)) {
                        continue;
                    }
                    TPrivilegeStatus status = new TPrivilegeStatus();
                    status.setPrivilegeType(PaloPrivilege.privInPaloToMysql.get(paloPriv));
                    status.setGrantee(grantee);
                    status.setSchema(dbName);
                    status.setIsGrantable(isGrantable);
                    dbPrivResult.add(status);
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

            for (PrivEntry userPrivEntry : userPrivTable.getEntries()) {
                String grantee = new String("\'")
                        .concat(ClusterNamespace.getNameFromFullName(userPrivEntry.getOrigUser()))
                        .concat("\'@\'").concat(userPrivEntry.getOrigHost()).concat("\'");
                String isGrantable = userPrivEntry.getPrivSet().get(2) ? "YES" : "NO"; // GRANT_PRIV
                for (PaloPrivilege paloPriv : userPrivEntry.getPrivSet().toPrivilegeList()) {
                    if (paloPriv == PaloPrivilege.ADMIN_PRIV) {
                        // ADMIN_PRIV includes all privileges of table and resource.
                        for (String priv : PaloPrivilege.privInPaloToMysql.values()) {
                            TPrivilegeStatus status = new TPrivilegeStatus();
                            status.setPrivilegeType(priv);
                            status.setGrantee(grantee);
                            status.setIsGrantable("YES");
                            userPrivResult.add(status);
                        }
                        break;
                    }
                    if (!PaloPrivilege.privInPaloToMysql.containsKey(paloPriv)) {
                        continue;
                    }
                    TPrivilegeStatus status = new TPrivilegeStatus();
                    status.setPrivilegeType(PaloPrivilege.privInPaloToMysql.get(paloPriv));
                    status.setGrantee(grantee);
                    status.setIsGrantable(isGrantable);
                    userPrivResult.add(status);
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

    private void setRoleToUser(UserIdentity userIdent, String role) throws DdlException {
        // 1. check if role exist
        PaloRole newRole = roleManager.getRole(role);
        if (newRole == null) {
            throw new DdlException("Role " + role + " does not exist");
        }
        // 2. find the origin role which the user belongs to
        PaloRole originRole = roleManager.findRoleForUser(userIdent);
        if (originRole != null) {
            // User belong to a role, first revoke privs of origin role from user.
            revokePrivsByRole(userIdent, originRole);
        }
        // 3. grant privs of new role to user
        grantPrivsByRole(userIdent, newRole);
    }

    public static PaloAuth read(DataInput in) throws IOException {
        PaloAuth auth = new PaloAuth();
        auth.readFields(in);
        return auth;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // role manager must be first, because role should be exist before any user
        roleManager.write(out);
        userPrivTable.write(out);
        catalogPrivTable.write(out);
        dbPrivTable.write(out);
        tablePrivTable.write(out);
        resourcePrivTable.write(out);
        propertyMgr.write(out);
        ldapInfo.write(out);
        passwdPolicyManager.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        roleManager = RoleManager.read(in);
        userPrivTable = (UserPrivTable) PrivTable.read(in);
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_111) {
            catalogPrivTable = (CatalogPrivTable) PrivTable.read(in);
        } else {
            catalogPrivTable = userPrivTable.degradeToInternalCatalogPriv();
            LOG.info("Load PaloAuth from meta version < {}, degrade UserPrivTable to CatalogPrivTable",
                    FeMetaVersion.VERSION_111);
        }
        dbPrivTable = (DbPrivTable) PrivTable.read(in);
        tablePrivTable = (TablePrivTable) PrivTable.read(in);
        resourcePrivTable = (ResourcePrivTable) PrivTable.read(in);
        propertyMgr = UserPropertyMgr.read(in);
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_106) {
            ldapInfo = LdapInfo.read(in);
        }

        if (userPrivTable.isEmpty()) {
            // init root and admin user
            initUser();
        }
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_113) {
            passwdPolicyManager = PasswordPolicyManager.read(in);
        } else {
            passwdPolicyManager = new PasswordPolicyManager();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(userPrivTable).append("\n");
        sb.append(dbPrivTable).append("\n");
        sb.append(tablePrivTable).append("\n");
        sb.append(resourcePrivTable).append("\n");
        sb.append(roleManager).append("\n");
        sb.append(propertyMgr).append("\n");
        sb.append(ldapInfo).append("\n");
        return sb.toString();
    }
}
