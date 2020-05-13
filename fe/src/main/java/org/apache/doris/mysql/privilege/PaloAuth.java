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

import org.apache.doris.analysis.CreateRoleStmt;
import org.apache.doris.analysis.CreateUserStmt;
import org.apache.doris.analysis.DropRoleStmt;
import org.apache.doris.analysis.DropUserStmt;
import org.apache.doris.analysis.GrantStmt;
import org.apache.doris.analysis.RevokeStmt;
import org.apache.doris.analysis.SetPassVar;
import org.apache.doris.analysis.SetUserPropertyStmt;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AuthorizationInfo;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Writable;
import org.apache.doris.load.DppConfig;
import org.apache.doris.persist.PrivInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFetchResourceResult;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
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

public class PaloAuth implements Writable {
    private static final Logger LOG = LogManager.getLogger(PaloAuth.class);

    // root user's role is operator.
    // each Palo system has only one root user.
    public static final String ROOT_USER = "root";
    public static final String ADMIN_USER = "admin";

    private UserPrivTable userPrivTable = new UserPrivTable();
    private DbPrivTable dbPrivTable = new DbPrivTable();
    private TablePrivTable tablePrivTable = new TablePrivTable();

    private RoleManager roleManager = new RoleManager();;
    private UserPropertyMgr propertyMgr = new UserPropertyMgr();

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
        GLOBAL, DATABASE, TABLE
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

    private void grantDbPrivs(UserIdentity userIdentity, String db, boolean errOnExist, boolean errOnNonExist,
            PrivBitSet privs) throws DdlException {
        DbPrivEntry entry;
        try {
            entry = DbPrivEntry.create(userIdentity.getHost(), db, userIdentity.getQualifiedUser(),
                    userIdentity.isDomain(), privs);
            entry.setSetByDomainResolver(false);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        dbPrivTable.addEntry(entry, errOnExist, errOnNonExist);
    }

    private void revokeDbPrivs(UserIdentity userIdentity, String db, PrivBitSet privs, boolean errOnNonExist)
            throws DdlException {
        DbPrivEntry entry;
        try {
            entry = DbPrivEntry.create(userIdentity.getHost(), db, userIdentity.getQualifiedUser(),
                    userIdentity.isDomain(), privs);
            entry.setSetByDomainResolver(false);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        dbPrivTable.revoke(entry, errOnNonExist, true /* delete entry when empty */);
    }

    private void grantTblPrivs(UserIdentity userIdentity, String db, String tbl, boolean errOnExist,
            boolean errOnNonExist, PrivBitSet privs) throws DdlException {
        TablePrivEntry entry;
        try {
            entry = TablePrivEntry.create(userIdentity.getHost(), db, userIdentity.getQualifiedUser(), tbl,
                    userIdentity.isDomain(), privs);
            entry.setSetByDomainResolver(false);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        tablePrivTable.addEntry(entry, errOnExist, errOnNonExist);
    }

    private void revokeTblPrivs(UserIdentity userIdentity, String db, String tbl, PrivBitSet privs,
            boolean errOnNonExist) throws DdlException {
        TablePrivEntry entry;
        try {
            entry = TablePrivEntry.create(userIdentity.getHost(), db, userIdentity.getQualifiedUser(), tbl,
                    userIdentity.isDomain(), privs);
            entry.setSetByDomainResolver(false);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        tablePrivTable.revoke(entry, errOnNonExist, true /* delete entry when empty */);
    }

    /*
     * check password, if matched, save the userIdentity in matched entry.
     * the following auth checking should use userIdentity saved in currentUser.
     */
    public boolean checkPassword(String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString,
            List<UserIdentity> currentUser) {
        if (!Config.enable_auth_check) {
            return true;
        }
        if ((remoteUser.equals(ROOT_USER) || remoteUser.equals(ADMIN_USER)) && remoteHost.equals("127.0.0.1")) {
            // root and admin user is allowed to login from 127.0.0.1, in case user forget password.
            if (remoteUser.equals(ROOT_USER)) {
                currentUser.add(UserIdentity.ROOT);
            } else {
                currentUser.add(UserIdentity.ADMIN);
            }
            return true;
        }
        
        readLock();
        try {
            return userPrivTable.checkPassword(remoteUser, remoteHost, remotePasswd, randomString, currentUser);
        } finally {
            readUnlock();
        }
    }

    public boolean checkPlainPassword(String remoteUser, String remoteHost, String remotePasswd,
            List<UserIdentity> currentUser) {
        if (!Config.enable_auth_check) {
            return true;
        }
        readLock();
        try {
            return userPrivTable.checkPlainPassword(remoteUser, remoteHost, remotePasswd, currentUser);
        } finally {
            readUnlock();
        }
    }

    public boolean checkGlobalPriv(ConnectContext ctx, PrivPredicate wanted) {
        return checkGlobalPriv(ctx.getCurrentUserIdentity(), wanted);
    }

    public boolean checkGlobalPriv(UserIdentity currentUser, PrivPredicate wanted) {
        if (!Config.enable_auth_check) {
            return true;
        }
        PrivBitSet savedPrivs = PrivBitSet.of();
        if (checkGlobalInternal(currentUser, wanted, savedPrivs)) {
            return true;
        }

        LOG.debug("failed to get wanted privs: {}, ganted: {}", wanted, savedPrivs);
        return false;
    }

    public boolean checkDbPriv(ConnectContext ctx, String qualifiedDb, PrivPredicate wanted) {
        return checkDbPriv(ctx.getCurrentUserIdentity(), qualifiedDb, wanted);
    }

    /*
     * Check if 'user'@'host' on 'db' has 'wanted' priv.
     * If the given db is null, which means it will no check if database name is matched.
     */
    public boolean checkDbPriv(UserIdentity currentUser, String db, PrivPredicate wanted) {
        if (!Config.enable_auth_check) {
            return true;
        }
        if (wanted.getPrivs().containsNodePriv()) {
            LOG.debug("should not check NODE priv in Database level. user: {}, db: {}",
                    currentUser, db);
            return false;
        }

        PrivBitSet savedPrivs = PrivBitSet.of();
        if (checkGlobalInternal(currentUser, wanted, savedPrivs)
                || checkDbInternal(currentUser, db, wanted, savedPrivs)) {
            return true;
        }

        // if user has any privs of table in this db, and the wanted priv is SHOW, return true
        if (db != null && wanted == PrivPredicate.SHOW && checkTblWithDb(currentUser, db)) {
            return true;
        }

        LOG.debug("failed to get wanted privs: {}, ganted: {}", wanted, savedPrivs);
        return false;
    }

    /*
     * User may not have privs on a database, but have privs of tables in this database.
     * So we have to check if user has any privs of tables in this database.
     * if so, the database should be visible to this user.
     */
    private boolean checkTblWithDb(UserIdentity currentUser, String db) {
        readLock();
        try {
            return tablePrivTable.hasPrivsOfDb(currentUser, db);
        } finally {
            readUnlock();
        }
    }

    public boolean checkTblPriv(ConnectContext ctx, String qualifiedDb, String tbl, PrivPredicate wanted) {
        return checkTblPriv(ctx.getCurrentUserIdentity(), qualifiedDb, tbl, wanted);
    }

    public boolean checkTblPriv(UserIdentity currentUser, String db, String tbl, PrivPredicate wanted) {
        if (!Config.enable_auth_check) {
            return true;
        }
        if (wanted.getPrivs().containsNodePriv()) {
            LOG.debug("should check NODE priv in GLOBAL level. user: {}, db: {}, tbl: {}", currentUser, db, tbl);
            return false;
        }

        PrivBitSet savedPrivs = PrivBitSet.of();
        if (checkGlobalInternal(currentUser, wanted, savedPrivs)
                || checkDbInternal(currentUser, db, wanted, savedPrivs)
                || checkTblInternal(currentUser, db, tbl, wanted, savedPrivs)) {
            return true;
        }

        LOG.debug("failed to get wanted privs: {}, ganted: {}", wanted, savedPrivs);
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
            if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), authInfo.getDbName(),
                                                                    tblName, wanted)) {
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
        return checkHasPrivInternal(ctx.getRemoteIP(), ctx.getQualifiedUser(), priv, levels);
    }

    private boolean checkHasPrivInternal(String host, String user, PrivPredicate priv, PrivLevel... levels) {
        for (PrivLevel privLevel : levels) {
            switch (privLevel) {
            case GLOBAL:
                if (userPrivTable.hasPriv(host, user, priv)) {
                    return true;
                }
                break;
            case DATABASE:
                if (dbPrivTable.hasPriv(host, user, priv)) {
                    return true;
                }
                break;
            case TABLE:
                if (tablePrivTable.hasPriv(host, user, priv)) {
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

    private boolean checkDbInternal(UserIdentity currentUser, String db, PrivPredicate wanted,
            PrivBitSet savedPrivs) {
        readLock();
        try {
            dbPrivTable.getPrivs(currentUser, db, savedPrivs);
            if (PaloPrivilege.satisfy(savedPrivs, wanted)) {
                return true;
            }
        } finally {
            readUnlock();
        }
        return false;
    }

    private boolean checkTblInternal(UserIdentity currentUser, String db, String tbl,
            PrivPredicate wanted, PrivBitSet savedPrivs) {
        readLock();
        try {
            tablePrivTable.getPrivs(currentUser, db, tbl, savedPrivs);
            if (PaloPrivilege.satisfy(savedPrivs, wanted)) {
                return true;
            }
            return false;
        } finally {
            readUnlock();
        }
    }

    // for test only
    public void clear() {
        userPrivTable.clear();
        dbPrivTable.clear();
        tablePrivTable.clear();
    }

    // create user
    public void createUser(CreateUserStmt stmt) throws DdlException {
        createUserInternal(stmt.getUserIdent(), stmt.getQualifiedRole(), stmt.getPassword(), false);
    }

    public void replayCreateUser(PrivInfo privInfo) {
        try {
            createUserInternal(privInfo.getUserIdent(), privInfo.getRole(), privInfo.getPasswd(), true);
        } catch (DdlException e) {
            LOG.error("should not happen", e);
        }
    }

    /*
     * Do following steps:
     * 1. Check does specified role exist. If not, throw exception.
     * 2. Check does user already exist. If yes, throw exception.
     * 3. set password for specified user.
     * 4. grant privs of role to user, if role is specified.
     */
    private void createUserInternal(UserIdentity userIdent, String roleName, byte[] password,
            boolean isReplay) throws DdlException {
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
                throw new DdlException("User " + userIdent + " already exist");
            }

            // 3. set password
            setPasswordInternal(userIdent, password, null, false /* err on non exist */,
                    false /* set by resolver */, true /* is replay */);

            // 4. grant privs of role to user
            if (role != null) {
                for (Map.Entry<TablePattern, PrivBitSet> entry : role.getTblPatternToPrivs().entrySet()) {
                    // use PrivBitSet copy to avoid same object being changed synchronously
                    grantInternal(userIdent, null /* role */, entry.getKey(), entry.getValue().copy(),
                            false /* err on non exist */, true /* is replay */);
                }
            }

            if (role != null) {
                // add user to this role
                role.addUser(userIdent);
            }

            // other user properties
            propertyMgr.addUserResource(userIdent.getQualifiedUser(), false /* not system user */);

            if (!userIdent.getQualifiedUser().equals(ROOT_USER) && !userIdent.getQualifiedUser().equals(ADMIN_USER)) {
                // grant read privs to database information_schema
                TablePattern tblPattern = new TablePattern(InfoSchemaDb.DATABASE_NAME, "*");
                try {
                    tblPattern.analyze(ClusterNamespace.getClusterNameFromFullName(userIdent.getQualifiedUser()));
                } catch (AnalysisException e) {
                    LOG.warn("should not happen", e);
                }
                grantInternal(userIdent, null /* role */, tblPattern, PrivBitSet.of(PaloPrivilege.SELECT_PRIV),
                        false /* err on non exist */, true /* is replay */);
            }

            if (!isReplay) {
                PrivInfo privInfo = new PrivInfo(userIdent, null, null, password, roleName);
                Catalog.getCurrentCatalog().getEditLog().logCreateUser(privInfo);
            }
            LOG.info("finished to create user: {}, is replay: {}", userIdent, isReplay);
        } finally {
            writeUnlock();
        }
    }

    // drop user
    public void dropUser(DropUserStmt stmt) throws DdlException {
        dropUserInternal(stmt.getUserIdentity(), false);
    }

    public void replayDropUser(UserIdentity userIdent) {
        dropUserInternal(userIdent, true);
    }

    public void replayOldDropUser(String userName) {
        UserIdentity userIdentity = new UserIdentity(userName, "%");
        userIdentity.setIsAnalyzed();
        dropUserInternal(userIdentity, true /* is replay */);
    }

    private void dropUserInternal(UserIdentity userIdent, boolean isReplay) {
        writeLock();
        try {
            // we don't check if user exists
            userPrivTable.dropUser(userIdent);
            dbPrivTable.dropUser(userIdent);
            tablePrivTable.dropUser(userIdent);
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

            if (!isReplay) {
                Catalog.getCurrentCatalog().getEditLog().logNewDropUser(userIdent);
            }
            LOG.info("finished to drop user: {}, is replay: {}", userIdent.getQualifiedUser(), isReplay);
        } finally {
            writeUnlock();
        }
    }

    // grant
    public void grant(GrantStmt stmt) throws DdlException {
        PrivBitSet privs = PrivBitSet.of(stmt.getPrivileges());
        grantInternal(stmt.getUserIdent(), stmt.getQualifiedRole(), stmt.getTblPattern(), privs,
                true /* err on non exist */, false /* not replay */);
    }

    public void replayGrant(PrivInfo privInfo) {
        try {
            grantInternal(privInfo.getUserIdent(), privInfo.getRole(),
                    privInfo.getTblPattern(), privInfo.getPrivs(),
                    true /* err on non exist */, true /* is replay */);
        } catch (DdlException e) {
            LOG.error("should not happen", e);
        }
    }

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
                Catalog.getCurrentCatalog().getEditLog().logGrantPriv(info);
            }
            LOG.info("finished to grant privilege. is replay: {}", isReplay);
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
                case DATABASE:
                    grantDbPrivs(userIdent, tblPattern.getQuolifiedDb(),
                                 false /* err on exist */,
                                 false /* err on non exist */,
                                 privs);
                    break;
                case TABLE:
                    grantTblPrivs(userIdent, tblPattern.getQuolifiedDb(),
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

    // return true if user ident exist
    private boolean doesUserExist(UserIdentity userIdent) {
        if (userIdent.isDomain()) {
            return propertyMgr.doesUserExist(userIdent);
        } else {
            return userPrivTable.doesUserExist(userIdent);
        }
    }

    // revoke
    public void revoke(RevokeStmt stmt) throws DdlException {
        PrivBitSet privs = PrivBitSet.of(stmt.getPrivileges());
        revokeInternal(stmt.getUserIdent(), stmt.getQualifiedRole(), stmt.getTblPattern(), privs,
                       true /* err on non exist */, false /* is replay */);
    }

    public void replayRevoke(PrivInfo info) {
        try {
            revokeInternal(info.getUserIdent(), info.getRole(), info.getTblPattern(), info.getPrivs(),
                           true /* err on non exist */, true /* is replay */);
        } catch (DdlException e) {
            LOG.error("should not happend", e);
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
                Catalog.getCurrentCatalog().getEditLog().logRevokePriv(info);
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
                case DATABASE:
                    revokeDbPrivs(userIdent, tblPattern.getQuolifiedDb(), privs, errOnNonExist);
                    break;
                case TABLE:
                    revokeTblPrivs(userIdent, tblPattern.getQuolifiedDb(), tblPattern.getTbl(), privs,
                                   errOnNonExist);
                    break;
                default:
                    Preconditions.checkNotNull(null, tblPattern.getPrivLevel());
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
            LOG.error("should not happend", e);
        }
    }

    public void setPasswordInternal(UserIdentity userIdent, byte[] password, UserIdentity domainUserIdent,
            boolean errOnNonExist, boolean setByResolver, boolean isReplay) throws DdlException {
        Preconditions.checkArgument(!setByResolver || domainUserIdent != null, setByResolver + ", " + domainUserIdent);
        writeLock();
        try {
            if (userIdent.isDomain()) {
                // throw exception if this user already contains this domain
                propertyMgr.setPasswordForDomain(userIdent, password, true /* err on exist */, errOnNonExist /* err on non exist */);
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

            if (!isReplay) {
                PrivInfo info = new PrivInfo(userIdent, null, null, password, null);
                Catalog.getCurrentCatalog().getEditLog().logSetPassword(info);
            }
        } finally {
            writeUnlock();
        }
        LOG.info("finished to set password for {}. is replay: {}", userIdent, isReplay);
    }

    // create role
    public void createRole(CreateRoleStmt stmt) throws DdlException {
        createRoleInternal(stmt.getQualifiedRole(), false);
    }

    public void replayCreateRole(PrivInfo info) {
        try {
            createRoleInternal(info.getRole(), true);
        } catch (DdlException e) {
            LOG.error("should not happend", e);
        }
    }

    private void createRoleInternal(String role, boolean isReplay) throws DdlException {
        PaloRole emptyPrivsRole = new PaloRole(role);
        writeLock();
        try {
            roleManager.addRole(emptyPrivsRole, true /* err on exist */);

            if (!isReplay) {
                PrivInfo info = new PrivInfo(null, null, null, null, role);
                Catalog.getCurrentCatalog().getEditLog().logCreateRole(info);
            }
        } finally {
            writeUnlock();
        }
        LOG.info("finished to create role: {}, is replay: {}", role, isReplay);
    }

    // drop role
    public void dropRole(DropRoleStmt stmt) throws DdlException {
        dropRoleInternal(stmt.getQualifiedRole(), false);
    }

    public void replayDropRole(PrivInfo info) {
        try {
            dropRoleInternal(info.getRole(), true);
        } catch (DdlException e) {
            LOG.error("should not happend", e);
        }
    }

    private void dropRoleInternal(String role, boolean isReplay) throws DdlException {
        writeLock();
        try {
            roleManager.dropRole(role, true /* err on non exist */);

            if (!isReplay) {
                PrivInfo info = new PrivInfo(null, null, null, null, role);
                Catalog.getCurrentCatalog().getEditLog().logDropRole(info);
            }
        } finally {
            writeUnlock();
        }
        LOG.info("finished to drop role: {}, is replay: {}", role, isReplay);
    }

    // update user property
    public void updateUserProperty(SetUserPropertyStmt stmt) throws DdlException {
        List<Pair<String, String>> properties = stmt.getPropertyPairList();
        updateUserPropertyInternal(stmt.getUser(), properties, false /* is replay */);
    }

    public void replayUpdateUserProperty(UserPropertyInfo propInfo) throws DdlException {
        updateUserPropertyInternal(propInfo.getUser(), propInfo.getProperties(), true /* is replay */);
    }

    public void updateUserPropertyInternal(String user, List<Pair<String, String>> properties, boolean isReplay)
            throws DdlException {
        writeLock();
        try {
            propertyMgr.updateUserProperty(user, properties);
            if (!isReplay) {
                UserPropertyInfo propertyInfo = new UserPropertyInfo(user, properties);
                Catalog.getCurrentCatalog().getEditLog().logUpdateUserProperty(propertyInfo);
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
            propertyMgr.addUserPrivEntriesByResovledIPs(resolvedIPsMap);
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
            userAuthInfo.add(gEntry.getPrivSet().toString() + " (" + gEntry.isSetByDomainResolver() + ")");
            break;
        }

        if (userAuthInfo.isEmpty()) {
            if (!userIdent.isDomain()) {
                // If this is not a domain user identity, it must have global priv entry.
                // TODO(cmy): I don't know why previous comment said:
                // This may happen when we grant non global privs to a non exist user via GRANT stmt.
                LOG.warn("user identity does not have global priv entry: {}", userIdent);
                userAuthInfo.add(userIdent.toString());
                userAuthInfo.add("N/A");
                userAuthInfo.add("N/A");
            } else {
                // this is a domain user identity and fall in here, which means this user identity does not
                // have global priv, we need to check user property to see if it has password.
                userAuthInfo.add(userIdent.toString());
                userAuthInfo.add(propertyMgr.doesUserHasPassword(userIdent) ? "No" : "Yes");
                userAuthInfo.add("N/A");
            }
        }

        // db
        List<String> dbPrivs = Lists.newArrayList();
        for (PrivEntry entry : dbPrivTable.entries) {
            if (!entry.match(userIdent, true /* exact match */)) {
                continue;
            }
            DbPrivEntry dEntry = (DbPrivEntry) entry;
            dbPrivs.add(dEntry.getOrigDb() + ": " + dEntry.getPrivSet().toString()
                    + " (" + entry.isSetByDomainResolver() + ")");
        }
        if (dbPrivs.isEmpty()) {
            userAuthInfo.add("N/A");
        } else {
            userAuthInfo.add(Joiner.on("; ").join(dbPrivs));
        }

        // tbl
        List<String> tblPrivs = Lists.newArrayList();
        for (PrivEntry entry : tablePrivTable.entries) {
            if (!entry.match(userIdent, true /* exact match */)) {
                continue;
            }
            TablePrivEntry tEntry = (TablePrivEntry) entry;
            tblPrivs.add(tEntry.getOrigDb() + "." + tEntry.getOrigTbl() + ": "
                    + tEntry.getPrivSet().toString()
                    + " (" + entry.isSetByDomainResolver() + ")");
        }
        if (tblPrivs.isEmpty()) {
            userAuthInfo.add("N/A");
        } else {
            userAuthInfo.add(Joiner.on("; ").join(tblPrivs));
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

    public void dropUserOfCluster(String clusterName, boolean isReplay) {
        writeLock();
        try {
            Set<UserIdentity> allUserIdents = getAllUserIdents(true);
            for (UserIdentity userIdent : allUserIdents) {
                if (userIdent.getQualifiedUser().startsWith(clusterName)) {
                    dropUserInternal(userIdent, isReplay);
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
            createUserInternal(rootUser, PaloRole.OPERATOR_ROLE, new byte[0], true /* is replay */);
            UserIdentity adminUser = new UserIdentity(ADMIN_USER, "%");
            adminUser.setIsAnalyzed();
            createUserInternal(adminUser, PaloRole.ADMIN_ROLE, new byte[0], true /* is replay */);
        } catch (DdlException e) {
            LOG.error("should not happend", e);
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
        dbPrivTable.write(out);
        tablePrivTable.write(out);
        propertyMgr.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        roleManager = RoleManager.read(in);
        userPrivTable = (UserPrivTable) PrivTable.read(in);
        dbPrivTable = (DbPrivTable) PrivTable.read(in);
        tablePrivTable = (TablePrivTable) PrivTable.read(in);
        propertyMgr = UserPropertyMgr.read(in);

        if (userPrivTable.isEmpty()) {
            // init root and admin user
            initUser();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(userPrivTable).append("\n");
        sb.append(dbPrivTable).append("\n");
        sb.append(tablePrivTable).append("\n");
        sb.append(roleManager).append("\n");
        sb.append(propertyMgr).append("\n");
        return sb.toString();
    }
}

