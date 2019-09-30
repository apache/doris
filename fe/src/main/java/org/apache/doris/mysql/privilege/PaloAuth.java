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
import org.apache.doris.catalog.AccessPrivilege;
import org.apache.doris.catalog.AuthorizationInfo;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Writable;
import org.apache.doris.load.DppConfig;
import org.apache.doris.persist.PrivInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFetchResourceResult;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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

    private GlobalPrivEntry grantGlobalPrivs(String host, String user, byte[] password,
            boolean errOnExist, boolean errOnNonExist, boolean grantByResolver, PrivBitSet privs)
            throws DdlException {
        if (errOnExist && errOnNonExist) {
            throw new DdlException("Can only specified errOnExist or errOnNonExist");
        }
        GlobalPrivEntry entry;
        try {
            entry = GlobalPrivEntry.create(host, user, password, privs);
            entry.setSetByDomainResolver(grantByResolver);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        userPrivTable.addEntry(entry, errOnExist, errOnNonExist);
        return entry;
    }

    private void revokeGlobalPrivs(String host, String user, PrivBitSet privs, boolean revokeByResovler,
            boolean errOnNonExist, boolean deleteEntryWhenEmpty) throws DdlException {
        GlobalPrivEntry entry;
        try {
            entry = GlobalPrivEntry.create(host, user, new byte[0], privs);
            entry.setSetByDomainResolver(revokeByResovler);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        if (!userPrivTable.revoke(entry, errOnNonExist, deleteEntryWhenEmpty)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NONEXISTING_GRANT, user, host);
        }
    }

    private void grantDbPrivs(String host, String db, String user, boolean errOnExist, boolean errOnNonExist,
            boolean grantByResolver, PrivBitSet privs) throws DdlException {
        DbPrivEntry entry;
        try {
            entry = DbPrivEntry.create(host, db, user, privs);
            entry.setSetByDomainResolver(grantByResolver);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        dbPrivTable.addEntry(entry, errOnExist, errOnNonExist);
    }

    private void revokeDbPrivs(String host, String db, String user, PrivBitSet privs, boolean setByResolver,
            boolean errOnNonExist) throws DdlException {
        DbPrivEntry entry;
        try {
            entry = DbPrivEntry.create(host, db, user, privs);
            entry.setSetByDomainResolver(setByResolver);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        if (!dbPrivTable.revoke(entry, errOnNonExist, true /* delete entry when empty */)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NONEXISTING_GRANT, user, host);
        }
    }

    private void grantTblPrivs(String host, String db, String user, String tbl, boolean errOnExist,
            boolean errOnNonExist, boolean grantByDomain, PrivBitSet privs) throws DdlException {
        TablePrivEntry entry;
        try {
            entry = TablePrivEntry.create(host, db, user, tbl, privs);
            entry.setSetByDomainResolver(grantByDomain);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        tablePrivTable.addEntry(entry, errOnExist, errOnNonExist);
    }

    private void revokeTblPrivs(String host, String db, String user, String tbl, PrivBitSet privs,
            boolean setByResolver, boolean errOnNonExist) throws DdlException {
        TablePrivEntry entry;
        try {
            entry = TablePrivEntry.create(host, db, user, tbl, privs);
            entry.setSetByDomainResolver(setByResolver);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        if (!tablePrivTable.revoke(entry, errOnNonExist, true /* delete entry when empty */)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NONEXISTING_GRANT, user, host);
        }
    }

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

    public boolean checkPlainPassword(String remoteUser, String remoteHost, String remotePasswd) {
        if (!Config.enable_auth_check) {
            return true;
        }
        readLock();
        try {
            return userPrivTable.checkPlainPassword(remoteUser, remoteHost, remotePasswd);
        } finally {
            readUnlock();
        }
    }

    public boolean checkGlobalPriv(ConnectContext ctx, PrivPredicate wanted) {
        return checkGlobalPriv(ctx.getRemoteIP(), ctx.getQualifiedUser(), wanted);
    }

    public boolean checkGlobalPriv(String host, String user, PrivPredicate wanted) {
        if (!Config.enable_auth_check) {
            return true;
        }
        PrivBitSet savedPrivs = PrivBitSet.of();
        if (checkGlobalInternal(host, user, wanted, savedPrivs)) {
            return true;
        }

        LOG.debug("failed to get wanted privs: {}, ganted: {}", wanted, savedPrivs);
        return false;
    }

    public boolean checkDbPriv(ConnectContext ctx, String qualifiedDb, PrivPredicate wanted) {
        return checkDbPriv(ctx.getRemoteIP(), qualifiedDb, ctx.getQualifiedUser(), wanted);
    }

    /*
     * Check if 'user'@'host' on 'db' has 'wanted' priv.
     * If the given db is null, which means it will no check if database name is matched.
     */
    public boolean checkDbPriv(String host, String db, String user, PrivPredicate wanted) {
        if (!Config.enable_auth_check) {
            return true;
        }
        if (wanted.getPrivs().containsNodePriv()) {
            LOG.debug("should not check NODE priv in Database level. host: {}, user: {}, db: {}",
                      host, user, db);
            return false;
        }

        PrivBitSet savedPrivs = PrivBitSet.of();
        if (checkGlobalInternal(host, user, wanted, savedPrivs)
                || checkDbInternal(host, db, user, wanted, savedPrivs)) {
            return true;
        }

        // if user has any privs of table in this db, and the wanted priv is SHOW, return true
        if (db != null && wanted == PrivPredicate.SHOW && checkTblWithDb(host, db, user)) {
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
    private boolean checkTblWithDb(String host, String db, String user) {
        readLock();
        try {
            return tablePrivTable.hasPrivsOfDb(host, db, user);
        } finally {
            readUnlock();
        }
    }

    public boolean checkTblPriv(ConnectContext ctx, String qualifiedDb, String tbl, PrivPredicate wanted) {
        return checkTblPriv(ctx.getRemoteIP(), qualifiedDb, ctx.getQualifiedUser(), tbl, wanted);
    }

    public boolean checkTblPriv(String host, String db, String user, String tbl, PrivPredicate wanted) {
        if (!Config.enable_auth_check) {
            return true;
        }
        if (wanted.getPrivs().containsNodePriv()) {
            LOG.debug("should check NODE priv in GLOBAL level. host: {}, user: {}, db: {}",
                      host, user, db);
            return false;
        }

        PrivBitSet savedPrivs = PrivBitSet.of();
        if (checkGlobalInternal(host, user, wanted, savedPrivs)
                || checkDbInternal(host, db, user, wanted, savedPrivs)
                || checkTblInternal(host, db, user, tbl, wanted, savedPrivs)) {
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

    private boolean checkGlobalInternal(String host, String user, PrivPredicate wanted, PrivBitSet savedPrivs) {
        readLock();
        try {
            userPrivTable.getPrivs(host, user, savedPrivs);
            if (PaloPrivilege.satisfy(savedPrivs, wanted)) {
                return true;
            }
            return false;
        } finally {
            readUnlock();
        }
    }

    private boolean checkDbInternal(String host, String db, String user, PrivPredicate wanted,
            PrivBitSet savedPrivs) {
        readLock();
        try {
            dbPrivTable.getPrivs(host, db, user, savedPrivs);
            if (PaloPrivilege.satisfy(savedPrivs, wanted)) {
                return true;
            }
        } finally {
            readUnlock();
        }
        return false;
    }

    private boolean checkTblInternal(String host, String db, String user, String tbl,
            PrivPredicate wanted, PrivBitSet savedPrivs) {
        readLock();
        try {
            tablePrivTable.getPrivs(host, db, user, tbl, savedPrivs);
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

    private void createUserInternal(UserIdentity userIdent, String roleName, byte[] password,
            boolean isReplay) throws DdlException {
        writeLock();
        try {
            PaloRole role = null;
            if (roleName != null) {
                // get privs of role
                role = roleManager.getRole(roleName);
                if (role == null) {
                    throw new DdlException("Role: " + roleName + " does not exist");
                }
            }
            
            if (userIdent.isDomain()) {
                // the host here is a domain name, add it to whitelist.
                Map<TablePattern, PrivBitSet> privsMap = Maps.newHashMap();
                if (role != null) {
                    // grant privileges of role to this whitelist
                    privsMap = role.getTblPatternToPrivs();
                }
                propertyMgr.addOrGrantWhiteList(userIdent, privsMap, password, true /* err on exist */,
                                                false /* err on non exist*/);

            } else {
                // check if user already exist
                GlobalPrivEntry dummyEntry = null;
                try {
                    dummyEntry = GlobalPrivEntry.create(userIdent.getHost(), userIdent.getQualifiedUser(), null,
                                                        PrivBitSet.of());
                } catch (AnalysisException e) {
                    LOG.error("should not happen", e);
                }
                if (userPrivTable.getExistingEntry(dummyEntry) != null) {
                    throw new DdlException("User " + userIdent + " already exist");
                }
                
                if (role != null) {
                    // grant privs of role to user
                    for (Map.Entry<TablePattern, PrivBitSet> entry : role.getTblPatternToPrivs().entrySet()) {
                        // use PrivBitSet copy to avoid same object being changed synchronously
                        grantInternal(userIdent, null, entry.getKey(), entry.getValue().copy(),
                                      false /* not set by domain */,
                                      false /* err on non exist */, true /* is replay */);
                    }
                }

                // set password field of global priv entry
                // the global entry may or may not exist
                setPasswordInternal(userIdent, password, true /* add if not exist */,
                                    false /* set by resolver */, true);
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
                grantInternal(userIdent, null, tblPattern, PrivBitSet.of(PaloPrivilege.SELECT_PRIV),
                              false, false /* err on non exist */, true /* is replay */);
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
            userPrivTable.dropUser(userIdent.getQualifiedUser());
            dbPrivTable.dropUser(userIdent.getQualifiedUser());
            tablePrivTable.dropUser(userIdent.getQualifiedUser());

            // drop user in roles if exist
            roleManager.dropUser(userIdent.getQualifiedUser());

            // drop user property
            propertyMgr.dropUser(userIdent.getQualifiedUser());

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
                      false /* not set by domain */, true /* err on non exist */, false /* not replay */);
    }

    public void replayGrant(PrivInfo privInfo) {
        try {
            grantInternal(privInfo.getUserIdent(), privInfo.getRole(),
                          privInfo.getTblPattern(), privInfo.getPrivs(),
                          false /* not set by domain */, true /* err on non exist */, true /* is replay */);
        } catch (DdlException e) {
            LOG.error("should not happen", e);
        }
    }

    private void grantInternal(UserIdentity userIdent, String role, TablePattern tblPattern,
            PrivBitSet privs, boolean grantByResolver, boolean errOnNonExist, boolean isReplay)
            throws DdlException {
        writeLock();
        try {
            if (role != null) {
                // grant privs to role, role must exist
                PaloRole newRole = new PaloRole(role, tblPattern, privs);
                PaloRole existingRole = roleManager.addRole(newRole, false /* err on exist */);

                // update users' privs of this role
                for (UserIdentity user : existingRole.getUsers()) {
                    if (user.isDomain()) {
                        propertyMgr.addOrGrantWhiteList(user, existingRole.getTblPatternToPrivs(),
                                                        null, false /* err on exist */,
                                                        false /* err on non exist */);
                    } else {
                        for (Map.Entry<TablePattern, PrivBitSet> entry : existingRole.getTblPatternToPrivs().entrySet()) {
                            // copy the PrivBitSet
                            grantPrivs(user, entry.getKey(), entry.getValue().copy(), errOnNonExist, grantByResolver);
                        }
                    }
                }
            } else {
                if (userIdent.isDomain()) {
                    // grant privs to whitelist
                    Map<TablePattern, PrivBitSet> privsMap = Maps.newHashMap();
                    privsMap.put(tblPattern, privs);
                    propertyMgr.addOrGrantWhiteList(userIdent, privsMap, null, false /* err on exist */,
                                                    true /* err on non exist */);
                } else {
                    grantPrivs(userIdent, tblPattern, privs, errOnNonExist, grantByResolver);
                }
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
            boolean errOnNonExist, boolean grantByResolver) throws DdlException {

        LOG.debug("grant {} on {} to {}, err on non exist: {}, grant by resovler: {}",
                  privs, tblPattern, userIdent, errOnNonExist, grantByResolver);

        writeLock();
        try {
            // check is user identity already exist
            if (errOnNonExist && !userPrivTable.doesUserExist(userIdent, true /* exact match */)) {
                throw new DdlException("user " + userIdent + " does not exist");
            }

            // grant privs to user
            switch (tblPattern.getPrivLevel()) {
                case GLOBAL:
                    grantGlobalPrivs(userIdent.getHost(),
                                     userIdent.getQualifiedUser(),
                                     new byte[0],
                                     false /* err on exist */,
                                     errOnNonExist,
                                     grantByResolver, privs);
                    break;
                case DATABASE:
                    grantDbPrivs(userIdent.getHost(), tblPattern.getQuolifiedDb(),
                                 userIdent.getQualifiedUser(),
                                 false /* err on exist */,
                                 false /* err on non exist */,
                                 grantByResolver,
                                 privs);
                    break;
                case TABLE:
                    grantTblPrivs(userIdent.getHost(), tblPattern.getQuolifiedDb(),
                                  userIdent.getQualifiedUser(), tblPattern.getTbl(),
                                  false /* err on exist */,
                                  false /* err on non exist */,
                                  grantByResolver,
                                  privs);
                    break;
                default:
                    Preconditions.checkNotNull(null, tblPattern.getPrivLevel());
            }
        } finally {
            writeUnlock();
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
                PaloRole existingRole = roleManager.revokePrivs(role, tblPattern, privs,
                                                                true /* err on non exist */);
                Preconditions.checkNotNull(existingRole);

                // revoke privs from users of this role
                for (UserIdentity user : existingRole.getUsers()) {
                    if (user.isDomain()) {
                        propertyMgr.revokePrivsFromWhiteList(user, existingRole.getTblPatternToPrivs(),
                                                             false /* err on non exist */);
                    } else {
                        revokePrivs(user, tblPattern, privs, false /* set by resolver */,
                                    false /* err on non exist */, true /* delete entry when empty */);
                    }
                }
            } else {
                if (userIdent.isDomain()) {
                    Map<TablePattern, PrivBitSet> privsMap = Maps.newHashMap();
                    privsMap.put(tblPattern, privs);
                    propertyMgr.revokePrivsFromWhiteList(userIdent, privsMap, errOnNonExist /* err on non exist */);
                } else {
                    // revoke privs from user
                    revokePrivs(userIdent, tblPattern, privs, false /* set by resolver */, errOnNonExist,
                                false /* delete entry when empty */);
                }
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
            boolean setByResolver, boolean errOnNonExist, boolean deleteEntryWhenEmpty) throws DdlException {
        writeLock();
        try {
            switch (tblPattern.getPrivLevel()) {
                case GLOBAL:
                    revokeGlobalPrivs(userIdent.getHost(), userIdent.getQualifiedUser(), privs, setByResolver,
                                      errOnNonExist, deleteEntryWhenEmpty);
                    break;
                case DATABASE:
                    revokeDbPrivs(userIdent.getHost(), tblPattern.getQuolifiedDb(),
                                  userIdent.getQualifiedUser(), privs, setByResolver,
                                  errOnNonExist);
                    break;
                case TABLE:
                    revokeTblPrivs(userIdent.getHost(), tblPattern.getQuolifiedDb(),
                                   userIdent.getQualifiedUser(), tblPattern.getTbl(), privs, setByResolver,
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
        setPasswordInternal(stmt.getUserIdent(), stmt.getPassword(), false /* add if not exist */,
                            false /* set by resolver */, false);
    }

    public void replaySetPassword(PrivInfo info) {
        try {
            setPasswordInternal(info.getUserIdent(), info.getPasswd(), false /* add if not exist */,
                                false /* set by resolver */, true);
        } catch (DdlException e) {
            LOG.error("should not happend", e);
        }
    }

    public void setPasswordInternal(UserIdentity userIdent, byte[] password,
            boolean addIfNotExist, boolean setByResolver, boolean isReplay) throws DdlException {
        writeLock();
        try {
            if (userIdent.isDomain()) {
                // throw exception is user ident does not exist
                propertyMgr.setPassword(userIdent, password);
            } else {
                GlobalPrivEntry passwdEntry;
                try {
                    passwdEntry = GlobalPrivEntry.create(userIdent.getHost(), userIdent.getQualifiedUser(),
                                                         password, PrivBitSet.of());
                    passwdEntry.setSetByDomainResolver(setByResolver);
                } catch (AnalysisException e) {
                    throw new DdlException(e.getMessage());
                }

                userPrivTable.setPassword(passwdEntry, addIfNotExist);
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

    public void getCopiedWhiteList(Map<String, Set<String>> userMap) {
        readLock();
        try {
            propertyMgr.getCopiedWhiteList(userMap);
        } finally {
            readUnlock();
        }
    }

    public void updateResolovedIps(String qualifiedUser, String domain, Set<String> resolvedIPs) {
        writeLock();
        try {
            propertyMgr.updateResolovedIps(qualifiedUser, domain, resolvedIPs);
        } finally {
            writeUnlock();
        }
    }

    public List<List<String>> getAuthInfo(UserIdentity specifiedUserIdent, boolean isAll) {
        List<List<String>> userAuthInfos = Lists.newArrayList();

        readLock();
        try {
            if (specifiedUserIdent == null) {
                if (isAll) {
                    Set<UserIdentity> userIdents = getAllUserIdents(false /* include entry set by resolver */);
                    for (UserIdentity userIdent : userIdents) {
                        getUserAuthInfo(userAuthInfos, userIdent, true /* exact match */);
                    }

                    // get grants from whitelist
                    propertyMgr.getUserAuthInfo(userAuthInfos, null);
                } else {
                    Set<UserIdentity> userIdents = getAllUserIdents(true /* include entry set by resolver */);
                    for (UserIdentity userIdent : userIdents) {
                        getUserAuthInfo(userAuthInfos, userIdent, true /* exact match */);
                    }
                }
            } else {
                if (specifiedUserIdent.isDomain()) {
                    propertyMgr.getUserAuthInfo(userAuthInfos, specifiedUserIdent);
                } else {
                    getUserAuthInfo(userAuthInfos, specifiedUserIdent, false /* exact match */);
                }
            }
        } finally {
            readUnlock();
        }
        return userAuthInfos;
    }

    private void getUserAuthInfo(List<List<String>> userAuthInfos, UserIdentity userIdent,
            boolean exactMatch) {
        List<String> userAuthInfo = Lists.newArrayList();

        // global
        for (PrivEntry entry : userPrivTable.entries) {
            if (!entry.match(userIdent, exactMatch)) {
                continue;
            }
            GlobalPrivEntry gEntry = (GlobalPrivEntry) entry;
            userAuthInfo.add(userIdent.toString());
            userAuthInfo.add((gEntry.getPassword() == null || gEntry.getPassword().length == 0) ? "No" : "Yes");
            userAuthInfo.add(gEntry.getPrivSet().toString() + " (" + gEntry.isSetByDomainResolver() + ")");
            break;
        }
        if (userAuthInfo.isEmpty()) {
            // This may happen when we grant non global privs to a non exist user via GRANT stmt.
            userAuthInfo.add(userIdent.toString());
            userAuthInfo.add("N/A");
            userAuthInfo.add("N/A");
        }

        // db
        List<String> dbPrivs = Lists.newArrayList();
        for (PrivEntry entry : dbPrivTable.entries) {
            if (!entry.match(userIdent, exactMatch)) {
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
            if (!entry.match(userIdent, exactMatch)) {
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

    public void transformAndAddOldUserProperty(UserProperty userProperty) {
        Preconditions.checkState(Catalog.getCurrentCatalogJournalVersion() <= FeMetaVersion.VERSION_43);
        writeLock();
        try {
            // for forward compatibility, we need to transfer the old form of privilege to the new form.
            LOG.info("begin to transfer old user property: {}", userProperty.getQualifiedUser());

            if (userProperty.isAdmin()) {
                UserIdentity userIdent = new UserIdentity(userProperty.getQualifiedUser(), "%");
                userIdent.setIsAnalyzed();
                Map<TablePattern, PrivBitSet> adminPrivs = PaloRole.OPERATOR.getTblPatternToPrivs();
                for (Map.Entry<TablePattern, PrivBitSet> entry : adminPrivs.entrySet()) {
                    try {
                        grantPrivs(userIdent, entry.getKey(), entry.getValue(),
                                   false /* err on non exist */, false /* grant by resolver */);
                    } catch (DdlException e) {
                        LOG.error("should not happen", e);
                    }
                }

                try {
                    setPasswordInternal(userIdent, userProperty.getPassword(), true /* add if not exist */,
                                        false /* set by resolver */, true);
                } catch (DdlException e) {
                    LOG.warn("should not happen", e);
                }

            } else if (userProperty.isSuperuser()) {
                UserIdentity userIdent = new UserIdentity(userProperty.getQualifiedUser(), "%");
                userIdent.setIsAnalyzed();
                Map<TablePattern, PrivBitSet> adminPrivs = PaloRole.ADMIN.getTblPatternToPrivs();
                for (Map.Entry<TablePattern, PrivBitSet> entry : adminPrivs.entrySet()) {
                    try {
                        grantPrivs(userIdent, entry.getKey(), entry.getValue(),
                                   false /* err on non exist */, false /* grant by resolver */);
                    } catch (DdlException e) {
                        LOG.error("should not happen", e);
                    }
                }

                try {
                    setPasswordInternal(userIdent, userProperty.getPassword(), true /* add if not exist */,
                                        false /* set by resolver */, true);
                } catch (DdlException e) {
                    LOG.warn("should not happen", e);
                }

            } else { // normal user

                Set<String> ipWhiteList = userProperty.getWhiteList().getIpWhiteLists();
                Set<String> starIpWhiteList = userProperty.getWhiteList().getStarIpWhiteLists();
                Map<TablePattern, PrivBitSet> privsMap = Maps.newHashMap();

                // 1. get all privs and save them to privsMap
                for (Map.Entry<String, AccessPrivilege> entry : userProperty.getDbPrivMap().entrySet()) {
                    PrivBitSet privs = null;
                    switch (entry.getValue()) {
                        case READ_ONLY:
                            privs = PrivBitSet.of(PaloPrivilege.SELECT_PRIV);
                            break;
                        case READ_WRITE:
                        case ALL:
                            privs = PrivBitSet.of(PaloPrivilege.SELECT_PRIV, PaloPrivilege.LOAD_PRIV,
                                                  PaloPrivilege.ALTER_PRIV, PaloPrivilege.CREATE_PRIV,
                                                  PaloPrivilege.DROP_PRIV);
                            break;
                        default:
                            Preconditions.checkState(false, entry.getValue());
                            break;
                    }

                    TablePattern tblPattern = new TablePattern(ClusterNamespace.getNameFromFullName(entry.getKey()),
                            "*");
                    try {
                        tblPattern.analyze(ClusterNamespace.getClusterNameFromFullName(entry.getKey()));
                    } catch (AnalysisException e) {
                        LOG.error("should not happen", e);
                    }
                    privsMap.put(tblPattern, privs);
                }

                if (!ipWhiteList.isEmpty() || !starIpWhiteList.isEmpty()) {
                    // 2. handle the old whitelist
                    for (String ip : ipWhiteList) {
                        UserIdentity userIdent = new UserIdentity(userProperty.getQualifiedUser(), ip);
                        userIdent.setIsAnalyzed();
                        // 1. set password
                        try {
                            setPasswordInternal(userIdent, userProperty.getPassword(),
                                                true /* add if not exist */,
                                                false /* set by resolver */,
                                                true /* is replay */);
                        } catch (DdlException e) {
                            LOG.error("should not happen", e);
                        }

                        // 2. set privs
                        for (Map.Entry<TablePattern, PrivBitSet> entry : privsMap.entrySet()) {
                            try {
                                grantPrivs(userIdent, entry.getKey(), entry.getValue(),
                                           false /* err on non exist */, false /* grant by resolver */);
                            } catch (DdlException e) {
                                LOG.error("should not happen", e);
                            }
                        }
                    }

                    for (String starIp : starIpWhiteList) {
                        starIp = starIp.replaceAll("\\*", "%");
                        UserIdentity userIdent = new UserIdentity(userProperty.getQualifiedUser(), starIp);
                        userIdent.setIsAnalyzed();
                        // 1. set password
                        try {
                            setPasswordInternal(userIdent, userProperty.getPassword(),
                                                true /* add if not exist */,
                                                false /* set by resolver */,
                                                true /* is replay */);
                        } catch (DdlException e) {
                            LOG.error("should not happen", e);
                        }

                        // 2. set privs
                        for (Map.Entry<TablePattern, PrivBitSet> entry : privsMap.entrySet()) {
                            try {
                                grantPrivs(userIdent, entry.getKey(), entry.getValue(),
                                           false /* err on non exist */, false /* grant by resolver */);
                            } catch (DdlException e) {
                                LOG.error("should not happen", e);
                            }
                        }
                    }
                } else if (userProperty.getWhiteList().getAllDomains().isEmpty()) {
                    // 3. grant privs to user@'%' if there is no whitelist
                    UserIdentity userIdent = new UserIdentity(userProperty.getQualifiedUser(), "%");
                    userIdent.setIsAnalyzed();
                    for (Map.Entry<TablePattern, PrivBitSet> entry : privsMap.entrySet()) {
                        try {
                            grantPrivs(userIdent, entry.getKey(), entry.getValue(),
                                       false /* err on non exist */, false /* grant by resolver */);
                        } catch (DdlException e) {
                            LOG.error("should not happen", e);
                        }
                    }
                }

                // 4. domain is already saved in whitelist, and will be resolved later.
                // but here we add a user@'%' 's password entry, to avoid access deny during transitional period.
                UserIdentity userIdent = new UserIdentity(userProperty.getQualifiedUser(), "%");
                userIdent.setIsAnalyzed();
                try {
                    setPasswordInternal(userIdent, userProperty.getPassword(),
                                        true /* add if not exist */,
                                        false /* set by resolver */,
                                        true /* is replay */);
                } catch (DdlException e) {
                    LOG.error("should not happen", e);
                }

                // 5. update white list's privs info
                Set<String> allDomains = userProperty.getWhiteList().getAllDomains();
                for (String domain : allDomains) {
                    userProperty.getWhiteList().updateDomainMap(domain, privsMap);
                }
                LOG.info("update domains: {}, privs: {}", allDomains, privsMap);

            } // end for normal user property

            // add user property
            propertyMgr.addUserPropertyUnchecked(userProperty);

            LOG.info("finished to transform old user property for user: {}", userProperty.getQualifiedUser());
        } finally {
            writeUnlock();
        }
    }

    public void deletePassworEntry(UserIdentity userIdent) {
        writeLock();
        try {
            // here we try to delete the password entry of the specified user,
            // so that this user can not access to palo any more.
            // we use a tricky way: we revoke all global privs of this, and when no privs granted,
            // the priv entry will be deleted automatically.
            revokeGlobalPrivs(userIdent.getHost(), userIdent.getQualifiedUser(),
                              PrivBitSet.of(PaloPrivilege.values()),
                              true /* revoke by resolver */,
                              false /* err on non exist */,
                              true /* delete entry when empty */);
        } catch (DdlException e) {
            LOG.warn("should not happen", e);
        } finally {
            writeUnlock();
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

    @Deprecated
    public void replayAlterAccess(UserProperty userProperty) {
        Preconditions.checkState(Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_43);
        writeLock();
        try {
            transformAndAddOldUserProperty(userProperty);
        } finally {
            writeUnlock();
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

    @Override
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

