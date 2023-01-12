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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.io.Text;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.MysqlPassword;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/*
 * UserPrivTable saves all global privs and also password for users
 */
public class UserPrivTable extends PrivTable {
    private static final Logger LOG = LogManager.getLogger(UserPrivTable.class);

    public UserPrivTable() {
    }

    public void getPrivs(UserIdentity currentUser, PrivBitSet savedPrivs) {
        GlobalPrivEntry matchedEntry = null;
        for (PrivEntry entry : entries) {
            GlobalPrivEntry globalPrivEntry = (GlobalPrivEntry) entry;

            if (!globalPrivEntry.match(currentUser, true)) {
                continue;
            }

            matchedEntry = globalPrivEntry;
            break;
        }
        if (matchedEntry == null) {
            return;
        }

        savedPrivs.or(matchedEntry.getPrivSet());
    }

    // validate the connection by host, user and password.
    // return true if this connection is valid, and 'savedPrivs' save all global privs got from user table.
    // if currentUser is not null, save the current user identity
    public void checkPassword(String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString,
            List<UserIdentity> currentUser) throws AuthenticationException {
        LOG.debug("check password for user: {} from {}, password: {}, random string: {}",
                remoteUser, remoteHost, remotePasswd, randomString);

        PasswordPolicyManager passwdPolicyMgr = Env.getCurrentEnv().getAuth().getPasswdPolicyManager();
        // TODO(cmy): for now, we check user table from first entry to last,
        // This may not efficient, but works.
        for (PrivEntry entry : entries) {
            GlobalPrivEntry globalPrivEntry = (GlobalPrivEntry) entry;

            // check host
            if (!globalPrivEntry.isAnyHost() && !globalPrivEntry.getHostPattern().match(remoteHost)) {
                continue;
            }

            // check user
            if (!globalPrivEntry.isAnyUser() && !globalPrivEntry.getUserPattern().match(remoteUser)) {
                continue;
            }

            UserIdentity curUser = globalPrivEntry.getDomainUserIdent();
            // check password
            byte[] saltPassword = MysqlPassword.getSaltFromPassword(globalPrivEntry.getPassword());
            // when the length of password is zero, the user has no password
            if ((remotePasswd.length == saltPassword.length)
                    && (remotePasswd.length == 0
                            || MysqlPassword.checkScramble(remotePasswd, randomString, saltPassword))) {
                passwdPolicyMgr.checkAccountLockedAndPasswordExpiration(curUser);
                // found the matched entry
                if (currentUser != null) {
                    currentUser.add(curUser);
                }
                return;
            } else {
                // case A. this means we already matched a entry by user@host, but password is incorrect.
                // return false, NOT continue matching other entries.
                // For example, there are 2 entries in order:
                // 1. cmy@"192.168.%" identified by '123';
                // 2. cmy@"%" identified by 'abc';
                // if user cmy@'192.168.1.1' try to login with password 'abc', it will be denied.
                passwdPolicyMgr.onFailedLogin(curUser);
                throw new AuthenticationException(ErrorCode.ERR_ACCESS_DENIED_ERROR, remoteUser + "@" + remoteHost,
                        remotePasswd.length == 0 ? "NO" : "YES");
            }
        }

        throw new AuthenticationException(ErrorCode.ERR_ACCESS_DENIED_ERROR, remoteUser + "@" + remoteHost,
                remotePasswd.length == 0 ? "NO" : "YES");
    }

    public void checkPlainPassword(String remoteUser, String remoteHost, String remotePasswd,
            List<UserIdentity> currentUser) throws AuthenticationException {
        PasswordPolicyManager passwdPolicyMgr = Env.getCurrentEnv().getAuth().getPasswdPolicyManager();

        for (PrivEntry entry : entries) {
            GlobalPrivEntry globalPrivEntry = (GlobalPrivEntry) entry;

            // check host
            if (!globalPrivEntry.isAnyHost() && !globalPrivEntry.getHostPattern().match(remoteHost)) {
                continue;
            }

            // check user
            if (!globalPrivEntry.isAnyUser() && !globalPrivEntry.getUserPattern().match(remoteUser)) {
                continue;
            }

            UserIdentity curUser = globalPrivEntry.getDomainUserIdent();
            if (MysqlPassword.checkPlainPass(globalPrivEntry.getPassword(), remotePasswd)) {
                passwdPolicyMgr.checkAccountLockedAndPasswordExpiration(curUser);
                if (currentUser != null) {
                    currentUser.add(globalPrivEntry.getDomainUserIdent());
                }
                return;
            } else {
                // set case A. in checkPassword()
                passwdPolicyMgr.onFailedLogin(curUser);
                throw new AuthenticationException(ErrorCode.ERR_ACCESS_DENIED_ERROR, remoteUser + "@" + remoteHost,
                        "YES");
            }
        }
        throw new AuthenticationException(ErrorCode.ERR_ACCESS_DENIED_ERROR, remoteUser + "@" + remoteHost,
                "YES");
    }


    /*
     * set password for specified entry. It is same as adding an entry to the user priv table.
     */
    public void setPassword(GlobalPrivEntry passwdEntry, boolean errOnNonExist) throws DdlException {
        GlobalPrivEntry addedEntry = (GlobalPrivEntry) addEntry(passwdEntry, false /* err on exist */,
                errOnNonExist /* err on non exist */);
        addedEntry.setPassword(passwdEntry.getPassword());
    }

    // return true only if user exist and not set by domain
    // user set by domain should be checked in property manager
    public boolean doesUserExist(UserIdentity userIdent) {
        for (PrivEntry privEntry : entries) {
            if (privEntry.match(userIdent, true /* exact match */) && !privEntry.isSetByDomainResolver()) {
                return true;
            }
        }
        return false;
    }

    // Check whether the user exists and return the UserIdentity.
    public UserIdentity getCurrentUserIdentity(UserIdentity userIdent) {
        for (PrivEntry privEntry : entries) {
            GlobalPrivEntry globalPrivEntry = (GlobalPrivEntry) privEntry;
            if (globalPrivEntry.match(userIdent, false)) {
                return globalPrivEntry.getDomainUserIdent();
            }
        }
        return null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!isClassNameWrote) {
            String className = UserPrivTable.class.getCanonicalName();
            Text.writeString(out, className);
            isClassNameWrote = true;
        }

        super.write(out);
    }

    /**
     * When replay UserPrivTable from journal whose FeMetaVersion < VERSION_111, the global-level privileges should
     * degrade to internal-catalog-level privileges.
     */
    public CatalogPrivTable degradeToInternalCatalogPriv() throws IOException {
        CatalogPrivTable catalogPrivTable = new CatalogPrivTable();
        for (PrivEntry privEntry : entries) {
            GlobalPrivEntry globalPrivEntry = (GlobalPrivEntry) privEntry;
            if (!globalPrivEntry.match(UserIdentity.ROOT, true)
                    && !globalPrivEntry.match(UserIdentity.ADMIN, true)
                    && !globalPrivEntry.privSet.isEmpty()) {
                try {
                    // USAGE_PRIV is no need to degrade.
                    PrivBitSet removeUsagePriv = globalPrivEntry.privSet.copy();
                    removeUsagePriv.unset(PaloPrivilege.USAGE_PRIV.getIdx());
                    removeUsagePriv.unset(PaloPrivilege.NODE_PRIV.getIdx());
                    CatalogPrivEntry entry = CatalogPrivEntry.create(globalPrivEntry.origUser, globalPrivEntry.origHost,
                            InternalCatalog.INTERNAL_CATALOG_NAME, globalPrivEntry.isDomain, removeUsagePriv);
                    entry.setSetByDomainResolver(false);
                    catalogPrivTable.addEntry(entry, false, false);
                    if (globalPrivEntry.privSet.containsResourcePriv()) {
                        // Should only keep the USAGE_PRIV in userPrivTable, and remove other privs and entries.
                        globalPrivEntry.privSet.and(PrivBitSet.of(PaloPrivilege.USAGE_PRIV));
                    } else {
                        // Remove all other privs
                        globalPrivEntry.privSet.clean();
                    }
                } catch (Exception e) {
                    throw new IOException(e.getMessage());
                }
            }
        }
        return catalogPrivTable;
    }
}
