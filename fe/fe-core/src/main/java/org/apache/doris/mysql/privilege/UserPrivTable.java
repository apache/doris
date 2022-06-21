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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
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

    /*
     * Check if user@host has specified privilege
     */
    public boolean hasPriv(String host, String user, PrivPredicate wanted) {
        for (PrivEntry entry : entries) {
            GlobalPrivEntry globalPrivEntry = (GlobalPrivEntry) entry;
            // check host
            if (!globalPrivEntry.isAnyHost() && !globalPrivEntry.getHostPattern().match(host)) {
                continue;
            }
            // check user
            if (!globalPrivEntry.isAnyUser() && !globalPrivEntry.getUserPattern().match(user)) {
                continue;
            }
            if (globalPrivEntry.getPrivSet().satisfy(wanted)) {
                return true;
            }
        }
        return false;
    }

    // validate the connection by host, user and password.
    // return true if this connection is valid, and 'savedPrivs' save all global privs got from user table.
    // if currentUser is not null, save the current user identity
    public boolean checkPassword(String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString,
            List<UserIdentity> currentUser) {
        LOG.debug("check password for user: {} from {}, password: {}, random string: {}",
                  remoteUser, remoteHost, remotePasswd, randomString);

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

            // check password
            byte[] saltPassword = MysqlPassword.getSaltFromPassword(globalPrivEntry.getPassword());
            // when the length of password is zero, the user has no password
            if ((remotePasswd.length == saltPassword.length)
                    && (remotePasswd.length == 0
                            || MysqlPassword.checkScramble(remotePasswd, randomString, saltPassword))) {
                // found the matched entry
                if (currentUser != null) {
                    currentUser.add(globalPrivEntry.getDomainUserIdent());
                }
                return true;
            } else {
                // case A. this means we already matched a entry by user@host, but password is incorrect.
                // return false, NOT continue matching other entries.
                // For example, there are 2 entries in order:
                // 1. cmy@"192.168.%" identified by '123';
                // 2. cmy@"%" identified by 'abc';
                // if user cmy@'192.168.1.1' try to login with password 'abc', it will be denied.
                return false;
            }
        }

        return false;
    }

    public boolean checkPlainPassword(String remoteUser, String remoteHost, String remotePasswd,
            List<UserIdentity> currentUser) {
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

            if (MysqlPassword.checkPlainPass(globalPrivEntry.getPassword(), remotePasswd)) {
                if (currentUser != null) {
                    currentUser.add(globalPrivEntry.getDomainUserIdent());
                }
                return true;
            } else {
                // set case A. in checkPassword()
                return false;
            }
        }

        return false;
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
}
