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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.mysql.MysqlPassword;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class UserManager implements Writable {
    public static final String ANY_HOST = "%";
    private static final Logger LOG = LogManager.getLogger(UserManager.class);
    //    Set<User> users = new HashSet<>();
    //One name may have multiple User,because host can be different
    private Map<String, List<User>> nameToUsers = new HashMap<>();

    public boolean userIdentityExist(UserIdentity userIdentity) {
        List<User> users = nameToUsers.get(userIdentity.getQualifiedUser());
        if (CollectionUtils.isEmpty(users)) {
            return false;
        }
        for (User user : users) {
            if (user.getUserIdentity().getHost().equalsIgnoreCase(userIdentity.getHost())) {
                return true;
            }
        }
        return false;
    }

    public List<User> getUserByName(String name) {
        return nameToUsers.get(name);
    }

    public void checkPassword(String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString,
            List<UserIdentity> currentUser) throws AuthenticationException {
        LOG.debug("check password for user: {} from {}, password: {}, random string: {}",
                remoteUser, remoteHost, remotePasswd, randomString);

        PasswordPolicyManager passwdPolicyMgr = Env.getCurrentEnv().getAuth().getPasswdPolicyManager();
        List<User> users = nameToUsers.get(remoteUser);
        if (CollectionUtils.isEmpty(users)) {
            return;
        }

        for (User user : users) {
            if (user.getUserIdentity().isDomain()) {
                continue;
            }
            // check host
            if (!user.isAnyHost() && !user.getHostPattern().match(remoteHost)) {
                continue;
            }
            UserIdentity curUser = user.getDomainUserIdentity();
            // check password
            byte[] saltPassword = MysqlPassword.getSaltFromPassword(user.getPassword().getPassword());
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

    }

    public void checkPlainPassword(String remoteUser, String remoteHost, String remotePasswd,
            List<UserIdentity> currentUser) throws AuthenticationException {
        PasswordPolicyManager passwdPolicyMgr = Env.getCurrentEnv().getAuth().getPasswdPolicyManager();
        List<User> users = nameToUsers.get(remoteUser);
        if (CollectionUtils.isEmpty(users)) {
            return;
        }
        for (User user : users) {
            if (user.getUserIdentity().isDomain()) {
                continue;
            }
            // check host
            if (!user.isAnyHost() && !user.getHostPattern().match(remoteHost)) {
                continue;
            }
            UserIdentity curUser = user.getDomainUserIdentity();
            if (MysqlPassword.checkPlainPass(user.getPassword().getPassword(), remotePasswd)) {
                passwdPolicyMgr.checkAccountLockedAndPasswordExpiration(curUser);
                if (currentUser != null) {
                    currentUser.add(curUser);
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

    public void clearEntriesSetByResolver() {
        Iterator<Entry<String, List<User>>> iterator = nameToUsers.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, List<User>> next = iterator.next();
            Iterator<User> iter = next.getValue().iterator();
            while (iter.hasNext()) {
                User user = iter.next();
                if (user.isSetByDomainResolver()) {
                    iter.remove();
                }
            }
            if (CollectionUtils.isEmpty(next.getValue())) {
                iterator.remove();
            } else {
                Collections.sort(next.getValue());
            }
        }

    }

    public User createUser(UserIdentity userIdent, byte[] pwd, UserIdentity domainUserIdent, boolean setByResolver)
            throws AnalysisException {
        if (userIdentityExist(userIdent)) {
            User userByUserIdentity = getUserByUserIdentity(userIdent);
            userByUserIdentity.getPassword().setPassword(pwd);
            return userByUserIdentity;
        }

        PatternMatcher hostPattern = PatternMatcher
                .createMysqlPattern(userIdent.getHost(), CaseSensibility.HOST.getCaseSensibility());
        User user = new User();
        user.setAnyHost(userIdent.getHost().equals(ANY_HOST));
        user.setUserIdentity(userIdent);
        Password password = new Password();
        password.setPassword(pwd);
        user.setPassword(password);
        user.setHostPattern(hostPattern);
        if (setByResolver) {
            Preconditions.checkNotNull(domainUserIdent);
            user.setDomainUserIdentity(domainUserIdent);
        }
        List<User> nameToLists = nameToUsers.get(userIdent.getQualifiedUser());
        if (CollectionUtils.isEmpty(nameToLists)) {
            nameToLists = Lists.newArrayList(user);
            nameToUsers.put(userIdent.getQualifiedUser(), nameToLists);
        } else {
            Collections.sort(nameToLists);
        }
        return user;
    }

    public User getUserByUserIdentity(UserIdentity userIdent) {
        List<User> nameToLists = nameToUsers.get(userIdent.getQualifiedUser());
        if (CollectionUtils.isEmpty(nameToLists)) {
            return null;
        }
        Iterator<User> iter = nameToLists.iterator();
        while (iter.hasNext()) {
            User user = iter.next();
            if (user.getUserIdentity().equals(userIdent)) {
                return user;
            }
        }
        return null;
    }

    public void removeUser(UserIdentity userIdent) {
        List<User> nameToLists = nameToUsers.get(userIdent.getQualifiedUser());
        if (CollectionUtils.isEmpty(nameToLists)) {
            return;
        }
        Iterator<User> iter = nameToLists.iterator();
        while (iter.hasNext()) {
            User user = iter.next();
            if (user.getUserIdentity().equals(userIdent)) {
                iter.remove();
            }
        }
        if (CollectionUtils.isEmpty(nameToLists)) {
            nameToUsers.remove(userIdent.getQualifiedUser());
        } else {
            Collections.sort(nameToLists);
        }
    }

    public Map<String, List<User>> getNameToUsers() {
        return nameToUsers;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(nameToUsers.size());
        for (Entry<String, List<User>> entry : nameToUsers.entrySet()) {
            Text.writeString(out, entry.getKey());
            out.writeInt(entry.getValue().size());
            for (User user : entry.getValue()) {
                user.write(out);
            }
        }
    }

    public static UserManager read(DataInput in) throws IOException {
        UserManager userManager = new UserManager();
        userManager.readFields(in);
        return userManager;
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String name = Text.readString(in);
            int userIdentSize = in.readInt();
            List<User> users = new ArrayList<>(userIdentSize);
            for (int j = 0; j < userIdentSize; j++) {
                User user = User.read(in);
                users.add(user);
            }
            nameToUsers.put(name, users);
        }
    }
}
