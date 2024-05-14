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
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.QueryableReentrantReadWriteLock;
import org.apache.doris.mysql.MysqlPassword;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.Lock;

public class UserManager implements Writable, GsonPostProcessable {
    public static final String ANY_HOST = "%";
    private static final Logger LOG = LogManager.getLogger(UserManager.class);

    private static final QueryableReentrantReadWriteLock rwLock = new QueryableReentrantReadWriteLock(false);
    private static final Lock rlock = rwLock.readLock();
    private static final Lock wlock = rwLock.writeLock();

    // One name may have multiple User,because host can be different
    @SerializedName(value = "nameToUsers")
    private Map<String, List<User>> nameToUsers = Maps.newHashMap();

    public boolean userIdentityExist(UserIdentity userIdentity, boolean includeByDomain) {
        rlock.lock();
        try {
            return userIdentityExistWithoutLock(userIdentity, includeByDomain);
        } finally {
            rlock.unlock();
        }
    }

    public boolean userIdentityExistWithoutLock(UserIdentity userIdentity, boolean includeByDomain) {
        List<User> users = nameToUsers.get(userIdentity.getQualifiedUser());
        if (CollectionUtils.isEmpty(users)) {
            return false;
        }
        for (User user : users) {
            if (user.getUserIdentity().getHost().equalsIgnoreCase(userIdentity.getHost())) {
                if (includeByDomain || !user.isSetByDomainResolver()) {
                    return true;
                }
            }
        }
        return false;
    }

    public List<User> getUserByName(String name) {
        rlock.lock();
        try {
            List<User> users = nameToUsers.get(name);
            return users == null ? Collections.EMPTY_LIST : users;
        } finally {
            rlock.unlock();
        }
    }

    public void checkPassword(String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString,
            List<UserIdentity> currentUser) throws AuthenticationException {
        checkPasswordInternal(remoteUser, remoteHost, remotePasswd, randomString, null, currentUser, false);
    }

    public void checkPlainPassword(String remoteUser, String remoteHost, String remotePasswd,
            List<UserIdentity> currentUser) throws AuthenticationException {
        checkPasswordInternal(remoteUser, remoteHost, null, null, remotePasswd, currentUser, true);
    }

    private void checkPasswordInternal(String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString,
            String remotePasswdStr, List<UserIdentity> currentUser, boolean plain) throws AuthenticationException {
        PasswordPolicyManager passwdPolicyMgr = Env.getCurrentEnv().getAuth().getPasswdPolicyManager();
        List<User> users = new ArrayList<>();
        rlock.lock();
        try {
            users = nameToUsers.get(remoteUser);
            if (CollectionUtils.isEmpty(users)) {
                throw new AuthenticationException(ErrorCode.ERR_ACCESS_DENIED_ERROR, remoteUser + "@" + remoteHost,
                    "YES");
            }
        } finally {
            rlock.unlock();
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
            if (comparePassword(user.getPassword(), remotePasswd, randomString, remotePasswdStr, plain)) {
                passwdPolicyMgr.checkAccountLockedAndPasswordExpiration(curUser);
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
                        hasRemotePasswd(plain, remotePasswd));
            }
        }
        throw new AuthenticationException(ErrorCode.ERR_ACCESS_DENIED_ERROR, remoteUser + "@" + remoteHost,
                hasRemotePasswd(plain, remotePasswd));
    }

    public List<UserIdentity> getUserIdentityUncheckPasswd(String remoteUser, String remoteHost) {
        List<UserIdentity> userIdentities = Lists.newArrayList();
        rlock.lock();
        try {
            List<User> users = nameToUsers.getOrDefault(remoteUser, Lists.newArrayList());
            for (User user : users) {
                if (!user.getUserIdentity().isDomain()
                        && (user.isAnyHost() || user.getHostPattern().match(remoteHost))) {
                    userIdentities.add(user.getUserIdentity());
                }
            }
            return userIdentities;
        } finally {
            rlock.unlock();
        }
    }

    private String hasRemotePasswd(boolean plain, byte[] remotePasswd) {
        if (plain) {
            return "YES";
        }
        return remotePasswd.length == 0 ? "NO" : "YES";
    }

    private boolean comparePassword(Password curUserPassword, byte[] remotePasswd,
            byte[] randomString, String remotePasswdStr, boolean plain) {
        // check password
        if (plain) {
            return MysqlPassword.checkPlainPass(curUserPassword.getPassword(), remotePasswdStr);
        } else {
            byte[] saltPassword = MysqlPassword.getSaltFromPassword(curUserPassword.getPassword());
            // when the length of password is zero, the user has no password
            return ((remotePasswd.length == saltPassword.length)
                    && (remotePasswd.length == 0
                    || MysqlPassword.checkScramble(remotePasswd, randomString, saltPassword)));
        }
    }


    public void clearEntriesSetByResolver() {
        wlock.lock();
        try {
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
        } finally {
            wlock.unlock();
        }
    }

    public User createUser(UserIdentity userIdent, byte[] pwd, UserIdentity domainUserIdent, boolean setByResolver,
                           String comment) throws PatternMatcherException {
        wlock.lock();
        try {
            return createUserWithoutLock(userIdent, pwd, domainUserIdent, setByResolver, comment);
        } finally {
            wlock.unlock();
        }
    }

    public User createUserWithoutLock(UserIdentity userIdent, byte[] pwd, UserIdentity domainUserIdent,
                                      boolean setByResolver, String comment)
            throws PatternMatcherException {
        if (userIdentityExistWithoutLock(userIdent, true)) {
            User userByUserIdentity = getUserByUserIdentityWithoutLock(userIdent);
            if (!userByUserIdentity.isSetByDomainResolver() && setByResolver) {
                // If the user is NOT created by domain resolver,
                // and the current operation is done by DomainResolver,
                // we should not override it, just return
                return userByUserIdentity;
            }
            userByUserIdentity.setPassword(pwd);
            userByUserIdentity.setComment(comment);
            userByUserIdentity.setSetByDomainResolver(setByResolver);
            return userByUserIdentity;
        }

        PatternMatcher hostPattern = PatternMatcher
                .createMysqlPattern(userIdent.getHost(), CaseSensibility.HOST.getCaseSensibility());
        User user = new User(userIdent, pwd, setByResolver, domainUserIdent, hostPattern, comment);
        List<User> nameToLists = nameToUsers.get(userIdent.getQualifiedUser());
        if (CollectionUtils.isEmpty(nameToLists)) {
            nameToLists = Lists.newArrayList(user);
            nameToUsers.put(userIdent.getQualifiedUser(), nameToLists);
        } else {
            nameToLists.add(user);
            Collections.sort(nameToLists);
        }
        return user;

    }

    public User getUserByUserIdentity(UserIdentity userIdent) {
        rlock.lock();
        try {
            return getUserByUserIdentityWithoutLock(userIdent);
        } finally {
            rlock.unlock();
        }
    }

    public User getUserByUserIdentityWithoutLock(UserIdentity userIdent) {
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
        wlock.lock();
        try {
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
        } finally {
            wlock.unlock();
        }
    }

    public Map<String, List<User>> getNameToUsers() {
        rlock.lock();
        try {
            return ImmutableMap.copyOf(nameToUsers);
        } finally {
            rlock.unlock();
        }
    }

    public void setPassword(UserIdentity userIdentity, byte[] password, boolean errOnNonExist) throws DdlException {
        User user = getUserByUserIdentity(userIdentity);
        if (user == null) {
            if (errOnNonExist) {
                throw new DdlException("user " + userIdentity + " does not exist");
            }
            return;
        }
        user.setPassword(password);
    }

    public void getAllDomains(Set<String> allDomains) {
        rlock.lock();
        try {
            for (Entry<String, List<User>> entry : nameToUsers.entrySet()) {
                for (User user : entry.getValue()) {
                    if (user.getUserIdentity().isDomain()) {
                        allDomains.add(user.getUserIdentity().getHost());
                    }
                }
            }
        } finally {
            rlock.unlock();
        }
    }

    // handle new resolved IPs.
    // it will only modify password entry of these resolved IPs. All other privileges are binded
    // to the domain, so no need to modify.
    public void addUserPrivEntriesByResolvedIPs(Map<String, Set<String>> resolvedIPsMap) {
        wlock.lock();
        try {
            for (Entry<String, List<User>> userEntry : nameToUsers.entrySet()) {
                for (Map.Entry<String, Set<String>> entry : resolvedIPsMap.entrySet()) {
                    User domainUser = getDomainUser(userEntry.getValue(), entry.getKey());
                    if (domainUser == null) {
                        continue;
                    }
                    // this user ident will be saved along with each resolved "IP" user ident, so that when checking
                    // password, this "domain" user ident will be returned as "current user".
                    for (String newIP : entry.getValue()) {
                        UserIdentity userIdent = UserIdentity.createAnalyzedUserIdentWithIp(userEntry.getKey(), newIP);
                        byte[] password = domainUser.getPassword().getPassword();
                        Preconditions.checkNotNull(password, entry.getKey());
                        try {
                            createUserWithoutLock(userIdent, password, domainUser.getUserIdentity(), true, "");
                        } catch (PatternMatcherException e) {
                            LOG.info("failed to create user for user ident: {}, {}", userIdent, e.getMessage());
                        }
                    }
                }
            }
        } finally {
            wlock.unlock();
        }
    }

    private User getDomainUser(List<User> users, String domain) {
        for (User user : users) {
            if (user.getUserIdentity().isDomain() && user.getUserIdentity().getHost().equals(domain)) {
                return user;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        rlock.lock();
        try {
            return nameToUsers.toString();
        } finally {
            rlock.unlock();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static UserManager read(DataInput in) throws IOException {
        String json = Text.readString(in);
        UserManager um = GsonUtils.GSON.fromJson(json, UserManager.class);
        return um;
    }

    // should be removed after version 3.0
    private void removeClusterPrefix() {
        Map<String, List<User>> newNameToUsers = Maps.newHashMap();
        wlock.lock();
        try {
            for (Entry<String, List<User>> entry : nameToUsers.entrySet()) {
                String user = entry.getKey();
                newNameToUsers.put(ClusterNamespace.getNameFromFullName(user), entry.getValue());
            }
            this.nameToUsers = newNameToUsers;
        } finally {
            wlock.unlock();
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        removeClusterPrefix();
    }

    // ====== CLOUD ======
    public Set<String> getAllUsers() {
        rlock.lock();
        try {
            return new HashSet<>(nameToUsers.keySet());
        } finally {
            rlock.unlock();
        }
    }

    public String getUserId(String userName) {
        rlock.lock();
        try {
            if (!nameToUsers.containsKey(userName)) {
                LOG.warn("can't find userName {} 's userId, nameToUsers {}", userName, nameToUsers);
                return "";
            }
            List<User> users = nameToUsers.get(userName);
            if (users.isEmpty()) {
                LOG.warn("userName {}  empty users in map {}", userName, nameToUsers);
            }
            // here, all the users has same userid, just return one
            String userId = users.stream().map(User::getUserId).filter(Strings::isNotEmpty).findFirst().orElse("");
            LOG.debug("userName {}, userId {}, map {}", userName, userId, nameToUsers);
            return userId;
        } finally {
            rlock.unlock();
        }
    }

    // ====== CLOUD =====
}
