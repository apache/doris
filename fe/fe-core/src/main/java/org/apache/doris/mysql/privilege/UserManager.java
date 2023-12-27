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
import org.apache.doris.mysql.MysqlPassword;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class UserManager implements Writable, GsonPostProcessable {
    public static final String ANY_HOST = "%";
    private static final Logger LOG = LogManager.getLogger(UserManager.class);
    // Concurrency control is delegated by Auth, so not concurrentMap
    //One name may have multiple User,because host can be different
    @SerializedName(value = "nameToUsers")
    private Map<String, List<User>> nameToUsers = Maps.newHashMap();

    public boolean userIdentityExist(UserIdentity userIdentity, boolean includeByDomain) {
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
        List<User> users = nameToUsers.get(name);
        return users == null ? Collections.EMPTY_LIST : users;
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
        List<User> users = nameToUsers.get(remoteUser);
        if (CollectionUtils.isEmpty(users)) {
            throw new AuthenticationException(ErrorCode.ERR_ACCESS_DENIED_ERROR, remoteUser + "@" + remoteHost,
                    "YES");
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
        List<User> users = nameToUsers.getOrDefault(remoteUser, Lists.newArrayList());
        for (User user : users) {
            if (!user.getUserIdentity().isDomain() && (user.isAnyHost() || user.getHostPattern().match(remoteHost))) {
                userIdentities.add(user.getUserIdentity());
            }
        }
        return userIdentities;
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
            throws PatternMatcherException {
        if (userIdentityExist(userIdent, true)) {
            User userByUserIdentity = getUserByUserIdentity(userIdent);
            if (!userByUserIdentity.isSetByDomainResolver() && setByResolver) {
                // If the user is NOT created by domain resolver,
                // and the current operation is done by DomainResolver,
                // we should not override it, just return
                return userByUserIdentity;
            }
            userByUserIdentity.setPassword(pwd);
            userByUserIdentity.setSetByDomainResolver(setByResolver);
            return userByUserIdentity;
        }

        PatternMatcher hostPattern = PatternMatcher
                .createMysqlPattern(userIdent.getHost(), CaseSensibility.HOST.getCaseSensibility());
        User user = new User(userIdent, pwd, setByResolver, domainUserIdent, hostPattern);
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
        for (Entry<String, List<User>> entry : nameToUsers.entrySet()) {
            for (User user : entry.getValue()) {
                if (user.getUserIdentity().isDomain()) {
                    allDomains.add(user.getUserIdentity().getHost());
                }
            }
        }
    }

    // handle new resolved IPs.
    // it will only modify password entry of these resolved IPs. All other privileges are binded
    // to the domain, so no need to modify.
    public void addUserPrivEntriesByResolvedIPs(Map<String, Set<String>> resolvedIPsMap) {
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
                        createUser(userIdent, password, domainUser.getUserIdentity(), true);
                    } catch (PatternMatcherException e) {
                        LOG.info("failed to create user for user ident: {}, {}", userIdent, e.getMessage());
                    }
                }
            }
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
        return nameToUsers.toString();
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
        for (Entry<String, List<User>> entry : nameToUsers.entrySet()) {
            String user = entry.getKey();
            newNameToUsers.put(ClusterNamespace.getNameFromFullName(user), entry.getValue());
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        removeClusterPrefix();
    }
}
