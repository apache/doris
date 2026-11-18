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
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.mysql.MysqlPassword;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.validator.routines.InetAddressValidator;
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

public class UserManager implements Writable {
    public static final String ANY_HOST = "%";
    private static final Logger LOG = LogManager.getLogger(UserManager.class);

    private static final MonitoredReentrantReadWriteLock rwLock = new MonitoredReentrantReadWriteLock(false);
    private static final Lock rlock = rwLock.readLock();
    private static final Lock wlock = rwLock.writeLock();

    // One name may have multiple User,because host can be different
    @SerializedName(value = "nameToUsers")
    private Map<String, List<User>> nameToUsers = Maps.newHashMap();

    // Absent in images written before secure cluster initialization was introduced.
    @SerializedName(value = "secureInitialization")
    private volatile boolean secureInitialization;

    public boolean isSecureInitialization() {
        return secureInitialization;
    }

    public void setSecureInitialization() {
        secureInitialization = true;
    }

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
            return users == null ? Collections.emptyList() : new ArrayList<>(users);
        } finally {
            rlock.unlock();
        }
    }

    public boolean checkPassword(String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString,
            List<UserIdentity> currentUser, AuthenticationProtocol protocol) throws AuthenticationException {
        return checkPasswordInternal(remoteUser, remoteHost, remotePasswd, randomString, null,
                currentUser, false, protocol);
    }

    public void checkPlainPassword(String remoteUser, String remoteHost, String remotePasswd,
            List<UserIdentity> currentUser) throws AuthenticationException {
        checkPasswordInternal(remoteUser, remoteHost, null, null, remotePasswd, currentUser, true,
                AuthenticationProtocol.OTHER);
    }

    public boolean checkPasswordForUserIdentity(UserIdentity userIdentity, String remoteHost,
            byte[] remotePasswd, byte[] randomString, List<UserIdentity> currentUser,
            AuthenticationProtocol protocol) throws AuthenticationException {
        return checkPasswordInternalForUser(userIdentity, remoteHost, remotePasswd, randomString, null,
                currentUser, false, protocol);
    }

    public void checkPlainPasswordForUserIdentity(UserIdentity userIdentity, String remotePasswd,
            List<UserIdentity> currentUser) throws AuthenticationException {
        checkPasswordInternalForUser(userIdentity, null, null, null, remotePasswd, currentUser, true,
                AuthenticationProtocol.OTHER);
    }

    private boolean checkPasswordInternal(String remoteUser, String remoteHost,
            byte[] remotePasswd, byte[] randomString,
            String remotePasswdStr, List<UserIdentity> currentUser, boolean plain, AuthenticationProtocol protocol)
            throws AuthenticationException {
        PasswordPolicyManager passwdPolicyMgr = Env.getCurrentEnv().getAuth().getPasswdPolicyManager();
        boolean suppliedPassword = plain ? !Strings.isEmpty(remotePasswdStr) : remotePasswd.length != 0;
        List<User> users;
        rlock.lock();
        try {
            users = nameToUsers.get(remoteUser);
            if (CollectionUtils.isEmpty(users)) {
                throw BootstrapAuthPolicy.accessDenied(remoteUser, remoteHost, suppliedPassword);
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
            if (getAuthenticationUser(user).isAuthenticationBound()) {
                throw BootstrapAuthPolicy.accessDenied(remoteUser, remoteHost, suppliedPassword);
            }
            UserIdentity curUser = user.getDomainUserIdentity();
            if (comparePassword(user.getPassword(), remotePasswd, randomString, remotePasswdStr, plain)) {
                passwdPolicyMgr.checkAccountLockedAndPasswordExpiration(curUser);
                boolean passwordResetRequired = BootstrapAuthPolicy.check(user, secureInitialization,
                        remoteHost, protocol, suppliedPassword);
                if (currentUser != null) {
                    currentUser.add(curUser);
                }
                return passwordResetRequired;
            }
            // A matching user@host with the wrong password must not fall back to a broader host entry.
            passwdPolicyMgr.onFailedLogin(curUser);
            throw BootstrapAuthPolicy.accessDenied(remoteUser, remoteHost, suppliedPassword);
        }
        throw BootstrapAuthPolicy.accessDenied(remoteUser, remoteHost, suppliedPassword);
    }

    private boolean checkPasswordInternalForUser(UserIdentity userIdentity, String remoteHost,
            byte[] remotePasswd, byte[] randomString, String remotePasswdStr, List<UserIdentity> currentUser,
            boolean plain, AuthenticationProtocol protocol) throws AuthenticationException {
        PasswordPolicyManager passwdPolicyMgr = Env.getCurrentEnv().getAuth().getPasswdPolicyManager();
        boolean suppliedPassword = plain ? !Strings.isEmpty(remotePasswdStr) : remotePasswd.length != 0;
        User user = getUserByUserIdentity(userIdentity);
        if (user == null || getAuthenticationUser(user).isAuthenticationBound()) {
            throw BootstrapAuthPolicy.accessDenied(userIdentity.getQualifiedUser(), remoteHost, suppliedPassword);
        }
        UserIdentity currentIdentity = user.getDomainUserIdentity();
        if (comparePassword(user.getPassword(), remotePasswd, randomString, remotePasswdStr, plain)) {
            passwdPolicyMgr.checkAccountLockedAndPasswordExpiration(currentIdentity);
            boolean passwordResetRequired = BootstrapAuthPolicy.check(user, secureInitialization,
                    remoteHost, protocol, suppliedPassword);
            if (currentUser != null) {
                currentUser.add(currentIdentity);
            }
            return passwordResetRequired;
        }
        passwdPolicyMgr.onFailedLogin(currentIdentity);
        throw BootstrapAuthPolicy.accessDenied(userIdentity.getQualifiedUser(), remoteHost, suppliedPassword);
    }

    public User getAuthenticationUser(User user) {
        return user.isSetByDomainResolver() ? getUserByUserIdentity(user.getDomainUserIdentity()) : user;
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
                        Env.getCurrentEnv().getAuth().getPasswdPolicyManager().dropUser(user.getUserIdentity());
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
            if (userByUserIdentity.isSetByDomainResolver() && !setByResolver) {
                // An explicit account starts a new policy lifetime, independent of the domain.
                Env.getCurrentEnv().getAuth().getPasswdPolicyManager().dropUser(userIdent);
            }
            userByUserIdentity.setPassword(pwd);
            userByUserIdentity.setComment(comment);
            userByUserIdentity.setSetByDomainResolver(setByResolver);
            userByUserIdentity.setDomainUserIdentity(domainUserIdent);
            userByUserIdentity.setUserIdentity(userIdent);
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
                if (user.getUserIdentity().equals(userIdent)
                        || (user.isSetByDomainResolver() && userIdent.equals(user.getDomainUserIdentity()))) {
                    if (user.isSetByDomainResolver()) {
                        Env.getCurrentEnv().getAuth().getPasswdPolicyManager().dropUser(user.getUserIdentity());
                    }
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

    /** Keep already resolved domain entries consistent with the account's binding, TLS and manual lock. */
    public void refreshAuthenticationBinding(UserIdentity identity) {
        wlock.lock();
        try {
            User domain = getUserByUserIdentityWithoutLock(identity);
            PasswordPolicyManager policies = Env.getCurrentEnv().getAuth().getPasswdPolicyManager();
            for (User user : nameToUsers.get(identity.getQualifiedUser())) {
                if (user.isSetByDomainResolver() && identity.equals(user.getDomainUserIdentity())) {
                    user.setAuthenticationBinding(domain.getAuthIntegration(), domain.getAuthIdentity());
                    user.setPassword(domain.getPassword().getPassword());
                    user.setUserIdentity(createResolvedUserIdentity(identity.getQualifiedUser(),
                            user.getUserIdentity().getHost(), domain.getUserIdentity()));
                    user.setDomainUserIdentity(domain.getUserIdentity());
                    if (policies.isAccountLocked(identity)) {
                        policies.lockUser(user.getUserIdentity());
                    } else {
                        policies.unlockUser(user.getUserIdentity());
                    }
                }
            }
        } finally {
            wlock.unlock();
        }
    }

    public void checkDomainAuthenticationBinding(UserIdentity identity) throws DdlException {
        if (!identity.isDomain()) {
            return;
        }
        List<UserIdentity> ambiguous = new ArrayList<>();
        for (User user : getUserByName(identity.getQualifiedUser())) {
            if (!user.getUserIdentity().isDomain() && !user.hasKnownDomainResolverOrigin()
                    && InetAddressValidator.getInstance().isValid(user.getUserIdentity().getHost())) {
                ambiguous.add(user.getUserIdentity());
            }
        }
        if (!ambiguous.isEmpty()) {
            throw new DdlException("Cannot bind domain account " + identity
                    + ": legacy image does not record resolver origin for " + ambiguous
                    + "; drop stale aliases or recreate these IP accounts explicitly before binding");
        }
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
                        UserIdentity domainUserIdent = domainUser.getUserIdentity();
                        UserIdentity userIdent = createResolvedUserIdentity(userEntry.getKey(), newIP, domainUserIdent);
                        byte[] password = domainUser.getPassword().getPassword();
                        Preconditions.checkNotNull(password, entry.getKey());
                        try {
                            User resolved = createUserWithoutLock(userIdent, password,
                                    domainUser.getUserIdentity(), true, "");
                            if (resolved.isSetByDomainResolver()) {
                                resolved.setAuthenticationBinding(domainUser.getAuthIntegration(),
                                        domainUser.getAuthIdentity());
                                if (domainUser.isAuthenticationBound()) {
                                    PasswordPolicyManager policies = Env.getCurrentEnv().getAuth()
                                            .getPasswdPolicyManager();
                                    if (policies.isAccountLocked(domainUserIdent)) {
                                        policies.lockUser(userIdent);
                                    } else {
                                        policies.unlockUser(userIdent);
                                    }
                                }
                            }
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

    private UserIdentity createResolvedUserIdentity(String userName, String ip, UserIdentity domainIdentity) {
        UserIdentity identity = UserIdentity.createAnalyzedUserIdentWithIp(userName, ip);
        identity.setSan(domainIdentity.getSan());
        identity.setIssuer(domainIdentity.getIssuer());
        identity.setCipher(domainIdentity.getCipher());
        identity.setSubject(domainIdentity.getSubject());
        return identity;
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
