package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.UserIdentity;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UserManager {
    Set<User> users = new HashSet<>();
    Map<String, List<User>> nameToUsers = new HashMap<>();

    public boolean userIdentityExist(User user) {
        return users.contains(user);
    }

    public Set<User> getUsers() {
        return users;
    }

    public void addUserIdentity(UserIdentity userIdentity) {

    }

    public List<User> getUserByName(String name) {
        return nameToUsers.get(name);
    }

    public void checkPassword(String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString,
            List<UserIdentity> currentUser) {
        List<User> userIdentities = nameToUsers.get(remoteUser);
        //return match best UserIdentity
    }

    public void checkPlainPassword(String remoteUser, String remoteHost, String remotePasswd,
            List<UserIdentity> currentUser) {
        //return match best UserIdentity
    }

    public void clearEntriesSetByResolver() {
        // delete user which isSetByDomainResolver is true
    }
}
