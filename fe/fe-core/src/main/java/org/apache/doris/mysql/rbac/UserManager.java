package org.apache.doris.mysql.rbac;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UserManager {
    Set<UserIdentity> users = new HashSet<>();
    Map<String, List<UserIdentity>> userToUserIdentitys = new HashMap<>();

    public boolean userIdentityExist(UserIdentity userIdentity) {
        return users.contains(userIdentity);
    }

    public Set<UserIdentity> getUserIdentitys() {
        return users;
    }

    public void addUserIdentity(UserIdentity userIdentity) {

    }

    public List<UserIdentity> getUserIdentityByName (String name) {
        return userToUserIdentitys.get(name);
    }
    public void checkPassword(String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString, List<org.apache.doris.analysis.UserIdentity> currentUser) {
        List<UserIdentity> userIdentities = userToUserIdentitys.get(remoteUser);
        //return match best UserIdentity
    }

    public void checkPlainPassword(String remoteUser, String remoteHost, String remotePasswd,
            List<org.apache.doris.analysis.UserIdentity> currentUser) {
        //return match best UserIdentity
    }

    public void clearEntriesSetByResolver() {
        // delete user which isSetByDomainResolver is true
    }
}
