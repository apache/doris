package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.io.Writable;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class UserRoleManager implements Writable {
    private Map<UserIdentity, Set<String>> userToRoles = Maps.newConcurrentMap();
    private Map<String, Set<UserIdentity>> roleToUsers = Maps.newConcurrentMap();

    public void addUserRole(UserIdentity userIdentity, String roleName) {
        Set<String> roles = userToRoles.get(userIdentity);
        if (CollectionUtils.isEmpty(roles)) {
            roles = Sets.newHashSet();
        }
        roles.add(roleName);
        userToRoles.put(userIdentity, roles);
        Set<UserIdentity> userIdentities = roleToUsers.get(roleName);
        if (CollectionUtils.isEmpty(userIdentities)) {
            userIdentities = Sets.newHashSet();
        }
        userIdentities.add(userIdentity);
        roleToUsers.put(roleName, userIdentities);
    }

    public void dropUserRole(UserIdentity user, String roleName) {

    }

    public void dropUser(UserIdentity userIdentity) {
        Set<String> remove = userToRoles.remove(userIdentity);
        for (String roleName : remove) {
            Set<UserIdentity> userIdentities = roleToUsers.get(roleName);
            if (CollectionUtils.isEmpty(userIdentities)) {
                continue;
            }
            userIdentities.remove(userIdentity);
            if (CollectionUtils.isEmpty(userIdentities)) {
                roleToUsers.remove(roleName);
            }
        }
    }

    public void dropRole(String roleName) {
        Set<UserIdentity> remove = roleToUsers.remove(roleName);
        for (UserIdentity userIdentity : remove) {
            Set<String> roles = userToRoles.get(userIdentity);
            if (CollectionUtils.isEmpty(roles)) {
                continue;
            }
            roles.remove(roleName);
            if (CollectionUtils.isEmpty(roles)) {
                userToRoles.remove(userIdentity);
            }
        }
    }

    public Set<String> getRolesByUser(UserIdentity user) {
        return null;
    }

    public Set<UserIdentity> getUsersByRole(String roleName) {
        return null;
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    public static UserRoleManager read(DataInput in) throws IOException {
        UserRoleManager userRoleManager = new UserRoleManager();
        return userRoleManager;
    }
}
