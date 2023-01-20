package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.io.Writable;

import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class UserRoleManager implements Writable {
    private Map<UserIdentity, Set<String>> userToRoles = Maps.newConcurrentMap();
    private Map<String, Set<UserIdentity>> roleToUsers = Maps.newConcurrentMap();

    public void addUserRole(UserIdentity user, String roleName) {

    }

    public void dropUserRole(UserIdentity user, String roleName) {

    }

    public void dropUser(UserIdentity user) {
    }

    public void dropRole(String roleName) {

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
