package org.apache.doris.mysql.rbac;

import org.apache.doris.analysis.UserIdentity;

import org.jetbrains.annotations.NotNull;

public class User implements Comparable<User> {
    private UserIdentity domainUserIdentity;
    private boolean isSetByDomainResolver = false;

    private Password password;


    public Password getPassword() {
        return password;
    }

    public void setPassword(Password password) {
        this.password = password;
    }


    @Override
    public int compareTo(@NotNull User o) {
        return 0;
    }
}
