package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.PatternMatcher;

import org.jetbrains.annotations.NotNull;

public class User implements Comparable<User> {
    private UserIdentity userIdentity;
    private UserIdentity domainUserIdentity;
    private boolean isSetByDomainResolver = false;
    // host is not case sensitive
    protected PatternMatcher hostPattern;
    protected boolean isAnyHost = false;
    private Password password;


    public Password getPassword() {
        return password;
    }

    public void setPassword(Password password) {
        this.password = password;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public void setUserIdentity(UserIdentity userIdentity) {
        this.userIdentity = userIdentity;
    }

    public UserIdentity getDomainUserIdentity() {
        if (isSetByDomainResolver()) {
            return domainUserIdentity;
        } else {
            return userIdentity;
        }

    }

    public void setDomainUserIdentity(UserIdentity domainUserIdentity) {
        this.domainUserIdentity = domainUserIdentity;
    }

    public boolean isSetByDomainResolver() {
        return isSetByDomainResolver;
    }

    public void setSetByDomainResolver(boolean setByDomainResolver) {
        isSetByDomainResolver = setByDomainResolver;
    }

    public PatternMatcher getHostPattern() {
        return hostPattern;
    }

    public void setHostPattern(PatternMatcher hostPattern) {
        this.hostPattern = hostPattern;
    }

    public boolean isAnyHost() {
        return isAnyHost;
    }

    public void setAnyHost(boolean anyHost) {
        isAnyHost = anyHost;
    }

    @Override
    public int compareTo(@NotNull User o) {
        return 0;
    }
}
