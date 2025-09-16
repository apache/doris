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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherException;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.NotImplementedException;

public abstract class PrivEntry implements Comparable<PrivEntry> {
    @Deprecated
    protected static final String ANY_HOST = "%";
    @Deprecated
    protected static final String ANY_USER = "%";

    protected PrivBitSet privSet;

    // host is not case sensitive
    @Deprecated
    protected PatternMatcher hostPattern;
    @Deprecated
    protected String origHost;
    @Deprecated
    protected boolean isAnyHost = false;
    // user name is case sensitive
    @Deprecated
    protected PatternMatcher userPattern;
    @Deprecated
    protected String origUser;
    @Deprecated
    protected boolean isAnyUser = false;
    // true if this entry is set by domain resolver
    @Deprecated
    protected boolean isSetByDomainResolver = false;
    // true if origHost is a domain name.
    // For global priv entry, if isDomain is true, it should only be used for priv checking, not password checking
    @Deprecated
    protected boolean isDomain = false;

    // isClassNameWrote to guarantee the class name can only be written once when persisting.
    // see PrivEntry.read() for more details.
    @Deprecated
    protected boolean isClassNameWrote = false;
    @Deprecated
    protected UserIdentity userIdentity;

    protected PrivEntry() {
    }

    protected PrivEntry(PrivBitSet privSet) {
        this.privSet = privSet;
    }

    public PrivBitSet getPrivSet() {
        return privSet;
    }

    public void setPrivSet(PrivBitSet privSet) {
        this.privSet = privSet;
    }

    @Deprecated
    public void setSetByDomainResolver(boolean isSetByDomainResolver) {
        this.isSetByDomainResolver = isSetByDomainResolver;
    }

    public abstract boolean keyMatch(PrivEntry other);

    @Deprecated
    protected PrivEntry(PatternMatcher hostPattern, String origHost, PatternMatcher userPattern, String origUser,
            boolean isDomain, PrivBitSet privSet) {
        this.hostPattern = hostPattern;
        this.origHost = origHost;
        if (origHost.equals(ANY_HOST)) {
            isAnyHost = true;
        }
        this.userPattern = userPattern;
        this.origUser = origUser;
        if (origUser.equals(ANY_USER)) {
            isAnyUser = true;
        }
        this.isDomain = isDomain;
        this.privSet = privSet;
        if (isDomain) {
            userIdentity = UserIdentity.createAnalyzedUserIdentWithDomain(origUser, origHost);
        } else {
            userIdentity = UserIdentity.createAnalyzedUserIdentWithIp(origUser, origHost);
        }
    }

    @Deprecated
    public boolean match(UserIdentity userIdent, boolean exactMatch) {
        if (exactMatch) {
            return origUser.equals(userIdent.getQualifiedUser()) && origHost.equals(userIdent.getHost());
        } else {
            return origUser.equals(userIdent.getQualifiedUser()) && hostPattern.match(userIdent.getHost());
        }
    }

    @Override
    public int compareTo(PrivEntry o) {
        throw new NotImplementedException("should be implemented by derived class");
    }

    /**
     * Help derived classes compare in the order of 'user', 'host', 'catalog', 'db', 'ctl'.
     * Compare strings[i] with strings[i+1] successively, return if the comparison value is not 0 in current loop.
     */
    protected static int compareAssist(String... strings) {
        Preconditions.checkState(strings.length % 2 == 0);
        for (int i = 0; i < strings.length; i += 2) {
            int res = strings[i].compareTo(strings[i + 1]);
            if (res != 0) {
                return res;
            }
        }
        return 0;
    }

    protected abstract PrivEntry copy() throws AnalysisException, PatternMatcherException;
}
