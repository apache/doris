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
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import org.apache.commons.lang.NotImplementedException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public abstract class PrivEntry implements Comparable<PrivEntry>, Writable {
    protected static final String ANY_HOST = "%";
    protected static final String ANY_USER = "%";

    // host is not case sensitive
    protected PatternMatcher hostPattern;
    protected String origHost;
    protected boolean isAnyHost = false;
    // user name is case sensitive
    protected PatternMatcher userPattern;
    protected String origUser;
    protected boolean isAnyUser = false;
    protected PrivBitSet privSet;
    // true if this entry is set by domain resolver
    protected boolean isSetByDomainResolver = false;
    // true if origHost is a domain name.
    // For global priv entry, if isDomain is true, it should only be used for priv checking, not password checking
    protected boolean isDomain = false;

    // isClassNameWrote to guarantee the class name can only be written once when persisting.
    // see PrivEntry.read() for more details.
    protected boolean isClassNameWrote = false;

    private UserIdentity userIdentity;

    protected PrivEntry() {
    }

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

    public PatternMatcher getHostPattern() {
        return hostPattern;
    }

    public String getOrigHost() {
        return origHost;
    }

    public boolean isAnyHost() {
        return isAnyHost;
    }

    public PatternMatcher getUserPattern() {
        return userPattern;
    }

    public String getOrigUser() {
        return origUser;
    }

    public boolean isAnyUser() {
        return isAnyUser;
    }

    public PrivBitSet getPrivSet() {
        return privSet;
    }
    
    public void setPrivSet(PrivBitSet privSet) {
        this.privSet = privSet;
    }

    public boolean isSetByDomainResolver() {
        return isSetByDomainResolver;
    }
    
    public void setSetByDomainResolver(boolean isSetByDomainResolver) {
        this.isSetByDomainResolver = isSetByDomainResolver;
    }

    public UserIdentity getUserIdent() {
        return userIdentity;
    }

    public boolean match(UserIdentity userIdent, boolean exactMatch) {
        if (exactMatch) {
            return origUser.equals(userIdent.getQualifiedUser()) && origHost.equals(userIdent.getHost());
        } else {
            return origUser.equals(userIdent.getQualifiedUser()) && hostPattern.match(userIdent.getHost());
        }
    }

    public abstract boolean keyMatch(PrivEntry other);

    /*
     * It's a bit complicated when persisting instance which its class has derived classes.
     * eg: A (top class) -> B (derived) -> C (derived)
     * 
     * Write process:
     * C.write()
     *      |
     *      --- write class name
     *      |
     *      --- super.write()    -----> B.write()
     *      |                               |
     *      --- write C's self members      --- write class name (if not write before)
     *                                      |
     *                                      --- super.write()    -----> A.write()
     *                                      |                               |
     *                                      --- write B's self members      --- write class name (if not write before)
     *                                                                      |
     *                                                                      --- write A's self members
     *                                                                                                                                               
     * So the final write order is:
     *      1. C's class name
     *      2. A's self members
     *      3. B's self members
     *      4. C's self members
     *      
     * In case that class name should only be wrote once, we use isClassNameWrote flag.
     * 
     * Read process:
     * static A.read()
     *      |
     *      --- read class name and instantiated the class instance (eg. C class)
     *      |
     *      --- C.readFields()
     *          |
     *          --- super.readFields() --> B.readFields()
     *          |                           |
     *          --- read C's self members   --- super.readFields() --> A.readFields()
     *                                      |                           |
     *                                      --- read B's self members   --- read A's self members
     *                                      
     *  So the final read order is:
     *      1. C's class name
     *      2. A's self members
     *      3. B's self members
     *      4. C's self members
     *      
     *  Which is same as Write order.
     */
    public static PrivEntry read(DataInput in) throws IOException {
        String className = Text.readString(in);
        if (className.startsWith("com.baidu.palo")) {
            // we need to be compatible with former class name
            className = className.replaceFirst("com.baidu.palo", "org.apache.doris");
        }
        PrivEntry privEntry = null;
        try {
            Class<? extends PrivEntry> derivedClass = (Class<? extends PrivEntry>) Class.forName(className);
            privEntry = derivedClass.newInstance();
            Class[] paramTypes = { DataInput.class };
            Method readMethod = derivedClass.getMethod("readFields", paramTypes);
            Object[] params = { in };
            readMethod.invoke(privEntry, params);

            return privEntry;
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException
                | SecurityException | IllegalArgumentException | InvocationTargetException e) {
            throw new IOException("failed read PrivEntry", e);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!isClassNameWrote) {
            String className = PrivEntry.class.getCanonicalName();
            Text.writeString(out, className);
            isClassNameWrote = true;
        }
        Text.writeString(out, origHost);
        Text.writeString(out, origUser);
        privSet.write(out);

        out.writeBoolean(isSetByDomainResolver);
        out.writeBoolean(isDomain);

        isClassNameWrote = false;
    }

    public void readFields(DataInput in) throws IOException {
        origHost = Text.readString(in);
        try {
            hostPattern = PatternMatcher.createMysqlPattern(origHost, CaseSensibility.HOST.getCaseSensibility());
        } catch (AnalysisException e) {
            throw new IOException(e);
        }
        isAnyHost = origHost.equals(ANY_HOST);

        origUser = Text.readString(in);
        try {
            userPattern = PatternMatcher.createMysqlPattern(origUser, CaseSensibility.USER.getCaseSensibility());
        } catch (AnalysisException e) {
            throw new IOException(e);
        }
        isAnyUser = origUser.equals(ANY_USER);
        privSet = PrivBitSet.read(in);
        isSetByDomainResolver = in.readBoolean();
        isDomain = in.readBoolean();

        if (isDomain) {
            userIdentity = UserIdentity.createAnalyzedUserIdentWithDomain(origUser, origHost);
        } else {
            userIdentity = UserIdentity.createAnalyzedUserIdentWithIp(origUser, origHost);
        }
    }

    @Override
    public int compareTo(PrivEntry o) {
        throw new NotImplementedException();
    }
}
