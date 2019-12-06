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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GlobalPrivEntry extends PrivEntry {
    private static final Logger LOG = LogManager.getLogger(GlobalPrivEntry.class);

    private byte[] password;
    // set domainUserIdent when this a password entry and is set by domain resolver.
    // so that when user checking password with user@'IP' and match a entry set by the resolver,
    // it should return this domainUserIdent as "current user". And user can use this user ident to get privileges
    // further.
    private UserIdentity domainUserIdent;

    protected GlobalPrivEntry() {
    }

    protected GlobalPrivEntry(PatternMatcher hostPattern, String origHost,
            PatternMatcher userPattern, String origUser, boolean isDomain,
            byte[] password, PrivBitSet privSet) {
        super(hostPattern, origHost, userPattern, origUser, isDomain, privSet);
        this.password = password;
    }

    public static GlobalPrivEntry create(String host, String user, boolean isDomain, byte[] password, PrivBitSet privs)
            throws AnalysisException {
        PatternMatcher hostPattern = PatternMatcher.createMysqlPattern(host, CaseSensibility.HOST.getCaseSensibility());
        PatternMatcher userPattern = PatternMatcher.createMysqlPattern(user, CaseSensibility.USER.getCaseSensibility());
        return new GlobalPrivEntry(hostPattern, host, userPattern, user, isDomain, password, privs);
    }

    public byte[] getPassword() {
        return password;
    }

    public void setPassword(byte[] password) {
        this.password = password;
    }

    public void setDomainUserIdent(UserIdentity domainUserIdent) {
        this.domainUserIdent = domainUserIdent;
    }

    public UserIdentity getDomainUserIdent() {
        if (isSetByDomainResolver()) {
            return domainUserIdent;
        } else {
            return getUserIdent();
        }
    }

    /*
     * UserTable is ordered by Host, User
     * eg:
     * +-----------+----------+-
     * | Host      | User     | ...
     * +-----------+----------+-
     * | %         | root     | ...
     * | %         | jeffrey  | ...
     * | localhost | root     | ...
     * | localhost |          | ...
     * +-----------+----------+-
     * 
     * will be sorted like:
     * 
     * +-----------+----------+-
     * | Host      | User     | ...
     * +-----------+----------+-
     * | localhost | root     | ...
     * | localhost |          | ...
     * | %         | jeffrey  | ...
     * | %         | root     | ...
     * +-----------+----------+-
     * 
     * https://dev.mysql.com/doc/refman/8.0/en/connection-access.html
     */
    @Override
    public int compareTo(PrivEntry other) {
        if (!(other instanceof GlobalPrivEntry)) {
            throw new ClassCastException("cannot cast " + other.getClass().toString() + " to " + this.getClass());
        }

        GlobalPrivEntry otherEntry = (GlobalPrivEntry) other;
        int res = origHost.compareTo(otherEntry.origHost);
        if (res != 0) {
            return -res;
        }

        return -origUser.compareTo(otherEntry.origUser);
    }

    @Override
    public boolean keyMatch(PrivEntry other) {
        if (!(other instanceof GlobalPrivEntry)) {
            return false;
        }

        GlobalPrivEntry otherEntry = (GlobalPrivEntry) other;
        if (origHost.equals(otherEntry.origHost) && origUser.equals(otherEntry.origUser)
                && isDomain == otherEntry.isDomain) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("global priv. host: ").append(origHost).append(", user: ").append(origUser);
        sb.append(", priv: ").append(privSet).append(", set by resolver: ").append(isSetByDomainResolver);
        sb.append(", domain user ident: ").append(domainUserIdent);
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!isClassNameWrote) {
            String className = GlobalPrivEntry.class.getCanonicalName();
            Text.writeString(out, className);
            isClassNameWrote = true;
        }

        LOG.info("global priv: {}", this.toString());
        super.write(out);

        out.writeInt(password.length);
        out.write(password);
        isClassNameWrote = false;
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int passwordLen = in.readInt();
        password = new byte[passwordLen];
        in.readFully(password);
    }
}
