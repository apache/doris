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

package org.apache.doris.analysis;

import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.mysql.privilege.PaloAuth;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// https://dev.mysql.com/doc/refman/8.0/en/account-names.html
// user name must be literally matched.
// host name can take many forms, and wildcards are permitted.
// cmy@%
// cmy@192.168.%
// cmy@[domain.name]
public class UserIdentity implements Writable {
    private String user;
    private String host;
    private boolean isDomain;
    private boolean isAnalyzed = false;

    public static final UserIdentity ROOT;
    public static final UserIdentity ADMIN;

    static {
        ROOT = new UserIdentity(PaloAuth.ROOT_USER, "%");
        ROOT.setIsAnalyzed();
        ADMIN = new UserIdentity(PaloAuth.ADMIN_USER, "%");
        ADMIN.setIsAnalyzed();
    }

    private UserIdentity() {
    }

    public UserIdentity(String user, String host) {
        this.user = Strings.emptyToNull(user);
        this.host = Strings.emptyToNull(host);
        this.isDomain = false;
    }

    public UserIdentity(String user, String host, boolean isDomain) {
        this.user = Strings.emptyToNull(user);
        this.host = Strings.emptyToNull(host);
        this.isDomain = isDomain;
    }

    public String getQualifiedUser() {
        Preconditions.checkState(isAnalyzed);
        return user;
    }

    public String getHost() {
        return host;
    }

    public boolean isDomain() {
        return isDomain;
    }

    public void setIsAnalyzed() {
        this.isAnalyzed = true;
    }

    public void analyze(String clusterName) throws AnalysisException {
        if (isAnalyzed) {
            return;
        }
        if (Strings.isNullOrEmpty(user)) {
            throw new AnalysisException("Does not support anonymous user");
        }

        FeNameFormat.checkUserName(user);
        if (!user.equals(PaloAuth.ROOT_USER) && !user.equals(PaloAuth.ADMIN_USER)) {
            user = ClusterNamespace.getFullName(clusterName, user);
        }

        // reuse createMysqlPattern to validate host pattern
        PatternMatcher.createMysqlPattern(host, CaseSensibility.HOST.getCaseSensibility());
        isAnalyzed = true;
    }

    public boolean include(UserIdentity self) {
        Preconditions.checkState(isAnalyzed && self.isAnalyzed);
        if (!user.equals(self.user)) {
            return false;
        }
        
        if (host.equals(self.host)) {
            return true;
        }

        // same user with different host
        try {
            PatternMatcher patternMatcher = PatternMatcher.createMysqlPattern(host,
                                                                              CaseSensibility.HOST.getCaseSensibility());
            
            return patternMatcher.match(self.host);
        } catch (AnalysisException e) {
            Preconditions.checkNotNull(null, e.getMessage());
        }
        return false;
    }

    public static UserIdentity fromString(String userIdentStr) {
        if (Strings.isNullOrEmpty(userIdentStr)) {
            return null;
        }

        String[] parts = userIdentStr.split("@");
        if (parts.length != 2) {
            return null;
        }

        String user = parts[0];
        if (!user.startsWith("'") || !user.endsWith("'")) {
            return null;
        }

        String host = parts[1];
        if (host.startsWith("['") && host.endsWith("']")) {
            UserIdentity userIdent = new UserIdentity(user.substring(1, user.length() - 1),
                    host.substring(2, host.length() - 2), true);
            userIdent.setIsAnalyzed();
            return userIdent;
        } else if (host.startsWith("'") && host.endsWith("'")) {
            UserIdentity userIdent = new UserIdentity(user.substring(1, user.length() - 1),
                    host.substring(1, host.length() - 1));
            userIdent.setIsAnalyzed();
            return userIdent;
        }

        return null;
    }

    public static UserIdentity read(DataInput in) throws IOException {
        UserIdentity userIdentity = new UserIdentity();
        userIdentity.readFields(in);
        return userIdentity;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof UserIdentity)) {
            return false;
        }
        UserIdentity other = (UserIdentity) obj;
        if (this.isDomain != other.isDomain) {
            return false;
        }
        return user.equals(other.getQualifiedUser()) && host.equals(other.getHost());
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + user.hashCode();
        result = 31 * result + host.hashCode();
        result = 31 * result + Boolean.valueOf(isDomain).hashCode();
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("'");
        if (!Strings.isNullOrEmpty(user)) {
            sb.append(user);
        }
        sb.append("'@");
        if (!Strings.isNullOrEmpty(host)) {
            if (isDomain) {
                sb.append("['").append(host).append("']");
            } else {
                sb.append("'").append(host).append("'");
            }
        }
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Preconditions.checkState(isAnalyzed);
        Text.writeString(out, user);
        Text.writeString(out, host);
        out.writeBoolean(isDomain);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        user = Text.readString(in);
        host = Text.readString(in);
        isDomain = in.readBoolean();
        isAnalyzed = true;
    }
}
