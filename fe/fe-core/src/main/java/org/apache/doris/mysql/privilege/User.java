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
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class User implements Comparable<User>, Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(User.class);
    @SerializedName(value = "userIdentity")
    private UserIdentity userIdentity;
    private UserIdentity domainUserIdentity;
    private boolean isSetByDomainResolver = false;
    // host is not case sensitive
    protected PatternMatcher hostPattern;
    protected boolean isAnyHost = false;
    @SerializedName(value = "password")
    private Password password;

    @SerializedName(value = "userid")
    private String origUserId = "";

    @SerializedName(value = "comment")
    private String comment;

    public User() {
    }

    public User(UserIdentity userIdent, byte[] pwd, boolean setByResolver, UserIdentity domainUserIdent,
            PatternMatcher hostPattern, String comment) {
        this.isAnyHost = userIdent.getHost().equals(UserManager.ANY_HOST);
        this.userIdentity = userIdent;
        this.password = new Password(pwd);
        this.hostPattern = hostPattern;
        this.isSetByDomainResolver = setByResolver;
        if (setByResolver) {
            Preconditions.checkNotNull(domainUserIdent);
            this.domainUserIdentity = domainUserIdent;
        }
        this.comment = comment;
    }

    // ====== CLOUD ======
    public String getUserId() {
        return origUserId;
    }

    public void setUserId(String userId) {
        this.origUserId = userId;
    }
    // ====== CLOUD ======


    public Password getPassword() {
        return password;
    }

    public void setPassword(Password password) {
        this.password = password;
    }

    public void setPassword(byte[] password) {
        this.password = new Password(password);
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

    public boolean hasPassword() {
        return password != null && password.getPassword() != null && password.getPassword().length != 0;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public int compareTo(@NotNull User o) {
        return -userIdentity.getHost().compareTo(o.userIdentity.getHost());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("userIdentity: ").append(userIdentity).append(", isSetByDomainResolver: ")
                .append(isSetByDomainResolver).append(", domainUserIdentity: ").append(domainUserIdentity)
            .append(", userId: ").append(origUserId);
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static User read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, User.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        try {
            hostPattern = PatternMatcher
                    .createMysqlPattern(userIdentity.getHost(), CaseSensibility.HOST.getCaseSensibility());
        } catch (PatternMatcherException e) {
            // will not happen
            LOG.warn("readFields error,", e);
        }
        isAnyHost = userIdentity.getHost().equals(UserManager.ANY_HOST);
    }
}
