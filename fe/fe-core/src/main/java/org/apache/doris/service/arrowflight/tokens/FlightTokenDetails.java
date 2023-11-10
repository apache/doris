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

package org.apache.doris.service.arrowflight.tokens;

import org.apache.doris.analysis.UserIdentity;

import com.google.common.base.Preconditions;

/**
 * Details of a token.
 */
public final class FlightTokenDetails {

    private final String token;
    private final String username;
    private final long issuedAt;
    private final long expiresAt;
    private final String remoteIp;
    private final UserIdentity userIdentity;
    private boolean createdSession = false;

    public FlightTokenDetails() {
        this.token = "";
        this.username = "";
        this.issuedAt = 0;
        this.expiresAt = 0;
        this.remoteIp = "";
        this.userIdentity = new UserIdentity(username, remoteIp);
    }

    public FlightTokenDetails(String token, String username, long issuedAt, long expiresAt, UserIdentity userIdentity,
            String remoteIp) {
        Preconditions.checkNotNull(token);
        Preconditions.checkNotNull(username);
        this.token = token;
        this.username = username;
        this.issuedAt = issuedAt;
        this.expiresAt = expiresAt;
        this.remoteIp = remoteIp;
        this.userIdentity = userIdentity;
    }

    public FlightTokenDetails(String token, String username, long expiresAt) {
        Preconditions.checkNotNull(token);
        Preconditions.checkNotNull(username);
        this.token = token;
        this.username = username;
        this.expiresAt = expiresAt;
        this.issuedAt = 0;
        this.remoteIp = "";
        this.userIdentity = new UserIdentity(username, remoteIp);
    }

    public String getToken() {
        return token;
    }

    public String getUsername() {
        return username;
    }

    public long getIssuedAt() {
        return issuedAt;
    }

    public long getExpiresAt() {
        return expiresAt;
    }

    public String getRemoteIp() {
        return remoteIp;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public void setCreatedSession(boolean createdSession) {
        this.createdSession = createdSession;
    }

    public boolean getCreatedSession() {
        return createdSession;
    }
}
