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

import com.google.common.base.Preconditions;

/**
 * Details of a token.
 */
public final class TokenDetails {

    public final String token;
    public final String username;
    public final long expiresAt;

    private TokenDetails(String token, String username, long expiresAt) {
        Preconditions.checkNotNull(token);
        Preconditions.checkNotNull(username);
        this.token = token;
        this.username = username;
        this.expiresAt = expiresAt;
    }

    public static TokenDetails of(String token, String username, long expiresAt) {
        return new TokenDetails(token, username, expiresAt);
    }
}
