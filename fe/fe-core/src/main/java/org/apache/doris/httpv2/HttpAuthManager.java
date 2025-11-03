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

package org.apache.doris.httpv2;

import org.apache.doris.analysis.UserIdentity;

import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;

// We simulate a simplified session here: only store user-name of clients who already logged in,
// and we only have a default admin user for now.
public final class HttpAuthManager {
    private static final Logger LOG = LogManager.getLogger(HttpAuthManager.class);

    private static long SESSION_EXPIRE_TIME = 2; // hour
    private static long SESSION_MAX_SIZE = 100; // avoid to store too many

    private static HttpAuthManager instance = new HttpAuthManager();

    public static class SessionValue {
        public UserIdentity currentUser;
        public String password;
    }

    // session_id => session value
    private Cache<String, SessionValue> authSessions = CacheBuilder.newBuilder()
            .maximumSize(SESSION_MAX_SIZE)
            .expireAfterAccess(SESSION_EXPIRE_TIME, TimeUnit.HOURS)
            .build();

    private HttpAuthManager() {
        // do nothing
    }

    public static HttpAuthManager getInstance() {
        return instance;
    }

    public SessionValue getSessionValue(List<String> sessionIds) {
        for (String sessionId : sessionIds) {
            SessionValue sv = authSessions.getIfPresent(sessionId);
            if (sv != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("get session value {} by session id: {}, left size: {}",
                            sv == null ? null : sv.currentUser, sessionId, authSessions.size());
                }
                return sv;
            }
        }
        return null;
    }

    public void removeSession(String sessionId) {
        if (!Strings.isNullOrEmpty(sessionId)) {
            authSessions.invalidate(sessionId);
            if (LOG.isDebugEnabled()) {
                LOG.debug("remove session id: {}, left size: {}", sessionId, authSessions.size());
            }
        }
    }

    public void addSessionValue(String key, SessionValue value) {
        authSessions.put(key, value);
    }

    public Cache<String, SessionValue> getAuthSessions() {
        return authSessions;
    }
}
