package org.apache.doris.http;

import org.apache.doris.analysis.UserIdentity;

import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

// We simulate a simplified session here: only store user-name of clients who already logged in,
// and we only have a default admin user for now.
public final class HttpAuthManager {
    private static long SESSION_EXPIRE_TIME = 2; // hour
    private static long SESSION_MAX_SIZE = 100; // avoid to store too many

    private static HttpAuthManager instance = new HttpAuthManager();

    public static class SessionValue {
        public UserIdentity currentUser;
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

    public SessionValue getSessionValue(String sessionId) {
        if(!Strings.isNullOrEmpty(sessionId)) {
            return authSessions.getIfPresent(sessionId);
        }
        return null;
    }

    public void addSessionValue(String key, SessionValue value) {
        authSessions.put(key, value);
    }

    public Cache<String, SessionValue> getAuthSessions() {
        return authSessions;
    }
}

