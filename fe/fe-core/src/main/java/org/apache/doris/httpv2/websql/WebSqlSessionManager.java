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

package org.apache.doris.httpv2.websql;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;

import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;

/**
 * Manages bounded FE-local Web SQL sessions from creation through execution, reset, cancellation, and cleanup.
 * Each registered session is owner-scoped and holds exactly one persistent JDBC connection.
 */
@Component
public class WebSqlSessionManager implements DisposableBean {
    private static final Logger LOG = LogManager.getLogger(WebSqlSessionManager.class);
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final Pattern SESSION_ID_PATTERN = Pattern.compile("[A-Za-z0-9_-]{8}\\.[A-Za-z0-9_-]{43}");

    private final Map<String, WebSqlSession> sessions = new ConcurrentHashMap<>();
    private final Map<String, Integer> sessionsPerOwner = new HashMap<>();
    private final Cache<String, Boolean> expiredSessionIds = CacheBuilder.newBuilder()
            .maximumSize(10000)
            .expireAfterWrite(1, TimeUnit.HOURS)
            .build();
    private final Object lifecycleLock = new Object();
    private int pendingSessions;
    private volatile boolean destroyed;
    private final WebSqlConnectionFactory connectionFactory;
    private final WebSqlStatementExecutor statementExecutor;
    private final WebSqlLimits limits;
    private final LongSupplier clock;
    private final boolean useRuntimeConfig;
    private final String frontendHint;
    private final ScheduledExecutorService cleaner;

    public WebSqlSessionManager() {
        this(new JdbcWebSqlConnectionFactory(), new WebSqlStatementExecutor(), WebSqlLimits.fromConfig(),
                System::currentTimeMillis, true, true);
    }

    WebSqlSessionManager(WebSqlConnectionFactory connectionFactory, WebSqlStatementExecutor statementExecutor,
            WebSqlLimits limits, LongSupplier clock, boolean startCleaner) {
        this(connectionFactory, statementExecutor, limits, clock, startCleaner, false);
    }

    private WebSqlSessionManager(WebSqlConnectionFactory connectionFactory,
            WebSqlStatementExecutor statementExecutor, WebSqlLimits limits, LongSupplier clock,
            boolean startCleaner, boolean useRuntimeConfig) {
        this.connectionFactory = connectionFactory;
        this.statementExecutor = statementExecutor;
        this.limits = limits;
        this.clock = clock;
        this.useRuntimeConfig = useRuntimeConfig;
        this.frontendHint = randomToken(6);
        if (startCleaner && (useRuntimeConfig ? Config.enable_web_ui : limits.enabled)) {
            cleaner = ThreadPoolManager.newDaemonScheduledThreadPool(1, "web-sql-session-cleaner", true);
            cleaner.scheduleWithFixedDelay(this::cleanupExpiredSafely, limits.cleanupIntervalSeconds,
                    limits.cleanupIntervalSeconds, TimeUnit.SECONDS);
        } else {
            cleaner = null;
        }
    }

    public WebSqlSession createSession(String owner, String password) {
        return createSession(owner, password, null, null);
    }

    public WebSqlSession createSession(UserIdentity userIdentity, String password, String httpSessionId) {
        return createSession(userIdentity.getQualifiedUser(), password, userIdentity, httpSessionId);
    }

    private WebSqlSession createSession(String owner, String password, UserIdentity userIdentity,
            String httpSessionId) {
        requireEnabled();
        reserveSession(owner);
        Connection connection;
        try {
            connection = userIdentity == null
                    ? connectionFactory.open(owner, password)
                    : connectionFactory.open(userIdentity, password);
        } catch (SQLException exception) {
            releaseReservation(owner);
            throw connectionException(exception);
        }

        String id = frontendHint + "." + randomToken(32);
        WebSqlSession session = new WebSqlSession(id, owner, httpSessionId, connection, clock.getAsLong());
        boolean accepted;
        synchronized (lifecycleLock) {
            pendingSessions--;
            accepted = !destroyed;
            if (accepted) {
                sessions.put(id, session);
            } else {
                decrementOwnerCount(owner);
            }
        }
        if (!accepted) {
            closeConnection(session);
            throw new WebSqlException(WebSqlError.DISABLED);
        }
        return session;
    }

    /** Returns an existing owner-scoped session after applying the normal enabled, ID, and expiry checks. */
    public WebSqlSession getSession(String id, String owner) {
        requireEnabled();
        return requireOwnedSession(id, owner);
    }

    public WebSqlExecutionResult execute(String id, String owner, String sql) {
        requireEnabled();
        WebSqlSession session = requireOwnedSession(id, owner);
        enter(session);
        try {
            if (session.isClosed()) {
                throw new WebSqlException(WebSqlError.SESSION_NOT_FOUND);
            }
            session.touch(clock.getAsLong());
            return statementExecutor.execute(session, sql, limits);
        } finally {
            session.setActiveStatement(null);
            session.leave(clock.getAsLong());
        }
    }

    public boolean cancel(String id, String owner) {
        requireEnabled();
        WebSqlSession session = requireOwnedSession(id, owner);
        try {
            return session.cancel();
        } catch (SQLException exception) {
            throw new WebSqlException(WebSqlError.QUERY_ERROR, exception);
        }
    }

    public WebSqlSession reset(String id, String owner, String password) {
        return reset(id, owner, password, null);
    }

    public WebSqlSession reset(String id, UserIdentity userIdentity, String password) {
        return reset(id, userIdentity.getQualifiedUser(), password, userIdentity);
    }

    private WebSqlSession reset(String id, String owner, String password, UserIdentity userIdentity) {
        requireEnabled();
        WebSqlSession session = requireOwnedSession(id, owner);
        enter(session);
        try {
            session.touch(clock.getAsLong());
            Connection replacement;
            try {
                replacement = userIdentity == null
                        ? connectionFactory.open(owner, password)
                        : connectionFactory.open(userIdentity, password);
            } catch (SQLException exception) {
                throw connectionException(exception);
            }
            try {
                session.replaceConnection(replacement);
            } catch (SQLException exception) {
                try {
                    replacement.close();
                } catch (SQLException closeException) {
                    exception.addSuppressed(closeException);
                }
                throw new WebSqlException(WebSqlError.CONNECTION_ERROR, exception);
            }
            return session;
        } finally {
            session.leave(clock.getAsLong());
        }
    }

    public boolean closeSession(String id, String owner) {
        requireEnabled();
        requireValidSessionId(id);
        WebSqlSession session = sessions.get(id);
        if (session == null) {
            return false;
        }
        verifyOwner(session, owner);
        removeAndClose(session, false);
        return true;
    }

    /**
     * Closes the Web SQL sessions opened by one browser session.
     *
     * <p>Scoped to the browser session rather than to the Doris account: shared accounts such as
     * root are the norm, and signing out in one browser must not cancel the statements another
     * browser -- or another operator -- is running.
     */
    public int closeSessionsForHttpSession(String httpSessionId) {
        if (Strings.isNullOrEmpty(httpSessionId)) {
            return 0;
        }
        List<WebSqlSession> owned = new ArrayList<>();
        for (WebSqlSession session : sessions.values()) {
            if (httpSessionId.equals(session.getHttpSessionId())) {
                owned.add(session);
            }
        }
        for (WebSqlSession session : owned) {
            removeAndClose(session, false);
        }
        return owned.size();
    }

    public int cleanupExpired() {
        long now = clock.getAsLong();
        long idleTimeoutMillis = currentIdleTimeoutMillis();
        List<WebSqlSession> expired = new ArrayList<>();
        for (WebSqlSession session : sessions.values()) {
            if (now - session.getLastAccessMillis() >= idleTimeoutMillis
                    && session.tryEnterForCleanup()) {
                try {
                    if (now - session.getLastAccessMillis() >= idleTimeoutMillis) {
                        expired.add(session);
                        unregister(session, true);
                        closeConnection(session);
                    }
                } finally {
                    session.leaveWithoutTouch();
                }
            }
        }
        return expired.size();
    }

    public int size() {
        return sessions.size();
    }

    @Override
    public void destroy() {
        if (cleaner != null) {
            cleaner.shutdownNow();
        }
        destroyed = true;
        for (WebSqlSession session : new ArrayList<>(sessions.values())) {
            removeAndClose(session, false);
        }
    }

    private void enter(WebSqlSession session) {
        try {
            if (!session.tryEnter(limits.maxQueuedStatements, limits.lockWaitTimeoutMillis)) {
                throw new WebSqlException(WebSqlError.SESSION_BUSY);
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new WebSqlException(WebSqlError.SESSION_BUSY, exception);
        }
    }

    private WebSqlSession requireOwnedSession(String id, String owner) {
        requireValidSessionId(id);
        WebSqlSession session = sessions.get(id);
        if (session == null) {
            if (expiredSessionIds.getIfPresent(id) != null) {
                throw new WebSqlException(WebSqlError.SESSION_EXPIRED);
            }
            throw new WebSqlException(WebSqlError.SESSION_NOT_FOUND);
        }
        verifyOwner(session, owner);
        long idleTimeoutMillis = currentIdleTimeoutMillis();
        if (clock.getAsLong() - session.getLastAccessMillis() >= idleTimeoutMillis) {
            if (session.tryEnterForCleanup()) {
                try {
                    if (clock.getAsLong() - session.getLastAccessMillis() >= idleTimeoutMillis) {
                        unregister(session, true);
                        closeConnection(session);
                        throw new WebSqlException(WebSqlError.SESSION_EXPIRED);
                    }
                } finally {
                    session.leaveWithoutTouch();
                }
            }
        }
        return session;
    }

    private void verifyOwner(WebSqlSession session, String owner) {
        if (!session.getOwner().equals(owner)) {
            throw new WebSqlException(WebSqlError.ACCESS_DENIED);
        }
    }

    private void requireEnabled() {
        if (destroyed || !(useRuntimeConfig ? Config.enable_web_ui && Config.enable_web_sql_session : limits.enabled)) {
            throw new WebSqlException(WebSqlError.DISABLED);
        }
    }

    private void reserveSession(String owner) {
        synchronized (lifecycleLock) {
            if (destroyed) {
                throw new WebSqlException(WebSqlError.DISABLED);
            }
            int maxSessions = currentMaxSessions();
            int maxSessionsPerUser = Math.min(maxSessions, limits.maxSessionsPerUser);
            if (sessions.size() + pendingSessions >= maxSessions
                    || sessionsPerOwner.getOrDefault(owner, 0) >= maxSessionsPerUser) {
                throw new WebSqlException(WebSqlError.SESSION_LIMIT_EXCEEDED);
            }
            pendingSessions++;
            sessionsPerOwner.merge(owner, 1, Integer::sum);
        }
    }

    private void releaseReservation(String owner) {
        synchronized (lifecycleLock) {
            pendingSessions--;
            decrementOwnerCount(owner);
        }
    }

    private void decrementOwnerCount(String owner) {
        sessionsPerOwner.computeIfPresent(owner, (ignored, count) -> count <= 1 ? null : count - 1);
    }

    private WebSqlException connectionException(SQLException exception) {
        if (exception instanceof WebSqlIdentityMismatchException) {
            return new WebSqlException(WebSqlError.IDENTITY_MISMATCH,
                    Collections.singletonMap("message", exception.getMessage()), exception);
        }
        return new WebSqlException(WebSqlError.CONNECTION_ERROR, exception);
    }

    private long currentIdleTimeoutMillis() {
        return useRuntimeConfig ? Config.web_sql_session_idle_timeout_seconds * 1000L : limits.idleTimeoutMillis;
    }

    private int currentMaxSessions() {
        return useRuntimeConfig ? Config.web_sql_max_sessions : limits.maxSessions;
    }

    private void requireValidSessionId(String id) {
        if (id == null || !SESSION_ID_PATTERN.matcher(id).matches()) {
            throw new WebSqlException(WebSqlError.SESSION_NOT_FOUND);
        }
    }

    private void removeAndClose(WebSqlSession session, boolean expired) {
        if (!unregister(session, expired)) {
            return;
        }
        closeConnection(session);
    }

    private boolean unregister(WebSqlSession session, boolean expired) {
        synchronized (lifecycleLock) {
            if (!sessions.remove(session.getId(), session)) {
                return false;
            }
            decrementOwnerCount(session.getOwner());
            if (expired) {
                expiredSessionIds.put(session.getId(), true);
            }
        }
        return true;
    }

    private void closeConnection(WebSqlSession session) {
        try {
            session.closeConnection();
        } catch (SQLException exception) {
            LOG.warn("Failed to close a Web SQL session connection", exception);
        }
    }

    private void cleanupExpiredSafely() {
        try {
            cleanupExpired();
        } catch (RuntimeException exception) {
            LOG.warn("Failed to clean up expired Web SQL sessions", exception);
        }
    }

    private static String randomToken(int byteCount) {
        byte[] bytes = new byte[byteCount];
        SECURE_RANDOM.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }
}
