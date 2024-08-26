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
// https://github.com/dremio/dremio-oss/blob/master/services/arrow-flight/src/main/java/com/dremio/service/tokens/TokenManagerImpl.java
// and modified by Doris

package org.apache.doris.service.arrowflight.tokens;

import org.apache.doris.catalog.Env;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.arrowflight.auth2.FlightAuthResult;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Token manager implementation.
 */
public class FlightTokenManagerImpl implements FlightTokenManager {
    private static final Logger LOG = LogManager.getLogger(FlightTokenManagerImpl.class);

    private final SecureRandom generator = new SecureRandom();
    private final int cacheSize;
    private final int cacheExpiration;

    private final LoadingCache<String, FlightTokenDetails> tokenCache;
    // <username, <token, 1>>
    private final ConcurrentHashMap<String, LoadingCache<String, Integer>> usersTokenLRU = new ConcurrentHashMap<>();

    public FlightTokenManagerImpl(final int cacheSize, final int cacheExpiration) {
        this.cacheSize = cacheSize;
        this.cacheExpiration = cacheExpiration;

        this.tokenCache = CacheBuilder.newBuilder().maximumSize(cacheSize)
                .expireAfterWrite(cacheExpiration, TimeUnit.MINUTES)
                .removalListener(new RemovalListener<String, FlightTokenDetails>() {
                    @Override
                    public void onRemoval(@NotNull RemovalNotification<String, FlightTokenDetails> notification) {
                        // TODO: broadcast this message to other FE
                        String token = notification.getKey();
                        FlightTokenDetails tokenDetails = notification.getValue();
                        LOG.info("evict bearer token: " + token + ", reason: token number exceeded, "
                                + notification.getCause());
                        ConnectContext context = ExecuteEnv.getInstance().getScheduler()
                                .getContext(token);
                        if (context != null) {
                            ExecuteEnv.getInstance().getScheduler().unregisterConnection(context);
                            LOG.info("unregister flight connect context after evict bearer token: " + token);
                        }
                        usersTokenLRU.get(tokenDetails.getUsername()).invalidate(token);
                    }
                }).build(new CacheLoader<String, FlightTokenDetails>() {
                    @Override
                    public FlightTokenDetails load(String key) {
                        return new FlightTokenDetails();
                    }
                });
    }

    // From https://stackoverflow.com/questions/41107/how-to-generate-a-random-alpha-numeric-string
    // ... This works by choosing 130 bits from a cryptographically secure random bit generator, and encoding
    // them in base-32. 128 bits is considered to be cryptographically strong, but each digit in a base 32
    // number can encode 5 bits, so 128 is rounded up to the next multiple of 5 ... Why 32? Because 32 = 2^5;
    // each character will represent exactly 5 bits, and 130 bits can be evenly divided into characters.
    @Override
    public String newToken() {
        return new BigInteger(130, generator).toString(32);
    }

    @Override
    public FlightTokenDetails createToken(final String username, final FlightAuthResult flightAuthResult) {
        final String token = newToken();
        final long now = System.currentTimeMillis();
        final long expires = now + TimeUnit.MILLISECONDS.convert(cacheExpiration, TimeUnit.MINUTES);

        final FlightTokenDetails flightTokenDetails = new FlightTokenDetails(token, username, now, expires,
                flightAuthResult.getUserIdentity(), flightAuthResult.getRemoteIp());

        tokenCache.put(token, flightTokenDetails);
        if (!usersTokenLRU.containsKey(username)) {
            // TODO Modify usersTokenLRU size when user property maxConn changes. but LoadingCache currently not
            // support modify.
            usersTokenLRU.put(username,
                    CacheBuilder.newBuilder().maximumSize(Env.getCurrentEnv().getAuth().getMaxConn(username) / 2)
                            .removalListener(new RemovalListener<String, Integer>() {
                                @Override
                                public void onRemoval(@NotNull RemovalNotification<String, Integer> notification) {
                                    // TODO: broadcast this message to other FE
                                    assert notification.getKey() != null;
                                    tokenCache.invalidate(notification.getKey());
                                    LOG.info("evict bearer token: " + notification.getKey()
                                            + ", reason: user connection exceeded, " + notification.getCause());
                                }
                            }).build(new CacheLoader<String, Integer>() {
                                @NotNull
                                @Override
                                public Integer load(@NotNull String key) {
                                    return 1;
                                }
                            }));
        }
        usersTokenLRU.get(username).put(token, 1);
        LOG.info("Created flight token for user: {}, token: {}", username, token);
        return flightTokenDetails;
    }

    @Override
    public FlightTokenDetails validateToken(final String token) throws IllegalArgumentException {
        final FlightTokenDetails value = getTokenDetails(token);
        if (value.getToken().equals("")) {
            throw new IllegalArgumentException("invalid bearer token: " + token
                    + ", try reconnect, bearer token may not be created, or may have been evict, search for this "
                    + "token in fe.log to see the evict reason. currently in fe.conf, `arrow_flight_token_cache_size`="
                    + this.cacheSize + ", `arrow_flight_token_alive_time`=" + this.cacheExpiration);
        }
        if (System.currentTimeMillis() >= value.getExpiresAt()) {
            tokenCache.invalidate(token);
            throw new IllegalArgumentException("bearer token expired: " + token + ", try reconnect, "
                    + "currently in fe.conf, `arrow_flight_token_alive_time`=" + this.cacheExpiration);
        }
        if (usersTokenLRU.containsKey(value.getUsername())) {
            try {
                usersTokenLRU.get(value.getUsername()).get(token);
            } catch (ExecutionException ignored) {
                throw new IllegalArgumentException("usersTokenLRU not exist bearer token: " + token);
            }
        } else {
            throw new IllegalArgumentException(
                    "bearer token not created: " + token + ", username:  " + value.getUsername());
        }
        LOG.info("Validated bearer token for user: {}", value.getUsername());
        return value;
    }

    @Override
    public void invalidateToken(final String token) {
        LOG.info("Invalidate bearer token, {}", token);
        tokenCache.invalidate(token);
    }

    private FlightTokenDetails getTokenDetails(final String token) {
        Preconditions.checkNotNull(token, "invalid token");
        final FlightTokenDetails value;
        try {
            value = tokenCache.getUnchecked(token);
        } catch (CacheLoader.InvalidCacheLoadException ignored) {
            throw new IllegalArgumentException("InvalidCacheLoadException, invalid bearer token: " + token);
        }

        return value;
    }

    @Override
    public void close() throws Exception {
        tokenCache.invalidateAll();
    }
}
