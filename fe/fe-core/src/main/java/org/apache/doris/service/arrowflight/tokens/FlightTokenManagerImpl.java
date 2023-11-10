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

import org.apache.doris.service.arrowflight.auth2.FlightAuthResult;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

/**
 * Token manager implementation.
 */
public class FlightTokenManagerImpl implements FlightTokenManager {
    private static final Logger LOG = LogManager.getLogger(FlightTokenManagerImpl.class);

    private final SecureRandom generator = new SecureRandom();
    private final int cacheExpiration;

    private LoadingCache<String, FlightTokenDetails> tokenCache;

    public FlightTokenManagerImpl(final int cacheSize, final int cacheExpiration) {
        this.cacheExpiration = cacheExpiration;

        this.tokenCache = CacheBuilder.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterWrite(cacheExpiration, TimeUnit.MINUTES)
                .build(new CacheLoader<String, FlightTokenDetails>() {
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
        LOG.trace("Created flight token for user: {}", username);
        return flightTokenDetails;
    }

    @Override
    public FlightTokenDetails validateToken(final String token) throws IllegalArgumentException {
        final FlightTokenDetails value = getTokenDetails(token);
        if (System.currentTimeMillis() >= value.getExpiresAt()) {
            tokenCache.invalidate(token); // removes from the store as well
            throw new IllegalArgumentException("token expired");
        }

        LOG.trace("Validated flight token for user: {}", value.getUsername());
        return value;
    }

    @Override
    public void invalidateToken(final String token) {
        LOG.trace("Invalidate flight token, {}", token);
        tokenCache.invalidate(token); // removes from the store as well
    }

    private FlightTokenDetails getTokenDetails(final String token) {
        Preconditions.checkNotNull(token, "invalid token");
        final FlightTokenDetails value;
        try {
            value = tokenCache.getUnchecked(token);
        } catch (CacheLoader.InvalidCacheLoadException ignored) {
            throw new IllegalArgumentException("invalid token");
        }

        return value;
    }

    @Override
    public void close() throws Exception {
        tokenCache.invalidateAll();
    }
}
