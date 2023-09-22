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

import org.apache.doris.service.arrowflight.auth2.DorisAuthResult;

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
public class TokenManagerImpl implements TokenManager {
    private static final Logger LOG = LogManager.getLogger(TokenManagerImpl.class);

    private final SecureRandom generator = new SecureRandom();
    private final int cacheExpiration;

    private LoadingCache<String, SessionState> tokenCache;

    public TokenManagerImpl(final int cacheSize, final int cacheExpiration) {
        this.cacheExpiration = cacheExpiration;

        this.tokenCache = CacheBuilder.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterWrite(cacheExpiration, TimeUnit.MINUTES)
                .build(new CacheLoader<String, SessionState>() {
                    @Override
                    public SessionState load(String key) {
                        return new SessionState();
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
    public TokenDetails createToken(final String username, final DorisAuthResult dorisAuthResult) {
        final String token = newToken();
        final long now = System.currentTimeMillis();
        final long expires = now + TimeUnit.MILLISECONDS.convert(cacheExpiration, TimeUnit.MINUTES);

        final SessionState state = new SessionState(username, now, expires);
        state.setAuthResult(dorisAuthResult);

        tokenCache.put(token, state);
        LOG.trace("Created token for user: {}", username);
        return TokenDetails.of(token, username, expires);
    }

    @Override
    public TokenDetails validateToken(final String token) throws IllegalArgumentException {
        final SessionState value = getSessionState(token);
        if (System.currentTimeMillis() >= value.getExpiresAt()) {
            tokenCache.invalidate(token); // removes from the store as well
            throw new IllegalArgumentException("token expired");
        }

        LOG.trace("Validated token for user: {}", value.getUsername());
        return TokenDetails.of(token, value.getUsername(), value.getExpiresAt());
    }

    @Override
    public void invalidateToken(final String token) {
        LOG.trace("Invalidate token");
        tokenCache.invalidate(token); // removes from the store as well
    }

    private SessionState getSessionState(final String token) {
        Preconditions.checkNotNull(token, "invalid token");
        final SessionState value;
        try {
            value = tokenCache.getUnchecked(token);
        } catch (CacheLoader.InvalidCacheLoadException ignored) {
            throw new IllegalArgumentException("invalid token");
        }

        return value;
    }
}
