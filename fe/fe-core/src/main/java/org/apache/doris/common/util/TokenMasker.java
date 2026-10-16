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

package org.apache.doris.common.util;

import com.google.common.base.Strings;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;

/**
 * Helpers to render a secret (bearer token, auth token, ...) in log lines and error messages
 * without writing the secret itself, since logs are routinely shipped to places that are far
 * less protected than the credential store.
 *
 * <p>Two different renderings are offered, pick by what the reader of the message needs:
 * {@link #tokenId} when the reader only needs to correlate messages about the same secret, and
 * {@link #maskPrefix} when a human needs to recognize <i>which</i> configured secret was used.
 */
public class TokenMasker {
    public static final String EMPTY_TOKEN = "<empty>";

    // A truncated digest: long enough that two live tokens are very unlikely to collide,
    // short enough that it is useless as a credential.
    private static final int TOKEN_ID_LEN = 8;
    private static final String TOKEN_ID_PREFIX = "sha256:";

    // Minimum token length required before we reveal a masked prefix. Shorter tokens would
    // leak too large a fraction of the secret, so they are hidden entirely with only a length hint.
    private static final int MIN_TOKEN_LEN_FOR_PREFIX = 8;
    private static final int TOKEN_PREFIX_LEN = 3;

    private TokenMasker() {
    }

    /**
     * Returns a stable, non-reversible handle for a token, e.g. {@code sha256:1a2b3c4d}. The same
     * token always renders to the same handle, so log lines and the error message handed back to
     * the client can be matched up, while no part of the secret is disclosed.
     */
    public static String tokenId(String token) {
        if (Strings.isNullOrEmpty(token)) {
            return EMPTY_TOKEN;
        }
        return TOKEN_ID_PREFIX + Hashing.sha256().hashString(token, StandardCharsets.UTF_8).toString()
                .substring(0, TOKEN_ID_LEN);
    }

    /**
     * Masks a token by revealing only a short leading prefix (e.g. {@code abc***}), so that a
     * token mismatch is diagnosable during rotation, while never logging the full secret. Empty
     * tokens and tokens too short to safely show a prefix are hidden. Prefer {@link #tokenId}
     * unless the reader really needs to recognize the secret by sight.
     */
    public static String maskPrefix(String token) {
        if (Strings.isNullOrEmpty(token)) {
            return EMPTY_TOKEN;
        }
        if (token.length() < MIN_TOKEN_LEN_FOR_PREFIX) {
            // Too short to reveal any prefix without leaking a large fraction of the secret.
            return "<hidden, token length " + token.length() + " < " + MIN_TOKEN_LEN_FOR_PREFIX + ">";
        }
        return token.substring(0, TOKEN_PREFIX_LEN) + "***";
    }
}
