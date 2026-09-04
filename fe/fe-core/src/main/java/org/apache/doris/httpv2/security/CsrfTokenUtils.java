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

package org.apache.doris.httpv2.security;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * Creates and verifies tokens that protect cookie-authenticated HTTP mutations from cross-site request forgery.
 *
 * <p>For example, after an administrator signs in to Doris, the browser automatically attaches the
 * {@code PALO_SESSION_ID} cookie to Doris requests. A malicious page opened in another tab could try to submit a
 * POST request to Doris with that cookie. The malicious page cannot read the CSRF token returned by Doris, so the
 * request is rejected unless it also supplies the matching {@code X-Doris-CSRF-Token} header.</p>
 */
public final class CsrfTokenUtils {
    public static final String HEADER_NAME = "X-Doris-CSRF-Token";
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final int CSRF_TOKEN_BYTES = 32;

    private CsrfTokenUtils() {
    }

    /**
     * Generates a token bound to an authenticated HTTP session. Browsers automatically attach the Doris
     * session cookie, so state-changing Web UI requests must also present this non-cookie value to prove
     * that they originated from the Doris page rather than from another web site.
     */
    public static String newCsrfToken() {
        byte[] value = new byte[CSRF_TOKEN_BYTES];
        RANDOM.nextBytes(value);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value);
    }

    public static boolean csrfTokenMatches(String expected, String actual) {
        if (expected == null || actual == null) {
            return false;
        }
        return MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8), actual.getBytes(StandardCharsets.UTF_8));
    }
}
