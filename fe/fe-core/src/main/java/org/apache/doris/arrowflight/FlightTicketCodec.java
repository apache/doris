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

package org.apache.doris.arrowflight;

import org.apache.doris.common.Config;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** Adds authentication to the existing {@code queryId&host&port&base64(sql)} ticket. */
public final class FlightTicketCodec {
    private static final long MAX_LIFETIME_MS = 3_600_000;
    private static final byte[] KEY_DOMAIN = "doris/flight-ticket/key/v1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PAYLOAD_DOMAIN =
            "doris/flight-ticket/payload/v1\0".getBytes(StandardCharsets.UTF_8);

    private FlightTicketCodec() {
    }

    public static String sign(String token, int clusterId, String ticket) throws GeneralSecurityException {
        return sign(token, clusterId, ticket, System.currentTimeMillis(),
                Config.arrow_flight_ticket_lifetime_seconds * 1000L);
    }

    static String sign(String token, int clusterId, String ticket, long nowMs, long lifetimeMs)
            throws GeneralSecurityException {
        if (token == null || token.isEmpty()) {
            throw new IllegalArgumentException("Flight signing token is not ready");
        }
        if (lifetimeMs <= 0 || lifetimeMs > MAX_LIFETIME_MS || nowMs < 0
                || nowMs > Long.MAX_VALUE - lifetimeMs) {
            throw new IllegalArgumentException("Invalid Flight ticket lifetime");
        }
        // Preserve all four original fields. Authenticate them and the metadata as exact UTF-8 bytes.
        String payload = ticket + "&v1&" + clusterId + "&" + nowMs + "&" + (nowMs + lifetimeMs);
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(token.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        byte[] key = mac.doFinal(KEY_DOMAIN);
        mac.init(new SecretKeySpec(key, "HmacSHA256"));
        mac.update(PAYLOAD_DOMAIN);
        return payload + "&" + Base64.getEncoder().encodeToString(
                mac.doFinal(payload.getBytes(StandardCharsets.UTF_8)));
    }
}
