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

package org.apache.doris.connector.delta;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;

import java.util.Map;

/** Rejects vended credentials that cannot cover the planned operation lifetime. */
final class DeltaVendedCredentialLifetime {
    private static final String QUERY_TIMEOUT = "query_timeout";
    private static final String INSERT_TIMEOUT = "insert_timeout";
    private static final String LOCATION_EXPIRATION =
            "location." + DeltaStorageProperties.S3_TOKEN_EXPIRATION_TIME_MS;

    private DeltaVendedCredentialLifetime() {
    }

    static void validateScan(ConnectorSession session, Map<String, String> properties,
            long safetyMarginMs) {
        validate(session, properties, LOCATION_EXPIRATION, QUERY_TIMEOUT,
                safetyMarginMs, System.currentTimeMillis());
    }

    static void validateWrite(ConnectorSession session, Map<String, String> properties,
            long safetyMarginMs) {
        validate(session, properties, DeltaStorageProperties.S3_TOKEN_EXPIRATION_TIME_MS,
                INSERT_TIMEOUT, safetyMarginMs, System.currentTimeMillis());
    }

    static void validate(ConnectorSession session, Map<String, String> properties,
            String expirationProperty, String timeoutProperty, long safetyMarginMs, long nowMs) {
        String expiration = properties.get(expirationProperty);
        if (expiration == null || session == null) {
            return;
        }
        String timeout = session.getSessionProperties().get(timeoutProperty);
        if (timeout == null) {
            return;
        }
        try {
            long timeoutMs = Math.multiplyExact(Long.parseLong(timeout), 1000L);
            long requiredUntil = Math.addExact(nowMs, Math.addExact(timeoutMs, safetyMarginMs));
            if (Long.parseLong(expiration) <= requiredUntil) {
                throw new DorisConnectorException(
                        "Unity Catalog storage credential expires before " + timeoutProperty
                                + " plus " + safetyMarginMs + " ms safety margin; automatic BE "
                                + "credential renewal is not yet available");
            }
        } catch (ArithmeticException | NumberFormatException e) {
            throw new DorisConnectorException(
                    "Invalid Unity credential expiry or " + timeoutProperty, e);
        }
    }
}
