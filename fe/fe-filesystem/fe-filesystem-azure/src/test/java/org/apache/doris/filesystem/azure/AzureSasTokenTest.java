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

package org.apache.doris.filesystem.azure;

import org.apache.doris.foundation.property.StoragePropertiesException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

class AzureSasTokenTest {

    private static final Clock NOW = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);

    @Test
    void of_stripsTransportPrefixesWithoutChangingSignedContents() {
        AzureSasToken token = AzureSasToken.of(" ?&sv=2024-01-01&sig=a+b%2Fc ", null);

        Assertions.assertEquals("sv=2024-01-01&sig=a+b%2Fc", token.value());
        Assertions.assertTrue(token.expiresAt().isEmpty());
    }

    @Test
    void of_readsEncodedTokenExpiry() {
        AzureSasToken token = AzureSasToken.of("sv=2024-01-01&se=2026-01-02T03%3A04%3A05Z&sig=x", null);

        Assertions.assertEquals(Instant.parse("2026-01-02T03:04:05Z"), token.expiresAt().orElseThrow());
        Assertions.assertFalse(token.isExpired(NOW));
    }

    @Test
    void of_usesEarlierOfExplicitAndTokenExpiry() {
        AzureSasToken token = AzureSasToken.of(
                "sv=2024-01-01&se=2026-01-03T00:00:00Z&sig=x",
                Instant.parse("2026-01-02T00:00:00Z").toEpochMilli());

        Assertions.assertEquals(Instant.parse("2026-01-02T00:00:00Z"), token.expiresAt().orElseThrow());
    }

    @Test
    void isExpired_handlesBoundary() {
        AzureSasToken token = AzureSasToken.of("sv=2024-01-01&se=2026-01-01T00:00:00Z&sig=x", null);

        Assertions.assertTrue(token.isExpired(NOW));
    }

    @Test
    void of_rejectsMalformedExpiryAndLineBreak() {
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> AzureSasToken.of("sv=2024-01-01&se=not-a-date&sig=x", null));
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> AzureSasToken.of("sv=2024-01-01\n&sig=x", null));
    }

    @Test
    void of_rejectsNonPositiveExplicitExpiry() {
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> AzureSasToken.of("sv=2024-01-01&sig=x", 0L));
    }
}
