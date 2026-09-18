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

package org.apache.doris.datasource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

class DelegatedCredentialTest {

    @Test
    void testUsesExplicitExpirationMillis() {
        DelegatedCredential credential = new DelegatedCredential(DelegatedCredential.Type.ID_TOKEN,
                "opaque-token", OptionalLong.of(1_700_000_000_000L));

        Assertions.assertTrue(credential.getExpiresAtMillis().isPresent());
        Assertions.assertEquals(1_700_000_000_000L, credential.getExpiresAtMillis().getAsLong());
        Assertions.assertTrue(credential.isExpired(1_700_000_000_000L));
    }

    @Test
    void testJwtTokenHasNoExpirationWhenPluginDoesNotReturnOne() {
        DelegatedCredential credential = new DelegatedCredential(DelegatedCredential.Type.ID_TOKEN,
                "eyJhbGciOiJub25lIn0.eyJleHAiOjE3MDAwMDAwMDB9.");

        Assertions.assertFalse(credential.getExpiresAtMillis().isPresent());
        Assertions.assertFalse(credential.isExpired(Long.MAX_VALUE));
    }

    @Test
    void testOpaqueTokenHasNoExpirationWhenPluginDoesNotReturnOne() {
        DelegatedCredential credential = new DelegatedCredential(DelegatedCredential.Type.ACCESS_TOKEN,
                "opaque-token");

        Assertions.assertFalse(credential.getExpiresAtMillis().isPresent());
        Assertions.assertFalse(credential.isExpired(Long.MAX_VALUE));
    }

}
