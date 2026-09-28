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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TokenMaskerTest {

    // Same length and alphabet as the tokens FlightTokenManagerImpl mints (130 random bits in
    // base 32), but deliberately low entropy and readable, so that no secret scanner has to
    // decide whether a random-looking 26 character literal in the tree is a real credential.
    private static final String TOKEN = "notarealtokennotarealtoken";

    @Test
    public void testTokenIdHidesTheSecret() {
        String id = TokenMasker.tokenId(TOKEN);
        Assertions.assertFalse(id.contains(TOKEN));
        // The id is a truncated digest and nothing else, so no part of the token can survive in it.
        Assertions.assertTrue(id.matches("sha256:[0-9a-f]{8}"), "unexpected token id: " + id);
    }

    @Test
    public void testTokenIdIsStableAndDistinguishing() {
        // Same token always renders identically, so a log line and the error message returned to
        // the client can be matched up.
        Assertions.assertEquals(TokenMasker.tokenId(TOKEN), TokenMasker.tokenId(TOKEN));
        Assertions.assertNotEquals(TokenMasker.tokenId(TOKEN), TokenMasker.tokenId(TOKEN + "x"));
        Assertions.assertEquals("sha256:", TokenMasker.tokenId(TOKEN).substring(0, 7));
        Assertions.assertEquals(15, TokenMasker.tokenId(TOKEN).length());
    }

    @Test
    public void testTokenIdOfEmptyToken() {
        Assertions.assertEquals(TokenMasker.EMPTY_TOKEN, TokenMasker.tokenId(null));
        Assertions.assertEquals(TokenMasker.EMPTY_TOKEN, TokenMasker.tokenId(""));
    }

    @Test
    public void testMaskPrefix() {
        Assertions.assertEquals("not***", TokenMasker.maskPrefix(TOKEN));
        Assertions.assertEquals(TokenMasker.EMPTY_TOKEN, TokenMasker.maskPrefix(null));
        Assertions.assertEquals(TokenMasker.EMPTY_TOKEN, TokenMasker.maskPrefix(""));
        // Too short to show a prefix: hidden entirely, only the length is reported.
        Assertions.assertEquals("<hidden, token length 7 < 8>", TokenMasker.maskPrefix("1234567"));
        Assertions.assertEquals("123***", TokenMasker.maskPrefix("12345678"));
    }
}
