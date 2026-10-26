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

package org.apache.doris.httpv2.util;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Locale;

public class HttpHeaderUtilsTest {
    @Test
    public void testSensitiveHeadersAreMaskedRegardlessOfCase() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        for (String name : new String[] {"Authorization", "Proxy-Authorization", "Cookie", "Set-Cookie",
                "token", "Auth-Token", "auth_code", "aUtHoRiZaTiOn", "pRoXy-AuThOrIzAtIoN"}) {
            for (String spelling : new String[] {name, name.toLowerCase(Locale.ROOT), name.toUpperCase(Locale.ROOT)}) {
                Assertions.assertEquals("***MASKED***", HttpHeaderUtils.getRedactedHeaderValueForLogging(request, spelling));
            }
        }
        Mockito.verifyNoInteractions(request);
    }

    @Test
    public void testMaskingDoesNotChangeAuthenticationHeader() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        for (String value : new String[] {"Basic dXNlcjpzZWNyZXQ=", "Bearer synthetic-token", "Basic malformed"}) {
            Mockito.when(request.getHeader("Authorization")).thenReturn(value);
            Assertions.assertEquals("***MASKED***", HttpHeaderUtils.getRedactedHeaderValueForLogging(request, "Authorization"));
            Assertions.assertEquals(value, request.getHeader("Authorization"));
        }
    }

    @Test
    public void testNonSensitiveHeadersKeepTheirValues() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getHeader("label")).thenReturn("load_label");
        Mockito.when(request.getHeader("fileName")).thenReturn("data.csv");
        Assertions.assertEquals("load_label", HttpHeaderUtils.getRedactedHeaderValueForLogging(request, "label"));
        Assertions.assertEquals("data.csv", HttpHeaderUtils.getRedactedHeaderValueForLogging(request, "fileName"));
        Assertions.assertNull(HttpHeaderUtils.getRedactedHeaderValueForLogging(request, "missing"));
    }
}
