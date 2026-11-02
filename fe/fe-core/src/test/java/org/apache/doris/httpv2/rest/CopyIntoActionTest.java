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

package org.apache.doris.httpv2.rest;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class CopyIntoActionTest {
    @Test
    public void testHeadersForUploadAndQueryLogsMaskCredentials() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        String[] sensitiveHeaders = {"aUtHoRiZaTiOn", "PROXY-AUTHORIZATION", "cookie", "Set-Cookie",
                "TOKEN", "aUtH-tOkEn", "AUTH_CODE"};
        for (String value : new String[] {"Basic dXNlcjpzZWNyZXQ=", "Bearer synthetic-token", "Basic malformed"}) {
            Mockito.when(request.getHeaderNames()).thenReturn(Collections.enumeration(Arrays.asList(
                    "aUtHoRiZaTiOn", "PROXY-AUTHORIZATION", "cookie", "Set-Cookie", "TOKEN", "aUtH-tOkEn",
                    "AUTH_CODE", "fileName", "label")));
            Mockito.when(request.getHeader(Mockito.anyString())).thenReturn(value);
            Mockito.when(request.getHeader("fileName")).thenReturn("data.csv");
            Mockito.when(request.getHeader("label")).thenReturn("copy_label");

            Map<String, String> headers = getHeadersInfo(request);

            for (String name : sensitiveHeaders) {
                Assertions.assertEquals("***MASKED***", headers.get(name));
            }
            Assertions.assertEquals("data.csv", headers.get("fileName"));
            Assertions.assertEquals("copy_label", headers.get("label"));
            Assertions.assertFalse(headers.toString().contains(value));
            Assertions.assertEquals(value, request.getHeader("aUtHoRiZaTiOn"));
        }
    }

    @Test
    public void testHeadersUnavailable() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Assertions.assertTrue(getHeadersInfo(request).isEmpty());
        Mockito.when(request.getHeaderNames()).thenThrow(new IllegalStateException("headers unavailable"));
        Assertions.assertTrue(getHeadersInfo(request).isEmpty());
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> getHeadersInfo(HttpServletRequest request) throws Exception {
        Method method = CopyIntoAction.class.getDeclaredMethod("getHeadersInfo", HttpServletRequest.class);
        method.setAccessible(true);
        return (Map<String, String>) method.invoke(null, request);
    }
}
