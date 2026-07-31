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

import org.apache.doris.thrift.TNetworkAddress;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class RestBaseControllerTest {

    @Test
    public void testBuildRedirectUrlPreservesEncodedPath() {
        // Keep the original encoded path unchanged when rebuilding the redirect URL.
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getScheme()).thenReturn("http");
        Mockito.when(request.getHeader("Authorization")).thenReturn(null);

        TestRestController controller = new TestRestController();
        String redirectUrl = controller.buildRedirectUrlForTest(request,
                new TNetworkAddress("be-host", 8040), "/api/db%2Ftbl/_stream_load", "k=a%2Bb");

        Assert.assertEquals("http://be-host:8040/api/db%2Ftbl/_stream_load?k=a%2Bb", redirectUrl);
    }

    @Test
    public void testBuildRedirectUrlWithoutQueryString() {
        // Avoid appending a dangling question mark when the original request has no query string.
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getScheme()).thenReturn("http");
        Mockito.when(request.getHeader("Authorization")).thenReturn(null);

        TestRestController controller = new TestRestController();
        String redirectUrl = controller.buildRedirectUrlForTest(request,
                new TNetworkAddress("be-host", 8040), "/api/db%2Ftbl/_stream_load", null);

        Assert.assertEquals("http://be-host:8040/api/db%2Ftbl/_stream_load", redirectUrl);
    }

    @Test
    public void testBuildRedirectUrlToBackendForcesHttpEvenWhenRequestIsHttps() {
        // BE never terminates TLS, so the redirect must stay "http" regardless of request scheme.
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getScheme()).thenReturn("https");
        Mockito.when(request.getHeader("Authorization")).thenReturn(null);

        TestRestController controller = new TestRestController();
        String redirectUrl = controller.buildRedirectUrlToBackendForTest(request,
                new TNetworkAddress("be-host", 8040), "/api/db/tbl/_stream_load", "k=v");

        Assert.assertEquals("http://be-host:8040/api/db/tbl/_stream_load?k=v", redirectUrl);
    }

    // Expose the protected helper so the redirect URL can be verified directly.
    private static class TestRestController extends RestBaseController {
        private String buildRedirectUrlForTest(HttpServletRequest request, TNetworkAddress addr,
                String requestPath, String queryString) {
            return buildRedirectUrl(request, addr, requestPath, queryString);
        }

        private String buildRedirectUrlToBackendForTest(HttpServletRequest request, TNetworkAddress addr,
                String requestPath, String queryString) {
            return buildRedirectUrlToBackend(request, addr, requestPath, queryString);
        }
    }
}
