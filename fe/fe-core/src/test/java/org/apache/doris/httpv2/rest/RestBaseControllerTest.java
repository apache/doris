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

import org.apache.doris.httpv2.exception.BadRequestException;
import org.apache.doris.thrift.TNetworkAddress;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

public class RestBaseControllerTest {

    private static final List<Integer> ROWS = Arrays.asList(0, 1, 2);

    @Test
    public void testBuildRedirectUrlPreservesEncodedPath() {
        // Keep the original encoded path unchanged when rebuilding the redirect URL.
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getScheme()).thenReturn("http");
        Mockito.when(request.getHeader("Authorization")).thenReturn(null);

        TestRestController controller = new TestRestController();
        String redirectUrl = controller.buildRedirectUrlForTest(request,
                new TNetworkAddress("be-host", 8040), "/api/db%2Ftbl/_stream_load", "k=a%2Bb");

        Assertions.assertEquals("http://be-host:8040/api/db%2Ftbl/_stream_load?k=a%2Bb", redirectUrl);
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

        Assertions.assertEquals("http://be-host:8040/api/db%2Ftbl/_stream_load", redirectUrl);
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

        Assertions.assertEquals("http://be-host:8040/api/db/tbl/_stream_load?k=v", redirectUrl);
    }

    @Test
    public void testPaginateDefaultAndNormalRanges() {
        assertPage(null, null, Arrays.asList(0, 1, 2));
        assertPage("1", null, Arrays.asList(0));
        assertPage("1", "1", Arrays.asList(1));
        assertPage("0", "1", Arrays.asList());
        assertPage("10", "1", Arrays.asList(1, 2));
    }

    @Test
    public void testPaginateLargeValues() {
        assertPage("1", Long.toString(Long.MAX_VALUE), Arrays.asList());
        assertPage(Long.toString(Long.MAX_VALUE), "1", Arrays.asList(1, 2));
    }

    @Test
    public void testPaginateOffsetRequiresLimit() {
        BadRequestException exception = Assertions.assertThrows(BadRequestException.class,
                () -> paginate(null, "1"));
        Assertions.assertEquals("Param offset should be set with param limit", exception.getMessage());
    }

    @Test
    public void testPaginateInvalidParameters() {
        assertInvalid("-1", null, "Param limit should be a non-negative integer");
        assertInvalid("not-a-number", null, "Param limit should be a non-negative integer");
        assertInvalid("9223372036854775808", null, "Param limit should be a non-negative integer");
        assertInvalid("1", "-1", "Param offset should be a non-negative integer");
        assertInvalid("1", "not-a-number", "Param offset should be a non-negative integer");
        assertInvalid("1", "9223372036854775808", "Param offset should be a non-negative integer");
    }

    private void assertPage(String limit, String offset, List<Integer> expected) {
        Assertions.assertEquals(expected, paginate(limit, offset));
    }

    private void assertInvalid(String limit, String offset, String expectedMessage) {
        BadRequestException exception = Assertions.assertThrows(BadRequestException.class,
                () -> paginate(limit, offset));
        Assertions.assertEquals(expectedMessage, exception.getMessage());
    }

    private List<Integer> paginate(String limit, String offset) {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getParameter("limit")).thenReturn(limit);
        Mockito.when(request.getParameter("offset")).thenReturn(offset);
        return new TestRestController().paginateForTest(request, ROWS);
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

        private <T> List<T> paginateForTest(HttpServletRequest request, List<T> rows) {
            return paginate(request, rows);
        }
    }
}
