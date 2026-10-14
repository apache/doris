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

package org.apache.doris.filesystem.spi;

import java.util.Locale;

/**
 * Host extraction for endpoint / object-storage URI marker matching.
 *
 * <p>Cloud dialect detection keys on host markers such as {@code oss-dls.aliyuncs.com},
 * {@code myqcloud.com} or {@code amazonaws.com}. Those markers are host names, so they may only be
 * matched against the host part: a URI whose object key happens to contain the same text (for
 * example {@code s3://bucket/archive/AMAZONAWS.COM/file}) does not point at that cloud's storage.
 *
 * <p>Deliberately string based rather than {@code java.net.URI} based: a value such as
 * {@code oss://bucket/my dir/...} does not parse as a URI, and a failed parse must not silently
 * turn a real endpoint into "no match".
 */
public final class EndpointHosts {

    private EndpointHosts() {
    }

    /**
     * Returns the host part of {@code endpointOrUri}: everything after {@code scheme://}, up to the
     * first {@code /} that starts the path, with any {@code userinfo@} removed. A value without a
     * scheme is a bare host, so it is returned as written (minus any path/port unaffected parts).
     */
    public static String hostOf(String endpointOrUri) {
        if (endpointOrUri == null) {
            return "";
        }
        String value = endpointOrUri.trim();
        int authorityStart = authorityStart(value);
        // The authority ends at the first path, query or fragment delimiter: an '@' inside a fragment
        // (e.g. host#contact@other) must not be taken for userinfo and discard the real host.
        String host = value.substring(authorityStart, authorityEnd(value, authorityStart));
        int userInfoIndex = host.lastIndexOf('@');
        if (userInfoIndex >= 0) {
            host = host.substring(userInfoIndex + 1);
        }
        return host;
    }

    /**
     * Returns {@code endpointOrUri} with the scheme and the authority (userinfo, host and port) folded
     * to lower case, leaving the path, the query and the fragment untouched.
     *
     * <p>Only the host is case insensitive. A custom endpoint may carry a case sensitive base path
     * (e.g. {@code https://gateway.internal/Proxy}); folding the whole value would retarget it to a
     * different namespace. Callers that must hand a canonical host to a case sensitive SDK therefore
     * fold the host only.
     */
    public static String lowercaseSchemeAndHost(String endpointOrUri) {
        if (endpointOrUri == null) {
            return "";
        }
        String value = endpointOrUri.trim();
        int authorityStart = authorityStart(value);
        int authorityEnd = authorityEnd(value, authorityStart);
        return value.substring(0, authorityStart).toLowerCase(Locale.ROOT)
                + value.substring(authorityStart, authorityEnd).toLowerCase(Locale.ROOT)
                + value.substring(authorityEnd);
    }

    /** Index just after {@code scheme://}, or 0 when the value carries no scheme. */
    private static int authorityStart(String value) {
        int schemeIndex = value.indexOf("://");
        return schemeIndex >= 0 ? schemeIndex + 3 : 0;
    }

    /** Index at which the authority ends: the first path, query or fragment delimiter, if any. */
    private static int authorityEnd(String value, int from) {
        for (int i = from; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '/' || c == '?' || c == '#') {
                return i;
            }
        }
        return value.length();
    }
}
