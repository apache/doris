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
        String host = endpointOrUri.trim();
        int schemeIndex = host.indexOf("://");
        if (schemeIndex >= 0) {
            host = host.substring(schemeIndex + 3);
        }
        // End the authority at the first path, query or fragment delimiter before looking for
        // userinfo: an '@' inside a fragment (e.g. host#contact@other) must not be taken for userinfo,
        // or the real host would be discarded.
        int authorityEnd = host.length();
        int pathIndex = host.indexOf('/');
        if (pathIndex >= 0) {
            authorityEnd = Math.min(authorityEnd, pathIndex);
        }
        int queryIndex = host.indexOf('?');
        if (queryIndex >= 0) {
            authorityEnd = Math.min(authorityEnd, queryIndex);
        }
        int fragmentIndex = host.indexOf('#');
        if (fragmentIndex >= 0) {
            authorityEnd = Math.min(authorityEnd, fragmentIndex);
        }
        host = host.substring(0, authorityEnd);
        int userInfoIndex = host.lastIndexOf('@');
        if (userInfoIndex >= 0) {
            host = host.substring(userInfoIndex + 1);
        }
        return host;
    }
}
