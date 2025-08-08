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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

public class PathUtils {

    /**
     * Compares two paths ignoring the scheme (protocol).
     * <p>
     * Examples:
     * <ul>
     *   <li>s3://bucket/path/file.txt == cos://bucket/path/file.txt</li>
     *   <li>hdfs://namenode/path == file:///path</li>
     * </ul>
     *
     * @param path1 first path string
     * @param path2 second path string
     * @return true if host and path are equal ignoring the scheme
     */
    public static boolean equalsIgnoreScheme(String path1, String path2) {
        if (path1 == null || path2 == null) {
            return false;
        }
        try {
            URI uri1 = new URI(path1);
            URI uri2 = new URI(path2);

            String host1 = normalizeNull(uri1.getHost());
            String host2 = normalizeNull(uri2.getHost());

            String normPath1 = normalizePath(uri1.getPath());
            String normPath2 = normalizePath(uri2.getPath());

            return Objects.equals(host1, host2)
                    && Objects.equals(normPath1, normPath2);

        } catch (URISyntaxException e) {
            // Fallback: compare as raw strings without scheme
            return stripScheme(path1).equals(stripScheme(path2));
        }
    }

    private static String normalizeNull(String s) {
        return (s == null || s.isEmpty()) ? null : s.toLowerCase();
    }

    private static String normalizePath(String path) {
        if (path == null || path.isEmpty()) {
            return "/";
        }
        // Remove trailing slashes except root "/"
        if (path.length() > 1 && path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        return path;
    }

    private static String stripScheme(String s) {
        int idx = s.indexOf("://");
        if (idx >= 0) {
            return s.substring(idx + 3);
        }
        return s;
    }
}
