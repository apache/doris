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
     * Compares two URI strings for equality with special handling for the "s3" scheme.
     * <p>
     * The comparison rules are:
     * - If both URIs have the same scheme, compare the full URI strings (case-insensitive).
     * - If the schemes differ, but one of them is "s3", then compare only the authority and path parts,
     * ignoring the scheme.
     * - Otherwise, consider the URIs as not equal.
     * <p>
     * This is useful in scenarios where "s3" URIs should be treated as equivalent to other schemes
     * if the host and path match, ignoring scheme differences.
     *
     * @param p1 the first URI string to compare
     * @param p2 the second URI string to compare
     * @return true if the URIs are considered equal under the above rules, false otherwise
     */
    public static boolean equalsIgnoreSchemeIfOneIsS3(String p1, String p2) {
        if (p1 == null || p2 == null) {
            return p1 == null && p2 == null;
        }

        try {
            URI uri1 = new URI(p1);
            URI uri2 = new URI(p2);

            String scheme1 = uri1.getScheme();
            String scheme2 = uri2.getScheme();

            // If schemes are equal, compare the full URI strings ignoring case
            if (scheme1 != null && scheme1.equalsIgnoreCase(scheme2)) {
                return p1.equalsIgnoreCase(p2);
            }

            // If schemes differ but one is "s3", compare only authority and path ignoring scheme
            if ("s3".equalsIgnoreCase(scheme1) || "s3".equalsIgnoreCase(scheme2)) {
                String auth1 = normalize(uri1.getAuthority());
                String auth2 = normalize(uri2.getAuthority());
                String path1 = normalize(uri1.getPath());
                String path2 = normalize(uri2.getPath());

                return Objects.equals(auth1, auth2) && Objects.equals(path1, path2);
            }

            // Otherwise, URIs are not equal
            return false;

        } catch (URISyntaxException e) {
            // If URI parsing fails, fallback to simple case-insensitive string comparison
            return p1.equalsIgnoreCase(p2);
        }
    }

    /**
     * Normalizes a URI component by converting null to empty string and
     * removing trailing slashes.
     *
     * @param s the string to normalize
     * @return normalized string without trailing slashes
     */
    private static String normalize(String s) {
        if (s == null) {
            return "";
        }
        // Remove trailing slashes for consistent comparison
        String trimmed = s.replaceAll("/+$", "");
        return trimmed.isEmpty() ? "" : trimmed;
    }
}
