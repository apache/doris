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

package org.apache.doris.foundation.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.regex.Pattern;

public class PathUtils {
    private static final Pattern TRAILING_SLASHES = Pattern.compile("/+$");

    /**
     * Compares two URI strings for equality with special handling for the "s3" scheme.
     * <p>
     * The comparison rules are:
     * - If the schemes are equal (case-insensitively, per RFC 3986 section 3.1; including both null
     *   for bare paths), or they differ but one of them is "s3", compare the authority
     *   (bucket/host), path, query string, and fragment, ignoring the scheme. Any run of trailing
     *   slashes on the path is insignificant (a location compares equal with or without trailing
     *   path slashes, e.g. "s3://bucket/data", "s3://bucket/data/" and "s3://bucket/data//" are all
     *   equal); the authority is compared as-is because java.net.URI always places the "/" delimiter
     *   in the path, never in the authority. (Exception: a bare schemeless filesystem root "/" and
     *   the empty path "" are treated as distinct locations even though both normalize to the empty
     *   string, so equalsIgnoreSchemeIfOneIsS3("/", "") returns false.) The authority and path are
     *   compared byte-for-byte and case-sensitively over their raw (percent-encoded) forms, because
     *   object-storage keys are case-sensitive; the scheme itself is matched case-insensitively
     *   (RFC 3986 section 3.1). Query strings and fragments are compared by value (case-sensitively);
     *   a URI carrying a query string or fragment differs from one without (null vs non-null).
     * - The following inputs are malformed for object-storage purposes and fall back to exact
     *   (case-sensitive) string comparison: opaque URIs (e.g. "s3:bucket/key" with no "//"); a
     *   non-opaque URI whose scheme is non-null but whose authority is absent (the triple-slash
     *   form, e.g. "s3:///path" or "file:///path"); a network-path reference whose authority is
     *   present but scheme is absent (e.g. "//bucket/path"); and URIs that fail to parse.
     * - Otherwise (different schemes, neither is "s3"), consider the URIs as not equal.
     * <p>
     * This is useful in scenarios where "s3" URIs should be treated as equivalent to other schemes
     * if the host and path match, ignoring scheme differences. The trailing-slash and case handling is
     * applied consistently regardless of whether the two schemes match.
     *
     * @param p1 the first URI string to compare
     * @param p2 the second URI string to compare
     * @return true if the URIs are considered equal under the above rules, false otherwise
     */
    public static boolean equalsIgnoreSchemeIfOneIsS3(String p1, String p2) {
        if (p1 == null || p2 == null) {
            return p1 == null && p2 == null;
        }

        // Fast path: identical raw strings are equal under every normalization rule below, so we
        // can skip the two URI parses (and their Matcher/substring allocations) on the common
        // unchanged-location case. This also makes the identity property hold for inputs that would
        // otherwise fall back to exact string comparison (opaque, triple-slash, network-path).
        if (p1.equals(p2)) {
            return true;
        }

        try {
            URI uri1 = new URI(p1);
            URI uri2 = new URI(p2);

            // Opaque URIs (e.g. "s3:bucket/key" with no "//") have null authority and path, so they
            // would all normalize to "" and compare equal regardless of content. Such URIs are
            // malformed for object-storage purposes; fall back to exact string comparison.
            if (uri1.isOpaque() || uri2.isOpaque()) {
                return p1.equals(p2);
            }

            // Two classes of inputs are malformed for object-storage purposes and fall back to
            // exact string comparison so they cannot spuriously match via the structural path:
            //   1. A URI with a scheme but a null authority (e.g. the triple-slash form
            //      "s3:///path" or "file:///path"): its absent authority would normalize to "" and
            //      could spuriously match another null-authority URI across schemes.
            //   2. A network-path reference: a URI with an authority but a null scheme
            //      (e.g. "//bucket/path"). java.net.URI parses this as scheme=null,
            //      authority="bucket". It is not a valid object-storage location, so without this
            //      guard it would fall through to the structural comparison and spuriously match a
            //      fully-qualified s3 URI ("s3://bucket/path").
            // Schemeless bare paths (null scheme AND null authority, e.g. "/path") are intentionally
            // left to the structural comparison below so identical bare paths still compare equal.
            if ((uri1.getScheme() != null && uri1.getRawAuthority() == null)
                    || (uri2.getScheme() != null && uri2.getRawAuthority() == null)
                    || (uri1.getScheme() == null && uri1.getRawAuthority() != null)
                    || (uri2.getScheme() == null && uri2.getRawAuthority() != null)) {
                return p1.equals(p2);
            }

            // Distinguish the schemeless filesystem root "/" from the empty/current path "". With no
            // scheme and no authority, both normalize() to "" (the trailing-slash strip collapses
            // "/" to ""), which would make them spuriously equal even though they are distinct
            // locations. This must NOT fire when an authority is present (e.g. "s3://bucket/" vs
            // "s3://bucket"), where the trailing slash on the bucket root IS insignificant. The
            // exact-string fallback returns false for "/" vs "" (the fast path above already handled
            // "/" == "/" and "" == "").
            if (uri1.getScheme() == null && uri1.getRawAuthority() == null) {
                String rawPath1 = uri1.getRawPath();
                String rawPath2 = uri2.getRawPath();
                if (("/".equals(rawPath1) && "".equals(rawPath2))
                        || ("".equals(rawPath1) && "/".equals(rawPath2))) {
                    return p1.equals(p2);
                }
            }

            String scheme1 = uri1.getScheme();
            String scheme2 = uri2.getScheme();

            // Null-safe, case-insensitive scheme equality (RFC 3986 section 3.1). Two null schemes
            // (e.g. bare paths) are treated as the same scheme so identical schemeless locations
            // compare equal. The explicit scheme2 != null guard makes the null handling obvious
            // without relying on String.equalsIgnoreCase(null)'s documented behavior.
            boolean sameScheme = scheme1 == null
                    ? scheme2 == null
                    : (scheme2 != null && scheme1.equalsIgnoreCase(scheme2));
            boolean oneIsS3 = "s3".equalsIgnoreCase(scheme1) || "s3".equalsIgnoreCase(scheme2);

            // Different schemes and neither is "s3": treat as different locations.
            if (!sameScheme && !oneIsS3) {
                return false;
            }

            // Same scheme, or cross-scheme where one side is "s3" (object stores are unified under
            // the s3 scheme on the BE): the scheme is irrelevant -- compare only the authority
            // (bucket/host) and the path. The raw (still percent-encoded) components are used so the
            // comparison is byte-for-byte and case-sensitive (e.g. "a%2Fb" differs from "a/b"), since
            // object-storage keys are case-sensitive. Both are normalized so a directory location
            // compares equal with or without a trailing slash. Query strings and fragments are also
            // compared so two locations that differ only there are not treated as identical.
            // The authority is compared as-is (null -> ""); java.net.URI never places trailing
            // slashes in the authority (the "/" delimiter always belongs to the path), so the
            // trailing-slash normalize() is meaningful only for the path.
            return Objects.equals(Objects.toString(uri1.getRawAuthority(), ""),
                            Objects.toString(uri2.getRawAuthority(), ""))
                    && Objects.equals(normalize(uri1.getRawPath()), normalize(uri2.getRawPath()))
                    && Objects.equals(uri1.getRawQuery(), uri2.getRawQuery())
                    && Objects.equals(uri1.getRawFragment(), uri2.getRawFragment());

        } catch (URISyntaxException e) {
            // If URI parsing fails, fall back to exact string comparison.
            return p1.equals(p2);
        }
    }

    /**
     * Normalizes a URI component by converting null to the empty string and removing any run of
     * trailing slashes. Stripping the whole run (not just one slash) means a directory location
     * compares equal regardless of how many trailing slashes it carries, e.g.
     * "/data/" -> "/data", "/data//" -> "/data", "/" and "//" -> "".
     *
     * @param s the string to normalize
     * @return normalized string without trailing slashes
     */
    private static String normalize(String s) {
        if (s == null) {
            return "";
        }
        // Skip the Matcher allocation when there is no trailing slash (the overwhelmingly common
        // case for real object-storage paths), letting the JIT avoid the regex engine entirely.
        if (s.isEmpty() || s.charAt(s.length() - 1) != '/') {
            return s;
        }
        return TRAILING_SLASHES.matcher(s).replaceAll("");
    }
}
