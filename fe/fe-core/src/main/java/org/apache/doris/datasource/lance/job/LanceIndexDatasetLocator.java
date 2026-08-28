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

package org.apache.doris.datasource.lance.job;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * Dataset locator normalization v1 for the durable fence key. The rules, in
 * order:
 *
 * <ol>
 *   <li>trim surrounding whitespace;</li>
 *   <li>if a {@code scheme://} prefix is present, lowercase the scheme
 *   (aligned with the {@code LanceStorageProvider.schemeOf} precedent);</li>
 *   <li>a URL whose authority carries userinfo is rejected: credential-bearing
 *   URLs are never identity;</li>
 *   <li>a locator with a scheme but neither an authority nor a path (for
 *   example {@code "s3://"}) carries no identity and is rejected; an empty
 *   authority with a non-empty path ({@code "file:///x"}) stays legal;</li>
 *   <li>trailing {@code '/'} characters are removed, keeping the root
 *   (a scheme-less {@code "/"} stays {@code "/"});</li>
 *   <li>without a scheme the locator must be an absolute path (start with
 *   {@code '/'}), otherwise it is rejected.</li>
 * </ol>
 *
 * <p>The authority (host/bucket) and path keep their original case: bucket and
 * path components are case-sensitive on the providers Doris supports, and
 * normalization v1 deliberately does not define cross-alias equivalence. URI
 * aliases and external writers replacing the dataset at the same URI are
 * outside Doris serialization.
 */
public final class LanceIndexDatasetLocator {
    private static final String SCHEME_SEPARATOR = "://";
    /** Finite bound for the stable locator persisted in every durable job record. */
    public static final int MAX_LOCATOR_BYTES = 4096;

    private LanceIndexDatasetLocator() {
    }

    /**
     * Normalize a raw dataset locator into its durable identity form.
     *
     * @throws IllegalArgumentException if the locator is null/empty/oversized,
     *         is not a valid hierarchical URI or absolute path, carries
     *         userinfo/query/fragment data, has neither an authority nor a path,
     *         or is a scheme-less relative path
     */
    public static String normalize(String rawLocator) {
        if (rawLocator == null) {
            throw new IllegalArgumentException("dataset locator must not be null");
        }
        String locator = rawLocator.trim();
        if (locator.isEmpty()) {
            throw new IllegalArgumentException("dataset locator must not be empty");
        }
        if (locator.getBytes(StandardCharsets.UTF_8).length > MAX_LOCATOR_BYTES) {
            throw new IllegalArgumentException(
                    "dataset locator exceeds " + MAX_LOCATOR_BYTES + " UTF-8 bytes");
        }

        // Preserve the established spelling of the scheme-less filesystem root.
        // java.net.URI rejects "//" as a network-path reference without an
        // authority, while it is a valid absolute filesystem path here.
        if (containsOnlySlashes(locator)) {
            return "/";
        }

        URI uri;
        try {
            uri = new URI(locator);
        } catch (URISyntaxException e) {
            // Do not include the raw locator: it may contain credentials.
            throw new IllegalArgumentException("dataset locator is not a valid URI or absolute path");
        }
        if (uri.getRawUserInfo() != null) {
            throw new IllegalArgumentException(
                    "credential-bearing dataset locators are never identity (userinfo is not allowed)");
        }
        if (uri.getRawQuery() != null) {
            // Presigned object-store URLs and SAS URLs carry credentials here.
            throw new IllegalArgumentException("dataset locator query parameters are not allowed");
        }
        if (uri.getRawFragment() != null) {
            throw new IllegalArgumentException("dataset locator fragments are not allowed");
        }

        String scheme = uri.getScheme();
        if (scheme == null) {
            if (!locator.startsWith("/")) {
                throw new IllegalArgumentException("dataset locator without a scheme must be an absolute path");
            }
            return stripTrailingSlashes(locator, 1);
        }
        if (uri.isOpaque() || !locator.regionMatches(scheme.length(), SCHEME_SEPARATOR, 0,
                SCHEME_SEPARATOR.length())) {
            throw new IllegalArgumentException("dataset locator scheme must use hierarchical '://' syntax");
        }
        String authority = uri.getRawAuthority() == null ? "" : uri.getRawAuthority();
        String rawPath = uri.getRawPath() == null ? "" : uri.getRawPath();
        String path = stripTrailingSlashes(rawPath, 0);
        if (authority.isEmpty() && path.isEmpty()) {
            // "s3://" / "file://" carry no identity at all; "file:///x" (empty
            // authority, non-empty path) is legal and does not reach this.
            throw new IllegalArgumentException("dataset locator has neither an authority nor a path");
        }
        return scheme.toLowerCase(Locale.ROOT) + SCHEME_SEPARATOR + authority + path;
    }

    private static boolean containsOnlySlashes(String value) {
        for (int index = 0; index < value.length(); index++) {
            if (value.charAt(index) != '/') {
                return false;
            }
        }
        return true;
    }

    private static String stripTrailingSlashes(String value, int minLength) {
        int end = value.length();
        while (end > minLength && value.charAt(end - 1) == '/') {
            end--;
        }
        return value.substring(0, end);
    }
}
