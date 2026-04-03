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

package org.apache.doris.filesystem;

import java.util.Objects;

/**
 * Immutable, typed URI wrapper replacing raw String path parameters.
 */
public final class Location {

    private final String uri;

    private Location(String uri) {
        this.uri = Objects.requireNonNull(uri, "uri must not be null");
    }

    public static Location of(String uri) {
        return new Location(uri);
    }

    public String uri() {
        return uri;
    }

    public String scheme() {
        int idx = uri.indexOf("://");
        return idx < 0 ? "" : uri.substring(0, idx);
    }

    public String withoutScheme() {
        int idx = uri.indexOf("://");
        return idx < 0 ? uri : uri.substring(idx + 3);
    }

    /**
     * Returns the last segment of the path (the file or directory name).
     *
     * <p>If the URI ends with {@code "/"} (directory marker), returns an empty string.
     * For example:
     * <ul>
     *   <li>{@code Location.of("s3://bucket/dir/file.parquet").fileName()} → {@code "file.parquet"}</li>
     *   <li>{@code Location.of("s3://bucket/dir").fileName()} → {@code "dir"}</li>
     *   <li>{@code Location.of("s3://bucket/dir/").fileName()} → {@code ""}</li>
     * </ul>
     */
    public String fileName() {
        if (uri.endsWith("/")) {
            return "";
        }
        int idx = uri.lastIndexOf('/');
        return idx < 0 ? uri : uri.substring(idx + 1);
    }

    /**
     * Returns the parent {@link Location}, or {@code null} if there is no parent.
     *
     * <p>For example, {@code Location.of("s3://bucket/dir/file.txt").parent()} returns
     * {@code Location.of("s3://bucket/dir")}.
     */
    public Location parent() {
        String path = uri.endsWith("/") ? uri.substring(0, uri.length() - 1) : uri;
        int idx = path.lastIndexOf('/');
        if (idx <= 0) {
            return null;
        }
        // Guard: don't strip past the authority (e.g. "s3://bucket")
        int schemeEnd = path.indexOf("://");
        if (schemeEnd >= 0 && idx <= schemeEnd + 2) {
            return null;
        }
        return new Location(path.substring(0, idx));
    }

    /**
     * Resolves a child path relative to this location, returning a new {@link Location}.
     *
     * <p>For example, {@code Location.of("s3://bucket/dir").resolve("sub/file.txt")} returns
     * {@code Location.of("s3://bucket/dir/sub/file.txt")}.
     *
     * @param child relative path; leading slashes are stripped automatically
     */
    public Location resolve(String child) {
        Objects.requireNonNull(child, "child must not be null");
        if (child.isEmpty()) {
            return this;
        }
        String base = uri.endsWith("/") ? uri : uri + "/";
        String sanitized = child.startsWith("/") ? child.substring(1) : child;
        return new Location(base + sanitized);
    }

    /**
     * Returns {@code true} if this location is equal to, or a descendant of, the given prefix.
     *
     * <p>The comparison is path-aware: {@code Location.of("s3://b/dir")} is a prefix of
     * {@code Location.of("s3://b/dir/file")} but NOT of {@code Location.of("s3://b/directory")}.
     */
    public boolean startsWith(Location prefix) {
        Objects.requireNonNull(prefix, "prefix must not be null");
        String prefixUri = prefix.uri.endsWith("/") ? prefix.uri : prefix.uri + "/";
        return uri.equals(prefix.uri) || uri.startsWith(prefixUri);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Location)) {
            return false;
        }
        return uri.equals(((Location) o).uri);
    }

    @Override
    public int hashCode() {
        return uri.hashCode();
    }

    @Override
    public String toString() {
        return uri;
    }
}
