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

package org.apache.doris.fs;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

/**
 * Immutable value object representing a storage location (URI).
 * <p>
 * Replaces bare {@code String} path parameters and the thin {@link org.apache.doris.fs.io.ParsedPath}
 * wrapper throughout the filesystem API. Does NOT include StorageProperties lookup logic
 * (that remains in {@link org.apache.doris.common.util.LocationPath}).
 * <p>
 * Examples:
 * <pre>
 *   Location loc = Location.of("s3://my-bucket/path/to/file.parquet");
 *   loc.scheme()    // "s3"
 *   loc.authority() // "my-bucket"
 *   loc.path()      // "/path/to/file.parquet"
 *
 *   Location child = loc.parent().resolve("other.parquet");
 *   // s3://my-bucket/path/to/other.parquet
 * </pre>
 */
public final class Location {

    private final String location;

    // Lazily parsed URI — avoids URI parsing cost for hot paths that only need toString()
    private volatile URI parsedUri;

    private Location(String location) {
        Objects.requireNonNull(location, "location must not be null");
        if (location.isEmpty()) {
            throw new IllegalArgumentException("location must not be empty");
        }
        this.location = location;
    }

    /**
     * Creates a Location from a raw URI string.
     *
     * @throws IllegalArgumentException if the string is null, empty, or malformed
     */
    public static Location of(String location) {
        return new Location(location);
    }

    /** URI scheme, e.g., "s3", "hdfs", "file". Returns empty string if no scheme. */
    public String scheme() {
        URI uri = ensureParsed();
        String s = uri.getScheme();
        return s == null ? "" : s;
    }

    /** URI authority (host + optional port), e.g., "bucket-name" or "namenode:8020". */
    public String authority() {
        URI uri = ensureParsed();
        String a = uri.getAuthority();
        return a == null ? "" : a;
    }

    /** The path component of the URI, e.g., "/warehouse/db/table". */
    public String path() {
        return ensureParsed().getPath();
    }

    /**
     * Returns the parent location (directory containing this location).
     * <p>
     * e.g., {@code s3://bucket/a/b/c} → {@code s3://bucket/a/b}
     */
    public Location parent() {
        URI uri = ensureParsed();
        String rawPath = uri.getRawPath();
        if (rawPath == null || rawPath.isEmpty() || rawPath.equals("/")) {
            throw new IllegalStateException("Location has no parent: " + location);
        }
        // Strip trailing slash before finding last separator
        String normalized = rawPath.endsWith("/") ? rawPath.substring(0, rawPath.length() - 1) : rawPath;
        int lastSlash = normalized.lastIndexOf('/');
        String parentPath = lastSlash <= 0 ? "/" : normalized.substring(0, lastSlash);
        try {
            URI parentUri = new URI(uri.getScheme(), uri.getAuthority(), parentPath, null, null);
            return new Location(parentUri.toString());
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Cannot compute parent of: " + location, e);
        }
    }

    /**
     * Appends a relative path segment to this location, returning a new child Location.
     * <p>
     * e.g., {@code Location.of("s3://bucket/a").resolve("b/c")} → {@code s3://bucket/a/b/c}
     */
    public Location resolve(String relativePath) {
        Objects.requireNonNull(relativePath, "relativePath must not be null");
        String base = location.endsWith("/") ? location : location + "/";
        String child = relativePath.startsWith("/") ? relativePath.substring(1) : relativePath;
        return new Location(base + child);
    }

    /**
     * Returns the last path component (filename or directory name).
     */
    public String name() {
        String p = path();
        if (p.isEmpty() || p.equals("/")) {
            return authority(); // root: use authority as name
        }
        String trimmed = p.endsWith("/") ? p.substring(0, p.length() - 1) : p;
        int lastSlash = trimmed.lastIndexOf('/');
        return lastSlash < 0 ? trimmed : trimmed.substring(lastSlash + 1);
    }

    /** The original location string as provided. */
    @Override
    public String toString() {
        return location;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Location)) {
            return false;
        }
        return location.equals(((Location) o).location);
    }

    @Override
    public int hashCode() {
        return location.hashCode();
    }

    private URI ensureParsed() {
        if (parsedUri == null) {
            synchronized (this) {
                if (parsedUri == null) {
                    try {
                        parsedUri = new URI(location);
                    } catch (URISyntaxException e) {
                        throw new IllegalArgumentException("Invalid location URI: " + location, e);
                    }
                }
            }
        }
        return parsedUri;
    }
}
