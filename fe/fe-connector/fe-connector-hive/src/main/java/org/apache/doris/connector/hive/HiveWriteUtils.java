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

package org.apache.doris.connector.hive;

import org.apache.doris.thrift.THivePartitionUpdate;

import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Pure, side-effect-free helpers for the Hive connector write path, ported verbatim from the legacy
 * fe-core {@code HMSTransaction}. Deliberately fe-core-free: the only dependencies are the Hadoop
 * {@code Path}, {@code java.net.URI}, and the shared thrift {@code THivePartitionUpdate}. Consumed
 * by the (in-progress) connector write transaction and its staging-cleanup logic.
 */
final class HiveWriteUtils {
    private HiveWriteUtils() {
    }

    /**
     * Merges partition updates that target the same partition name. For collisions the file sizes
     * and row counts are summed and the pending MPU-upload and file-name lists are concatenated onto
     * the first-seen update; distinct names are kept as-is. Mirrors the legacy
     * {@code HMSTransaction.mergePartitions}.
     *
     * <p>The first-seen update's {@code fileNames} / {@code s3MpuPendingUploads} lists are mutated in
     * place, so they must be mutable (thrift deserialization produces mutable {@code ArrayList}s).
     */
    static List<THivePartitionUpdate> mergePartitions(List<THivePartitionUpdate> hivePartitionUpdates) {
        Map<String, THivePartitionUpdate> merged = new HashMap<>();
        for (THivePartitionUpdate pu : hivePartitionUpdates) {
            if (merged.containsKey(pu.getName())) {
                THivePartitionUpdate old = merged.get(pu.getName());
                old.setFileSize(old.getFileSize() + pu.getFileSize());
                old.setRowCount(old.getRowCount() + pu.getRowCount());
                if (old.getS3MpuPendingUploads() != null && pu.getS3MpuPendingUploads() != null) {
                    old.getS3MpuPendingUploads().addAll(pu.getS3MpuPendingUploads());
                }
                old.getFileNames().addAll(pu.getFileNames());
            } else {
                merged.put(pu.getName(), pu);
            }
        }
        return new ArrayList<>(merged.values());
    }

    /**
     * Returns true when {@code child} is a strict subdirectory of {@code parent} on the same file
     * system (matching scheme + authority, path-prefix under a {@code /} boundary).
     */
    static boolean isSubDirectory(String parent, String child) {
        if (parent == null || child == null) {
            return false;
        }
        Path parentPath = new Path(parent);
        Path childPath = new Path(child);
        URI parentUri = parentPath.toUri();
        URI childUri = childPath.toUri();
        if (!sameFileSystem(parentUri, childUri)) {
            return false;
        }
        String parentPathValue = normalizePath(parentUri.getPath());
        String childPathValue = normalizePath(childUri.getPath());
        if (parentPathValue.isEmpty() || childPathValue.isEmpty()) {
            return false;
        }
        return !parentPathValue.equals(childPathValue)
                && childPathValue.startsWith(parentPathValue + "/");
    }

    /**
     * Returns the first-level child path of {@code parent} that contains {@code child},
     * or null if {@code child} is not a subdirectory of {@code parent}.
     * Example: parent=/warehouse/table, child=/warehouse/table/.doris_staging/user/uuid
     * returns /warehouse/table/.doris_staging.
     */
    static String getImmediateChildPath(String parent, String child) {
        if (!isSubDirectory(parent, child)) {
            return null;
        }
        Path parentPath = new Path(parent);
        URI parentUri = parentPath.toUri();
        URI childUri = new Path(child).toUri();
        String parentPathValue = normalizePath(parentUri.getPath());
        String childPathValue = normalizePath(childUri.getPath());
        String relative = childPathValue.substring(parentPathValue.length() + 1);
        int slashIndex = relative.indexOf("/");
        String firstComponent = slashIndex == -1 ? relative : relative.substring(0, slashIndex);
        return new Path(parentPath, firstComponent).toString();
    }

    /**
     * Returns true when {@code left} and {@code right} resolve to the same normalized path on the
     * same file system. Null-safe: two nulls are equal, one null is not.
     */
    static boolean pathsEqual(String left, String right) {
        if (left == null || right == null) {
            return left == null && right == null;
        }
        URI leftUri = new Path(left).toUri();
        URI rightUri = new Path(right).toUri();
        if (!sameFileSystem(leftUri, rightUri)) {
            return false;
        }
        return normalizePath(leftUri.getPath()).equals(normalizePath(rightUri.getPath()));
    }

    /**
     * Compares two URI strings for equality with special handling for the "s3" scheme. Byte-faithful port
     * of fe-core {@code PathUtils.equalsIgnoreSchemeIfOneIsS3} (NOT the same as {@link #pathsEqual}, which
     * treats a scheme mismatch as a different file system): in the BE all object stores are unified under
     * the "s3" URI scheme, so a path written with a different underlying scheme (e.g. "oss://") is the SAME
     * physical location as the "s3://" form. Used by the committer's {@code needRename} decision so an
     * in-place object-store write is not needlessly renamed.
     *
     * <p>Rules: same scheme -&gt; case-insensitive full-string compare; different schemes but one is "s3"
     * -&gt; compare only authority + path (trailing slashes stripped); otherwise not equal.
     */
    static boolean equalsIgnoreSchemeIfOneIsS3(String p1, String p2) {
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
                String auth1 = stripTrailingSlashes(uri1.getAuthority());
                String auth2 = stripTrailingSlashes(uri2.getAuthority());
                String path1 = stripTrailingSlashes(uri1.getPath());
                String path2 = stripTrailingSlashes(uri2.getPath());
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
     * Splits a Hive partition name ("c1=a/c2=b/c3=c") into its ordered values ("a", "b", "c"), URL-decoding
     * each value with {@link #unescapePathName}. Byte-faithful port of fe-core {@code HiveUtil.toPartitionValues}
     * (which delegated to Hive's {@code FileUtils.unescapePathName}). Ported inline so the connector needs no
     * hive-common dependency. Used to key the write transaction's per-partition action map and to build the
     * partition-value argument passed to the metastore write primitives.
     */
    static List<String> toPartitionValues(String partitionName) {
        List<String> result = new ArrayList<>();
        int start = 0;
        while (true) {
            while (start < partitionName.length() && partitionName.charAt(start) != '=') {
                start++;
            }
            start++;
            int end = start;
            while (end < partitionName.length() && partitionName.charAt(end) != '/') {
                end++;
            }
            if (start > partitionName.length()) {
                break;
            }
            result.add(unescapePathName(partitionName.substring(start, end)));
            start = end + 1;
        }
        return result;
    }

    /**
     * URL-decodes a Hive-escaped path component (e.g. "a%2Fb" -&gt; "a/b"). Byte-faithful port of Hive's
     * {@code org.apache.hadoop.hive.common.FileUtils.unescapePathName}, inlined to avoid a hive-common
     * dependency.
     */
    private static String unescapePathName(String path) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c == '%' && i + 2 < path.length()) {
                int code = -1;
                try {
                    code = Integer.parseInt(path.substring(i + 1, i + 3), 16);
                } catch (Exception e) {
                    code = -1;
                }
                if (code >= 0) {
                    sb.append((char) code);
                    i += 2;
                    continue;
                }
            }
            sb.append(c);
        }
        return sb.toString();
    }

    /** Strip trailing slashes for {@link #equalsIgnoreSchemeIfOneIsS3} (mirrors PathUtils.normalize). */
    private static String stripTrailingSlashes(String s) {
        if (s == null) {
            return "";
        }
        String trimmed = s.replaceAll("/+$", "");
        return trimmed.isEmpty() ? "" : trimmed;
    }

    private static boolean sameFileSystem(URI left, URI right) {
        String leftScheme = normalizeUriPart(left.getScheme());
        String rightScheme = normalizeUriPart(right.getScheme());
        if (!leftScheme.isEmpty() && !rightScheme.isEmpty()
                && !leftScheme.equalsIgnoreCase(rightScheme)) {
            return false;
        }
        String leftAuthority = normalizeUriPart(left.getAuthority());
        String rightAuthority = normalizeUriPart(right.getAuthority());
        if (!leftAuthority.isEmpty() && !rightAuthority.isEmpty()
                && !leftAuthority.equalsIgnoreCase(rightAuthority)) {
            return false;
        }
        return true;
    }

    private static String normalizeUriPart(String value) {
        return value == null ? "" : value;
    }

    private static String normalizePath(String path) {
        if (path == null || path.isEmpty()) {
            return "";
        }
        int end = path.length();
        while (end > 1 && path.charAt(end - 1) == '/') {
            end--;
        }
        return path.substring(0, end);
    }
}
