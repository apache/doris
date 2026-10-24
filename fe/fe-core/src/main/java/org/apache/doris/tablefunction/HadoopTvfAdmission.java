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

package org.apache.doris.tablefunction;

import org.apache.doris.common.Config;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.nereids.exceptions.AnalysisException;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * TVF-only admission for Hadoop consumers. Input properties have already been normalized by storage.
 * This module neither loads Hadoop resources nor constructs a filesystem. Java strings and native
 * C strings are projected separately before filesystem selection and mount-table interpretation.
 */
public final class HadoopTvfAdmission {
    public enum Access {
        NATIVE_WRITE,
        JAVA_DELETE,
        HADOOP_READ
    }

    private enum Provider {
        REMOTE,
        LOCAL,
        MOUNT_TABLE,
        DYNAMIC,
        UNKNOWN
    }

    private static final Set<String> REMOTE_SCHEMES = new HashSet<>(Arrays.asList(
            "hdfs", "s3a", "s3n", "oss", "ofs", "jfs", "abfs", "abfss", "wasb", "wasbs", "gs", "cosn"));
    private static final Set<String> REMOTE_IMPLEMENTATIONS = new HashSet<>(Arrays.asList(
            "org.apache.hadoop.hdfs.DistributedFileSystem",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem",
            "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem",
            "org.apache.hadoop.fs.azure.NativeAzureFileSystem",
            "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem",
            "com.aliyun.jindodata.oss.JindoOssFileSystem",
            "org.apache.hadoop.fs.obs.OBSFileSystem",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"));

    private final Access access;
    private final Map<String, String> properties = new HashMap<>();

    private HadoopTvfAdmission(Access access, Map<String, String> normalized, boolean nativeConsumer) {
        this.access = access;
        Set<String> nativeRoutingKeys = new HashSet<>();
        for (Map.Entry<String, String> entry : normalized.entrySet()) {
            // parse_properties / generateHdfsParam consumes the canonical NameNode before JNI.
            if (nativeConsumer && "fs.defaultFS".equalsIgnoreCase(entry.getKey())) {
                continue;
            }
            if (nativeConsumer && isRoutingKey(asciiCaseFold(nativeStringPrefix(entry.getKey()).trim()))) {
                checkNativeCharacters(access, entry.getKey());
                checkNativeCharacters(access, entry.getValue());
            }
            String key = (nativeConsumer ? nativeStringPrefix(entry.getKey()) : entry.getKey()).trim();
            // Native writes first pass through StringCaseMap, before JNI truncates strings.
            // Its comparator uses strncasecmp followed by the full UTF-8 byte length.
            if (nativeConsumer && access == Access.NATIVE_WRITE && isRoutingKey(asciiCaseFold(key))) {
                String identity = asciiCaseFold(nativeStringPrefix(entry.getKey())) + "\0"
                        + entry.getKey().getBytes(StandardCharsets.UTF_8).length;
                if (!nativeRoutingKeys.add(identity)) {
                    reject();
                }
            }
            String value = nativeConsumer ? nativeStringPrefix(entry.getValue()) : entry.getValue();
            String previous = properties.put(key, value);
            if (previous != null && !previous.equals(value) && isRoutingKey(key)) {
                reject();
            }
        }
    }

    /** Reject local/unknown Hadoop TVF routes before listing, deletion, schema discovery or data IO. */
    public static void check(Access access, String path, Map<String, String> normalized) {
        check(access, path, normalized, true);
    }

    /** A declared CSV schema skips the schema RPC; listing and data admission still apply. */
    public static void check(Access access, String path, Map<String, String> normalized,
            boolean needsSchemaDiscovery) {
        if (access != Access.HADOOP_READ && Config.enable_local_tvf) {
            return;
        }
        try {
            if (access == Access.NATIVE_WRITE) {
                String nameNode = nativeNameNode(access, normalized);
                URI output = nativeFileUri(access, path + "doris-tvf-file", nameNode);
                String parent = output.getPath();
                parent = parent.substring(0, parent.lastIndexOf('/') + 1);
                if (parent.length() > 1) {
                    parent = parent.substring(0, parent.length() - 1);
                }
                new HadoopTvfAdmission(access, normalized, true).checkNative(nameNode, parent);
            } else {
                HadoopTvfAdmission javaConsumer = new HadoopTvfAdmission(access, normalized, false);
                // Project the Java consumer independently from native convert_path/JNI.
                URI javaPath = hadoopPathUri(path.trim());
                javaConsumer.checkRoute(javaPath, javaPath.getPath(), new HashSet<>());
                if (access == Access.HADOOP_READ) {
                    // FileQueryScanNode sends this filesystem identifier in range.fs_name. BE
                    // falls back to hdfs_params.fs_name only when that range field is empty.
                    URI fileUri = LocationPath.of(path).getPath().toUri();
                    String nameNode = fileUri.getScheme() + "://" + fileUri.getAuthority();
                    if (nameNode.isEmpty()) {
                        nameNode = nativeNameNode(access, normalized);
                    }
                    HadoopTvfAdmission nativeConsumer = new HadoopTvfAdmission(access, normalized, true);
                    nativeConsumer.checkNativeRead(nameNode, path);
                    // Schema RPCs have no range.fs_name; they use THdfsParams.fs_name instead.
                    if (needsSchemaDiscovery) {
                        nativeConsumer.checkNativeRead(nativeNameNode(access, normalized), path);
                    }
                }
            }
        } catch (URISyntaxException e) {
            new HadoopTvfAdmission(access, normalized, false).reject();
        }
    }

    private static String nativeNameNode(Access access, Map<String, String> normalized) {
        String result = null;
        for (Map.Entry<String, String> entry : normalized.entrySet()) {
            if ("fs.defaultFS".equalsIgnoreCase(entry.getKey())) {
                if (result != null && !result.equals(entry.getValue())) {
                    reject(access);
                }
                result = entry.getValue();
            }
        }
        return result == null ? "" : result;
    }

    private void checkNative(String nameNode, String path) throws URISyntaxException {
        checkNativeCharacters(access, nameNode);
        nameNode = selector(nativeStringPrefix(nameNode)).trim();
        if (nameNode.isEmpty() && access == Access.NATIVE_WRITE) {
            // BE passes fs_name.c_str(), never NULL. libhdfs prefixes this empty string
            // with hdfs://, whose URI construction fails before any filesystem is selected.
            // Preserve existing analysis; execution reports the invalid NameNode as before.
            return;
        }
        if ("default".equals(nameNode) || nameNode.isEmpty()) {
            reject();
        }
        checkRoute(new URI(nameNode.contains("://") ? nameNode : "hdfs://" + nameNode),
                path, new HashSet<>());
    }

    private void checkNativeRead(String nameNode, String path) throws URISyntaxException {
        checkNative(nameNode, nativeFileUri(access, path, nameNode).getPath());
    }

    private List<URI> defaults() throws URISyntaxException {
        List<URI> defaults = new ArrayList<>();
        for (String key : Arrays.asList("fs.defaultFS", "fs.default.name")) {
            String value = property(key);
            if (value != null) {
                value = selector(value).trim();
                // FileSystem.fixName treats slash-free host[:port] strings as HDFS defaults.
                defaults.add(new URI("local".equals(value) ? "file:///"
                        : (value.indexOf('/') < 0 ? "hdfs://" + value : value)));
            }
        }
        if (defaults.isEmpty()) {
            defaults.add(new URI("file:///"));
        }
        return defaults;
    }

    private void checkRoute(URI filesystem, String path, Set<String> activeTables) throws URISyntaxException {
        if (filesystem.getScheme() == null) {
            for (URI defaultFs : defaults()) {
                checkViewFsEnabled(defaultFs, path, activeTables);
            }
            return;
        }
        if (filesystem.getAuthority() == null) {
            boolean completed = false;
            for (URI defaultFs : defaults()) {
                if (filesystem.getScheme().equals(defaultFs.getScheme()) && defaultFs.getAuthority() != null) {
                    checkViewFsEnabled(defaultFs, path, activeTables);
                    completed = true;
                }
            }
            if (completed) {
                return;
            }
        }
        checkViewFsEnabled(filesystem, path, activeTables);
    }

    private Provider provider(String scheme) {
        if ("file".equalsIgnoreCase(scheme)) {
            return Provider.LOCAL;
        }
        String implementation = property("fs." + scheme + ".impl");
        if (implementation != null) {
            implementation = selector(implementation).trim();
            if ("org.apache.hadoop.fs.LocalFileSystem".equals(implementation)
                    || "org.apache.hadoop.fs.RawLocalFileSystem".equals(implementation)) {
                return Provider.LOCAL;
            }
            if ("org.apache.hadoop.fs.viewfs.ViewFileSystem".equals(implementation)) {
                return Provider.MOUNT_TABLE;
            }
            if ("org.apache.hadoop.fs.viewfs.ViewFileSystemOverloadScheme".equals(implementation)
                    || "org.apache.hadoop.hdfs.ViewDistributedFileSystem".equals(implementation)) {
                return Provider.DYNAMIC;
            }
            return REMOTE_IMPLEMENTATIONS.contains(implementation) ? Provider.REMOTE : Provider.UNKNOWN;
        }
        if ("viewfs".equals(scheme)) {
            return Provider.MOUNT_TABLE;
        }
        return REMOTE_SCHEMES.contains(scheme) ? Provider.REMOTE : Provider.UNKNOWN;
    }

    private String[] targets(String key, String prefix) {
        String value = selector(property(key));
        // InodeTree treats SINGLE, SINGLE_FALLBACK and MERGE_SLASH as one URI.
        // Commas delimit targets only for MERGE and NFLY links.
        String entry = key.substring(prefix.length());
        boolean multiple = entry.startsWith("linkMerge.") || entry.startsWith("linkNfly.");
        return multiple ? value.split(",", -1) : new String[] {value};
    }

    private void checkGlobTargets(String key, String prefix, Set<String> activeTables) throws URISyntaxException {
        for (String target : targets(key, prefix)) {
            if (target.isEmpty()) {
                reject();
            }
            URI uri = new URI(target);
            // Listing can touch any descendant of a selected mount. Reuse the same route
            // interpreter for nested tables instead of assuming their root is representative.
            String path = uri.getPath() == null ? "/" : uri.getPath();
            checkRoute(uri, (path.endsWith("/") ? path : path + "/") + "*", activeTables);
        }
    }

    private void checkViewFsEnabled(URI filesystem, String path, Set<String> activeTables) {
        String scheme = filesystem.getScheme();
        Provider provider = provider(scheme);
        if (provider == Provider.LOCAL || provider == Provider.UNKNOWN || provider == Provider.DYNAMIC) {
            reject();
        }
        if (provider != Provider.MOUNT_TABLE) {
            return;
        }
        if (!path.startsWith("/")) {
            // Java resolves relative paths against the selected filesystem's working directory.
            // Its user/home configuration is not a proven absolute mount-table path here.
            reject();
        }
        String table = filesystem.getAuthority();
        String ignorePort = property("fs.viewfs.ignore.port.in.mount.table.name");
        if (ignorePort != null && Boolean.parseBoolean(selector(ignorePort).trim())
                && table != null && filesystem.getPort() != -1) {
            table = filesystem.getHost();
            if (table == null) {
                reject();
            }
        }
        if (table == null || table.isEmpty()) {
            table = property("fs.viewfs.mounttable.default.name.key");
            table = table == null ? "default" : selector(table);
        }
        // Nested ViewFS cycles or arbitrarily deep delegation cannot establish remote storage.
        if (activeTables.size() >= 32 || !activeTables.add(table)) {
            reject();
        }
        try {
            String prefix = "fs.viewfs.mounttable." + table + ".";
            String source = "";
            List<String> selectedKeys = new ArrayList<>();
            List<String> mountSources = new ArrayList<>();
            String fallback = null;
            String rootMerge = null;
            for (String key : properties.keySet()) {
                if (!key.startsWith(prefix)) {
                    continue;
                }
                String entry = key.substring(prefix.length());
                if (entry.equals("linkFallback")) {
                    fallback = key;
                    continue;
                }
                if (entry.equals("linkMergeSlash")) {
                    rootMerge = key;
                    continue;
                }
                if (entry.startsWith("linkRegex")) {
                    // Regex interception rewrites runtime paths before ordinary mount resolution.
                    reject();
                }
                int slash = entry.indexOf('/');
                if (slash < 0 || !(entry.startsWith("link.") || entry.startsWith("linkMerge.")
                        || entry.startsWith("linkNfly."))) {
                    continue;
                }
                String candidate = entry.substring(slash);
                // InodeTree splits source components and discards trailing empty components.
                while (candidate.length() > 1 && candidate.endsWith("/")) {
                    candidate = candidate.substring(0, candidate.length() - 1);
                }
                mountSources.add(candidate);
                if (!(path.equals(candidate) || path.startsWith(candidate + "/"))) {
                    continue;
                }
                if (candidate.length() > source.length()) {
                    source = candidate;
                    selectedKeys.clear();
                }
                if (candidate.equals(source)) {
                    selectedKeys.add(key);
                }
            }
            if (access == Access.HADOOP_READ && hasGlob(path)) {
                String literal = path.substring(0, firstGlob(path));
                int slash = literal.lastIndexOf('/');
                literal = slash < 0 ? "/" : literal.substring(0, slash + 1);
                for (String key : properties.keySet()) {
                    if (!key.startsWith(prefix)) {
                        continue;
                    }
                    String entry = key.substring(prefix.length());
                    int sourceStart = entry.indexOf('/');
                    if (sourceStart >= 0 && (entry.startsWith("link.") || entry.startsWith("linkMerge.")
                            || entry.startsWith("linkNfly."))) {
                        String mount = entry.substring(sourceStart);
                        if (mount.startsWith(literal) || literal.startsWith(mount + "/")) {
                            checkGlobTargets(key, prefix, activeTables);
                        }
                    }
                }
            }
            if (access == Access.HADOOP_READ && !hasGlob(path)) {
                String subtree = path.endsWith("/") ? path : path + "/";
                for (String key : properties.keySet()) {
                    if (key.startsWith(prefix)) {
                        String entry = key.substring(prefix.length());
                        int sourceStart = entry.indexOf('/');
                        if (sourceStart >= 0 && entry.substring(sourceStart).startsWith(subtree)) {
                            checkGlobTargets(key, prefix, activeTables);
                        }
                    }
                }
            }
            if (rootMerge != null) {
                selectedKeys.add(rootMerge);
                source = "";
            } else if (selectedKeys.isEmpty() && fallback != null) {
                selectedKeys.add(fallback);
            }
            if (selectedKeys.isEmpty()) {
                // Missing inline fallback does not prove absence of one in default resources.
                // Keep rejecting unproven routes, including globs that cross mount names.
                reject();
            }
            for (String key : selectedKeys) {
                for (String target : targets(key, prefix)) {
                    if (target.isEmpty()) {
                        reject();
                    }
                    URI uri = new URI(target);
                    if (uri.getScheme() == null) {
                        reject();
                    }
                    if (provider(uri.getScheme()) == Provider.MOUNT_TABLE) {
                        for (String nested : mountSources) {
                            if (!nested.startsWith(source + "/")) {
                                continue;
                            }
                            int branchEnd = nested.indexOf('/', source.length() + 1);
                            String branch = branchEnd < 0 ? nested : nested.substring(0, branchEnd);
                            if (path.equals(branch) || path.startsWith(branch + "/")) {
                                // A missing child below an internal nested-mount directory uses
                                // a different suffix in InodeTree. Do not guess its delegation.
                                reject();
                            }
                        }
                    }
                    String base = uri.getPath() == null ? "/" : uri.getPath();
                    String remaining = path.substring(source.length());
                    String nextPath = new URI(null, null,
                            (base.endsWith("/") ? base : base + "/")
                                    + (remaining.startsWith("/") ? remaining.substring(1) : remaining), null)
                            .normalize().getPath();
                    checkRoute(uri, nextPath, activeTables);
                }
            }
        } catch (URISyntaxException e) {
            reject();
        } finally {
            activeTables.remove(table);
        }
    }

    private static URI nativeFileUri(Access access, String path, String nameNode)
            throws URISyntaxException {
        checkNativeCharacters(access, path);
        // Match BE convert_path before the JNI C-string conversion. A different URI prefix
        // becomes a literal rooted Hadoop path; it must not be parsed as that URI's path.
        if (!nameNode.isEmpty() && path.contains(nameNode)) {
            if (!path.startsWith(nameNode)) {
                // convert_path truncates bytes at NameNode.length even for a non-prefix match.
                // Java character offsets cannot establish that malformed path's mount identity.
                reject(access);
            }
            path = path.substring(nameNode.length());
        }
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        return hadoopPathUri(nativeStringPrefix(path));
    }

    private static URI hadoopPathUri(String path) throws URISyntaxException {
        String scheme = uriScheme(path);
        String authority = null;
        String relative = scheme.isEmpty() ? path : path.substring(scheme.length() + 1);
        if (relative.startsWith("//")) {
            int slash = relative.indexOf('/', 2);
            authority = slash < 0 ? relative.substring(2) : relative.substring(2, slash);
            relative = slash < 0 ? "/" : relative.substring(slash);
        }
        if (!scheme.isEmpty() && !relative.startsWith("/")) {
            relative = "/" + relative;
        }
        // Hadoop Path(String) quotes path bytes before URI normalization; percent text is literal.
        return new URI(scheme.isEmpty() ? null : scheme, authority, relative, null, null).normalize();
    }

    private static String uriScheme(String path) {
        int colon = path.indexOf(':');
        int slash = path.indexOf('/');
        return colon > 0 && (slash < 0 || colon < slash) ? path.substring(0, colon) : "";
    }

    private String property(String key) {
        return properties.get(key);
    }

    private String selector(String value) {
        if (value.contains("${")) {
            // Runtime environment and Hadoop substitutions are not available to this FE projection.
            reject();
        }
        return value;
    }

    private static boolean isRoutingKey(String key) {
        return "fs.defaultFS".equalsIgnoreCase(key) || "fs.default.name".equals(key)
                || key.startsWith("fs.viewfs.") || (key.startsWith("fs.") && key.endsWith(".impl"));
    }

    private static int firstGlob(String path) {
        int first = path.length();
        for (char marker : new char[] {'*', '?', '[', '{'}) {
            int index = path.indexOf(marker);
            if (index >= 0) {
                first = Math.min(first, index);
            }
        }
        return first;
    }

    private static boolean hasGlob(String path) {
        return firstGlob(path) < path.length();
    }

    private static String asciiCaseFold(String value) {
        StringBuilder folded = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            folded.append(c >= 'A' && c <= 'Z' ? (char) (c + ('a' - 'A')) : c);
        }
        return folded.toString();
    }

    private static void checkNativeCharacters(Access access, String value) {
        // libhdfs uses NewStringUTF on standard UTF-8 bytes. Surrogates encode differently
        // in modified UTF-8, so supplementary/unpaired input cannot prove the same route.
        for (int i = 0; i < value.length() && value.charAt(i) != '\0'; i++) {
            if (Character.isSurrogate(value.charAt(i))) {
                reject(access);
            }
        }
    }

    private static String nativeStringPrefix(String value) {
        int nul = value.indexOf('\0');
        return nul < 0 ? value : value.substring(0, nul);
    }

    private void reject() {
        reject(access);
    }

    private static void reject(Access access) {
        if (access == Access.HADOOP_READ) {
            throw new AnalysisException("Hadoop TVFs require a proven remote filesystem route. "
                    + "Use local() for local files with LOCAL TVF privileges and secure-path restrictions.");
        }
        LocalTableValuedFunction.checkEnabled();
    }
}
