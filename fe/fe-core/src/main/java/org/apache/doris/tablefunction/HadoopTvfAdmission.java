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
import org.apache.doris.common.util.S3Util;
import org.apache.doris.nereids.exceptions.AnalysisException;

import org.apache.hadoop.fs.FileSystem.TvfGuard;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
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

    private final Access access;
    private final TvfGuard.Route routes;
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
        routes = new TvfGuard.Route(properties, access == Access.HADOOP_READ);
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
                    URI listingPath = hadoopPathUri(S3Util.extendGlobs(path.trim()));
                    javaConsumer.routes.checkGlobRoute(listingPath, listingPath.getPath(), new HashSet<>());
                }
                if (access == Access.HADOOP_READ) {
                    // FileQueryScanNode sends this filesystem identifier in range.fs_name. BE
                    // falls back to hdfs_params.fs_name only when that range field is empty.
                    String nameNode = nativeReadNameNode(path, normalized);
                    HadoopTvfAdmission nativeConsumer = new HadoopTvfAdmission(access, normalized, true);
                    nativeConsumer.checkNativeRead(nameNode, path);
                    // Schema RPCs have no range.fs_name; they use THdfsParams.fs_name instead.
                    if (needsSchemaDiscovery) {
                        nativeConsumer.checkNativeRead(nativeNameNode(access, normalized), path);
                    }
                }
            }
        } catch (IOException | URISyntaxException | IllegalArgumentException e) {
            new HadoopTvfAdmission(access, normalized, false).reject();
        }
    }

    private static String nativeReadNameNode(String path, Map<String, String> normalized) {
        URI fileUri = LocationPath.of(path).getPath().toUri();
        String nameNode = fileUri.getScheme() + "://" + fileUri.getAuthority();
        if (nameNode.isEmpty()) {
            nameNode = nativeNameNode(Access.HADOOP_READ, normalized);
        }
        return nameNode;
    }

    /** Reserved internal policy must not be selected by SQL/resource property aliases. */
    public static void removeRuntimePolicy(Map<String, String> properties) {
        properties.keySet().removeIf(key -> asciiCaseFold(nativeStringPrefix(key)).trim()
                .startsWith("fs.doris.tvf."));
    }

    public static Map<String, String> protectJava(Access access, String path, Map<String, String> properties) {
        Map<String, String> guarded = new HashMap<>(properties);
        removeRuntimePolicy(guarded);
        if (access == Access.HADOOP_READ || !Config.enable_local_tvf) {
            try {
                URI uri = hadoopPathUri(path.trim());
                java.util.List<TvfGuard.Rule> rules;
                if (access == Access.HADOOP_READ) {
                    URI listingPath = hadoopPathUri(S3Util.extendGlobs(path.trim()));
                    rules = TvfGuard.globRules(listingPath, listingPath.getPath());
                } else {
                    rules = java.util.Collections.singletonList(new TvfGuard.Rule(uri.toString(), uri.getPath(), true));
                }
                guarded.put(TvfGuard.POLICY_KEY, TvfGuard.encode(rules));
            } catch (IOException | URISyntaxException | IllegalArgumentException e) {
                reject(access);
            }
        }
        return guarded;
    }

    public static Map<String, String> protectNativeWrite(String path, Map<String, String> normalized) {
        Map<String, String> guarded = new HashMap<>(normalized);
        removeRuntimePolicy(guarded);
        if (!Config.enable_local_tvf) {
            guarded.put(TvfGuard.POLICY_KEY, TvfGuard.NATIVE_POLICY);
        }
        return guarded;
    }

    public static Map<String, String> protectNativeRead(Map<String, String> normalized) {
        Map<String, String> guarded = new HashMap<>(normalized);
        removeRuntimePolicy(guarded);
        guarded.put(TvfGuard.POLICY_KEY, TvfGuard.NATIVE_POLICY);
        return guarded;
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

    private void checkRoute(URI filesystem, String path, Set<String> activeTables) throws URISyntaxException {
        routes.checkRoute(filesystem, path, activeTables);
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
