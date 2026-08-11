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

package org.apache.doris.filesystem.properties;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;

/**
 * Credential fingerprints for the Hadoop {@code FileSystem.CACHE} key.
 *
 * <p>Hadoop keys cached FileSystem instances by {@code (scheme, authority, UGI)} only, so two
 * catalogs/TVFs reaching the same bucket or namenode with different credentials would share one
 * instance. Doris therefore used to force {@code fs.<schema>.impl.disable.cache=true} everywhere,
 * which makes every access (e.g. every JNI scanner split) build a brand-new FileSystem.
 *
 * <p>Instead, both FE and BE load the Doris-patched {@code org.apache.hadoop.fs.FileSystem}
 * shipped in {@code hadoop-deps.jar}, whose cache key carries one extra dimension read from
 * {@link #FS_CACHE_KEY_PROPERTY}. Vanilla (unpatched) Hadoop ignores the property, and an
 * absent/empty value keeps the vanilla cache-key semantics.
 *
 * <p><b>Why the key is per scheme.</b> The property is written as
 * {@code doris.fs.cache.key.<scheme>}, not as one shared key. A single catalog routinely holds
 * more than one storage (an object store plus HDFS), and every consumer merges their property maps
 * with {@code putAll} into one map / one {@code Configuration}. Under a single shared name that
 * merge is last-writer-wins: every scheme would end up tagged with one arbitrary storage's
 * fingerprint, which is both unstable (map iteration order) and unsafe — two catalogs differing
 * only in their object-store credentials but sharing an HDFS definition could collapse onto the
 * same key. Distinct per-scheme names make the merge lossless, so no merge site has to know this
 * mechanism exists.
 */
public final class FsCacheKeys {

    /**
     * Reserved Hadoop configuration property carrying a credential fingerprint; mixed into the
     * patched {@code FileSystem.CACHE} key. Written per scheme as
     * {@code doris.fs.cache.key.<scheme>} (see {@link #fsCacheKeyProperty}); the patched
     * FileSystem falls back to this scheme-less name when no per-scheme entry is present.
     */
    public static final String FS_CACHE_KEY_PROPERTY = "doris.fs.cache.key";

    private static final int FINGERPRINT_LENGTH = 32;

    /**
     * Raw property prefixes that reach the effective Hadoop {@code Configuration} verbatim, on top
     * of the derived map this class fingerprints, and which no provider declares as a
     * {@code @ConnectorProperty} alias (so {@link StorageProperties#matchedProperties()} cannot see
     * them). Two sites overlay them <em>after</em> the storage's own map, so they win:
     * {@code IcebergCatalogFactory.buildHadoopConfiguration} / {@code HudiScanPlanProvider
     * .buildHadoopConf} (from the catalog properties) and {@code HdfsProperties
     * .extractUserOverriddenHdfsConfig} (into the derived backend map).
     *
     * <p>Deliberately narrower than those overlay filters: {@code hive.} is overlaid by the Hudi
     * site too but configures the metastore client, never a FileSystem, and mixing it in would only
     * cost cache entries.
     */
    private static final String[] HADOOP_OVERLAY_PREFIXES = {"fs.", "dfs.", "hadoop.", "juicefs."};

    /**
     * Namespace for entries of a <em>derived</em> map mixed into an identity (see
     * {@link #derivedIdentityKey}). Keeps a derived key from silently shadowing the raw key of the
     * same name when the two disagree — e.g. {@code fs.defaultFS} extracted from {@code uri} versus
     * one the user spelled out.
     */
    private static final String DERIVED_KEY_NAMESPACE = "@derived.";

    private FsCacheKeys() {
    }

    /** The reserved property name carrying the fingerprint for {@code scheme}. */
    public static String fsCacheKeyProperty(String scheme) {
        return FS_CACHE_KEY_PROPERTY + "." + scheme.toLowerCase(Locale.ROOT);
    }

    /**
     * SHA-256 over {@code salt} and the sorted properties, truncated to 32 hex chars. The same
     * (salt, properties) pair always yields the same fingerprint — cache hits are preserved across
     * queries — while any credential or config change yields a new one.
     */
    public static String fingerprintOf(String salt, Map<String, String> props) {
        StringBuilder sb = new StringBuilder();
        appendFramed(sb, salt);
        if (props != null) {
            new TreeMap<>(props).forEach((k, v) -> {
                appendFramed(sb, k);
                appendFramed(sb, v);
            });
        }
        return sha256Hex(sb.toString()).substring(0, FINGERPRINT_LENGTH);
    }

    /**
     * Appends {@code s} length-prefixed, so that a concatenation of such appends decodes back to
     * exactly one sequence of strings.
     *
     * <p>Framing is required, not cosmetic: property names and values are both caller-controlled,
     * so a delimiter-joined encoding is not injective. Under a plain {@code "\nkey=value"} join, a
     * single value carrying embedded newlines reproduces the encoding of several separate entries —
     * {@code {"fs.ignored": "\nfs.s3a.access.key=AK"}} and {@code {"fs.ignored": "",
     * "fs.s3a.access.key": "AK"}} serialize identically, yet the connector's raw {@code fs.*}
     * overlay hands the first to Hadoop as one ignored value and the second as real S3A
     * credentials. Equal fingerprints there would hand both definitions the same cached
     * FileSystem, which is precisely the credential isolation this class exists to provide.
     *
     * <p>{@code null} is framed as length {@code -1}, keeping it distinct from the empty string.
     */
    private static void appendFramed(StringBuilder sb, String s) {
        if (s == null) {
            sb.append("-1:");
            return;
        }
        sb.append(s.length()).append(':').append(s);
    }

    /**
     * Fingerprint of one bound storage definition: its concrete class name plus
     * {@link #identityProperties}.
     */
    public static String fingerprintOf(StorageProperties properties) {
        return fingerprintOf(properties.getClass().getName(), identityProperties(properties));
    }

    /**
     * The user-supplied inputs that decide what FileSystem a definition opens: the properties it
     * matched during binding ({@link StorageProperties#matchedProperties()}, credentials included)
     * plus the raw {@link #HADOOP_OVERLAY_PREFIXES} entries.
     *
     * <p>The matched set alone is <b>not</b> a sufficient identity: it only holds
     * provider-declared aliases, while the effective Hadoop configuration is finished by overlaying
     * the raw keys on top. Two definitions agreeing on every typed alias but setting different
     * {@code fs.s3a.access.key} / {@code dfs.namenode.rpc-address.<ns>.<nn>} values talk to
     * different credentials or even different clusters, and must not share a cached FileSystem.
     *
     * <p>Where the same key appears in both sets the value is identical — both read it out of the
     * same raw map — so the merge cannot lose a distinguishing bit.
     */
    public static Map<String, String> identityProperties(StorageProperties properties) {
        Map<String, String> identity = new TreeMap<>();
        Map<String, String> matched = properties.matchedProperties();
        if (matched != null) {
            identity.putAll(matched);
        }
        Map<String, String> raw = properties.rawProperties();
        if (raw != null) {
            raw.forEach((key, value) -> {
                if (isHadoopOverlayKey(key)) {
                    identity.put(key, value);
                }
            });
        }
        return identity;
    }

    /**
     * Namespaced name under which an entry of a derived (already resolved) map is mixed into an
     * identity, for a provider whose derived map carries inputs the raw properties do not — e.g.
     * the HDFS families, which resolve {@code hadoop.config.resources} XML files into theirs, so
     * that editing such a file yields a new fingerprint.
     */
    public static String derivedIdentityKey(String key) {
        return DERIVED_KEY_NAMESPACE + key;
    }

    private static boolean isHadoopOverlayKey(String key) {
        if (key == null) {
            return false;
        }
        for (String prefix : HADOOP_OVERLAY_PREFIXES) {
            if (key.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The schemes a storage's fingerprint must be published under: everything it can be addressed
     * by, plus the legacy per-scheme cache-control set.
     *
     * <p>Neither set alone is sufficient. {@link FileSystemProperties#legacyCacheSchemes()} names what
     * the retired {@code fs.<schema>.impl.disable.cache} loop wrote and is wrong for the
     * S3-compatible dialects here — COS declares {@code {cos, cosn}}, yet Doris normalizes
     * {@code cos://} to {@code s3://} and the FileSystem is actually opened as {@code s3a}, so a
     * fingerprint published only under {@code cos} would never be read. (That mattered less for the
     * disable flag, which was written defensively over several names.)
     * {@link FileSystemProperties#getSupportedSchemes()} carries the real addressing aliases but is
     * empty for providers without a scheme identity, where the legacy set is all there is.
     */
    public static Set<String> fsCacheSchemes(FileSystemProperties properties) {
        Set<String> schemes = new LinkedHashSet<>();
        for (String scheme : properties.getSupportedSchemes()) {
            addScheme(schemes, scheme);
        }
        for (String scheme : properties.legacyCacheSchemes()) {
            addScheme(schemes, scheme);
        }
        return schemes;
    }

    /**
     * Publishes {@code properties}' fingerprint into {@code target} under every scheme it serves.
     * A no-op for a storage with no scheme identity (Broker, Local): those carry no credentials the
     * Hadoop FileSystem cache could confuse.
     *
     * <p>Call this from whatever builds a consumable property map, so that merging several storages
     * needs no special handling: the entries have distinct names and simply coexist.
     *
     * <p>Two storages of the same catalog CAN still declare one scheme in common (OSS-HDFS serves
     * {@code oss://} but also declares {@code hdfs}, and is a different {@code StorageTypeId} from
     * plain HDFS, so both can be bound at once). That scheme then resolves last-writer-wins, which
     * is the pre-existing behavior for every other colliding key in such a merge (e.g.
     * {@code fs.defaultFS}) — an ambiguous configuration, not a regression introduced here.
     *
     * <p>Be explicit about what that costs, though: in such a catalog the shared scheme gets
     * <em>no</em> isolation from this mechanism. Two catalogs differing only in their HDFS
     * credentials but sharing one OSS-HDFS definition publish the same {@code hdfs} fingerprint and
     * can share a cached FileSystem — exactly as they already do on a build without this feature,
     * since the HDFS families never emitted {@code fs.hdfs.impl.disable.cache} either. Splitting
     * the two definitions into separate catalogs is the way to get isolation back.
     */
    public static void putFsCacheKeys(Map<String, String> target, FileSystemProperties properties) {
        publishFsCacheKeys(properties, target::put);
    }

    /**
     * {@link #putFsCacheKeys} for sinks that are not a {@code Map} — e.g. a Hadoop
     * {@code Configuration}, which this module cannot reference.
     */
    public static void publishFsCacheKeys(FileSystemProperties properties, BiConsumer<String, String> sink) {
        Collection<String> schemes = fsCacheSchemes(properties);
        if (schemes.isEmpty()) {
            return;
        }
        String fingerprint = properties.fsCacheFingerprint();
        for (String scheme : schemes) {
            sink.accept(fsCacheKeyProperty(scheme), fingerprint);
        }
    }

    private static void addScheme(Set<String> target, String scheme) {
        if (scheme != null && !scheme.trim().isEmpty()) {
            target.add(scheme.trim().toLowerCase(Locale.ROOT));
        }
    }

    private static String sha256Hex(String input) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandated by the JLS; unreachable on any conformant JRE.
            throw new IllegalStateException("SHA-256 is not available", e);
        }
        byte[] bytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
        StringBuilder hex = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            hex.append(Character.forDigit((b >> 4) & 0xF, 16));
            hex.append(Character.forDigit(b & 0xF, 16));
        }
        return hex.toString();
    }
}
