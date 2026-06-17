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

package org.apache.doris.connector.metastore.spi;

import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Neutral parse helpers shared by the metastore backend parsers. All methods are pure functions of
 * the input maps and hold no Hadoop/SDK state, keeping this module hadoop/fs-free (DV-007). Ported
 * from the paimon connector's hand-copied helpers (the up-move source).
 */
public final class MetaStoreParseUtils {

    /**
     * The backend dispatch signal each {@link MetaStoreProvider} self-identifies on. This is the
     * paimon connector's key (the up-move source); the SPI is paimon-sourced for now (a generic
     * hive/iceberg detector could broaden the signal later — out of P2-T02 scope).
     */
    public static final String CATALOG_TYPE_KEY = "paimon.catalog.type";

    /** Hadoop S3A standard prefix (legacy {@code AbstractPaimonProperties.FS_S3A_PREFIX}). */
    public static final String FS_S3A_PREFIX = "fs.s3a.";

    /**
     * User storage prefixes re-keyed onto {@link #FS_S3A_PREFIX} during the storage overlay
     * (legacy {@code AbstractPaimonProperties.userStoragePrefixes}).
     */
    public static final String[] USER_STORAGE_PREFIXES = {
            "paimon.s3.", "paimon.s3a.", "paimon.fs.s3.", "paimon.fs.oss."};

    private MetaStoreParseUtils() {
    }

    /**
     * Returns the first non-blank value among the given keys, or {@code null} if none is set.
     * Mirrors the alias-priority semantics of {@code @ConnectorProperty(names=...)}.
     */
    public static String firstNonBlank(Map<String, String> props, String... keys) {
        for (String key : keys) {
            String value = props.get(key);
            if (StringUtils.isNotBlank(value)) {
                return value;
            }
        }
        return null;
    }

    /** Emits {@code (key, props.get(key))} to {@code setter} when the value is present and non-blank. */
    public static void copyIfPresent(Map<String, String> props, String key, BiConsumer<String, String> setter) {
        String value = props.get(key);
        if (StringUtils.isNotBlank(value)) {
            setter.accept(key, value);
        }
    }

    /** Returns {@code ""} for a null input, otherwise the input unchanged. */
    public static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    /**
     * Two-step storage overlay (legacy {@code AbstractPaimonProperties} precedence order): first the
     * pre-computed canonical object-store config, then the original
     * {@code paimon.s3./s3a./fs.s3./fs.oss.} re-key plus raw {@code fs./dfs./hadoop.} passthrough,
     * which run LAST and overlay the canonical translation (last-write-wins). HDFS is absent from
     * {@code storageHadoopConfig} and reaches the conf via the raw passthrough.
     */
    public static void applyStorageConfig(Map<String, String> storageHadoopConfig,
            Map<String, String> props, BiConsumer<String, String> setter) {
        storageHadoopConfig.forEach(setter);
        props.forEach((key, value) -> {
            for (String prefix : USER_STORAGE_PREFIXES) {
                if (key.startsWith(prefix)) {
                    setter.accept(FS_S3A_PREFIX + key.substring(prefix.length()), value);
                    return; // stop after the first matching prefix (legacy normalizeS3Config)
                }
            }
            if (key.startsWith("fs.") || key.startsWith("dfs.") || key.startsWith("hadoop.")) {
                setter.accept(key, value);
            }
        });
    }

    /**
     * The subset of {@code raw} actually matched by the {@code @ConnectorProperty} aliases declared on
     * {@code holder} (first-alias-wins, preserving declaration order). Backs
     * {@code MetaStoreProperties.matchedProperties()}.
     */
    public static Map<String, String> matchedProperties(Object holder, Map<String, String> raw) {
        Map<String, String> matched = new LinkedHashMap<>();
        for (Field field : ConnectorPropertiesUtils.getConnectorProperties(holder.getClass())) {
            String name = ConnectorPropertiesUtils.getMatchedPropertyName(field, raw);
            if (name != null) {
                matched.put(name, raw.get(name));
            }
        }
        return matched;
    }
}
