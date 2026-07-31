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

package org.apache.doris.connector.paimon;

import org.apache.commons.lang3.StringUtils;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.FallbackKey;
import org.apache.paimon.options.Options;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * The {@code paimon.table-option.*} catalog-property namespace (upstream #65955): a catalog-level
 * default for any Paimon {@link CoreOptions} table option, e.g.
 * {@code "paimon.table-option.read.batch-size" = "4096"}.
 *
 * <p>Ports the fe-core {@code AbstractPaimonProperties} table-option members deleted by P5-T29
 * ({@code TABLE_OPTION_PREFIX}, {@code extractTableOptions}, {@code validateTableOption},
 * {@code getTableOptionsForCopy}, {@code SupportedTableOptions}) into the connector, which is the
 * only paimon path left. Kept in fe-connector-paimon (not fe-connector-metastore-paimon) for the
 * same reason {@link PaimonCatalogFactory} holds the ported {@code appendCatalogOptions}: every
 * consumer — validate at CREATE CATALOG, exclude from the catalog Options passthrough, apply on
 * table load — lives in this module, and no metastore flavor needs it.
 */
public final class PaimonTableOptions {

    /** The suffix after this prefix is passed to Paimon as a dynamic table option. */
    public static final String TABLE_OPTION_PREFIX = "paimon.table-option.";

    /** BE/JNI-scanner knobs; consumed by {@link PaimonScanPlanProvider}, never a catalog Option. */
    public static final String JNI_PROPERTY_PREFIX = "paimon.jni.";

    /** Canonical and fallback {@link CoreOptions} names which support direct lookup. */
    private static final Map<String, ConfigOption<?>> SUPPORTED_TABLE_OPTIONS = buildSupportedTableOptions();

    private PaimonTableOptions() {
    }

    public static boolean isTableOptionProperty(String key) {
        return key.toLowerCase(Locale.ROOT).startsWith(TABLE_OPTION_PREFIX);
    }

    public static boolean isJniProperty(String key) {
        return key.toLowerCase(Locale.ROOT).startsWith(JNI_PROPERTY_PREFIX);
    }

    /**
     * Strips {@link #TABLE_OPTION_PREFIX} off every table-option property and validates each one
     * against the bundled Paimon version, so a typo or a bad value fails the CREATE/ALTER CATALOG
     * instead of surfacing later as a query error.
     *
     * @throws IllegalArgumentException on an empty, unknown, or unparseable option
     */
    public static Map<String, String> extract(Map<String, String> props) {
        Map<String, String> tableOptions = new LinkedHashMap<>();
        props.forEach((key, value) -> {
            if (isTableOptionProperty(key)) {
                String tableOptionKey = key.substring(TABLE_OPTION_PREFIX.length());
                if (StringUtils.isBlank(tableOptionKey)) {
                    throw new IllegalArgumentException(
                            "Paimon table option name must not be empty after prefix " + TABLE_OPTION_PREFIX);
                }
                validate(tableOptionKey, value);
                tableOptions.put(tableOptionKey, value);
            }
        });
        return Collections.unmodifiableMap(tableOptions);
    }

    /**
     * Narrows {@code tableOptions} to those the Paimon table does not already set itself, i.e. the
     * catalog-level default only fills a gap and never overrides an explicit table property.
     *
     * <p>The comparison goes through the Paimon {@link ConfigOption}, so canonical and fallback keys
     * follow the same precedence rule.
     */
    public static Map<String, String> forCopy(Map<String, String> tableOptions,
            Map<String, String> currentTableOptions) {
        if (tableOptions.isEmpty() || currentTableOptions.isEmpty()) {
            return tableOptions;
        }

        Options existingOptions = new Options(currentTableOptions);
        Map<String, String> optionsForCopy = new LinkedHashMap<>();
        tableOptions.forEach((key, value) -> {
            ConfigOption<?> option = SUPPORTED_TABLE_OPTIONS.get(key);
            if (!existingOptions.contains(option)) {
                optionsForCopy.put(key, value);
            }
        });
        return Collections.unmodifiableMap(optionsForCopy);
    }

    private static void validate(String key, String value) {
        ConfigOption<?> option = SUPPORTED_TABLE_OPTIONS.get(key);
        if (option == null) {
            throw new IllegalArgumentException("Unsupported Paimon table option '" + key
                    + "' for the bundled Paimon version");
        }

        try {
            new Options(Collections.singletonMap(key, value)).get(option);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid value for Paimon table option '" + key + "': "
                    + e.getMessage(), e);
        }
    }

    private static Map<String, ConfigOption<?>> buildSupportedTableOptions() {
        Map<String, ConfigOption<?>> exactOptions = new HashMap<>();
        for (ConfigOption<?> option : CoreOptions.getOptions()) {
            exactOptions.put(option.key(), option);
            for (FallbackKey fallbackKey : option.fallbackKeys()) {
                exactOptions.put(fallbackKey.getKey(), option);
            }
        }
        return Collections.unmodifiableMap(exactOptions);
    }
}
