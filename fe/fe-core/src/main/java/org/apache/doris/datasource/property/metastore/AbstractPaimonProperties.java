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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.foundation.property.ConnectorProperty;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.FallbackKey;
import org.apache.paimon.options.Options;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractPaimonProperties extends MetastoreProperties {
    @ConnectorProperty(
            names = {"warehouse"},
            description = "The location of the Paimon warehouse. This is where the tables will be stored."
    )
    protected String warehouse;

    @Getter
    protected ExecutionAuthenticator executionAuthenticator = new ExecutionAuthenticator() {
    };

    @Getter
    protected Options catalogOptions;

    private final AtomicReference<Map<String, String>> catalogOptionsMapRef = new AtomicReference<>();

    public abstract String getPaimonCatalogType();

    private static final String USER_PROPERTY_PREFIX = "paimon.";
    private static final String DORIS_JNI_PROPERTY_PREFIX = "paimon.jni.";
    /** The suffix after this prefix is passed to Paimon as a dynamic table option. */
    public static final String TABLE_OPTION_PREFIX = "paimon.table-option.";
    private static final SupportedTableOptions SUPPORTED_TABLE_OPTIONS = SupportedTableOptions.build();

    private Map<String, String> tableOptionsMap = Collections.emptyMap();

    protected AbstractPaimonProperties(Map<String, String> props) {
        super(Type.PAIMON, props);
    }

    public abstract Catalog initializeCatalog(String catalogName, List<StorageProperties> storagePropertiesList);

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        tableOptionsMap = extractTableOptions();
    }

    protected void appendCatalogOptions() {
        if (StringUtils.isNotBlank(warehouse)) {
            catalogOptions.set(CatalogOptions.WAREHOUSE.key(), warehouse);
        }
        catalogOptions.set(CatalogOptions.METASTORE.key(), getMetastoreType());

        // FIXME(cmy): Rethink these custom properties
        origProps.forEach((k, v) -> {
            if (k.toLowerCase(Locale.ROOT).startsWith(USER_PROPERTY_PREFIX)) {
                String newKey = k.substring(USER_PROPERTY_PREFIX.length());
                if (StringUtils.isNotBlank(newKey)) {
                    boolean excluded = isTableOptionProperty(k)
                            || k.toLowerCase(Locale.ROOT).startsWith(DORIS_JNI_PROPERTY_PREFIX)
                            || userStoragePrefixes.stream().anyMatch(k::startsWith);
                    if (!excluded) {
                        catalogOptions.set(newKey, v);
                    }
                }
            }
        });
    }

    /**
     * Build catalog options including common and subclass-specific ones.
     */
    public void buildCatalogOptions() {
        catalogOptions = new Options();
        appendCatalogOptions();
        appendCustomCatalogOptions();
    }

    protected void appendUserHadoopConfig(Configuration  conf) {
        normalizeS3Config().forEach(conf::set);
    }

    public Map<String, String> getCatalogOptionsMap() {
        // Return the cached map if already initialized
        Map<String, String> existing = catalogOptionsMapRef.get();
        if (existing != null) {
            return existing;
        }

        // Check that the catalog options source is available
        if (catalogOptions == null) {
            throw new IllegalStateException("Catalog options have not been initialized. Call"
                    + " buildCatalogOptions first.");
        }

        // Construct the map manually using the provided keys
        Map<String, String> computed = new HashMap<>();
        for (String key : catalogOptions.keySet()) {
            computed.put(key, catalogOptions.get(key));
        }

        // Attempt to set the constructed map atomically; only one thread wins
        if (catalogOptionsMapRef.compareAndSet(null, computed)) {
            return computed;
        } else {
            // Another thread already initialized it; return the existing one
            return catalogOptionsMapRef.get();
        }
    }

    public Map<String, String> getTableOptionsMap() {
        return tableOptionsMap;
    }

    /**
     * Returns Catalog table options which are not explicitly configured by the Paimon table.
     *
     * <p>The comparison is based on Paimon {@link ConfigOption}s so canonical and fallback keys
     * follow the same precedence rule.
     */
    public Map<String, String> getTableOptionsForCopy(Map<String, String> currentTableOptions) {
        if (tableOptionsMap.isEmpty() || currentTableOptions.isEmpty()) {
            return tableOptionsMap;
        }

        Options existingOptions = new Options(currentTableOptions);
        Map<String, String> optionsForCopy = new LinkedHashMap<>();
        tableOptionsMap.forEach((key, value) -> {
            ConfigOption<?> option = SUPPORTED_TABLE_OPTIONS.find(key);
            if (!existingOptions.contains(option)) {
                optionsForCopy.put(key, value);
            }
        });
        return Collections.unmodifiableMap(optionsForCopy);
    }

    public static boolean isTableOptionProperty(String key) {
        return key.toLowerCase(Locale.ROOT).startsWith(TABLE_OPTION_PREFIX);
    }

    private Map<String, String> extractTableOptions() {
        Map<String, String> tableOptions = new LinkedHashMap<>();
        origProps.forEach((key, value) -> {
            if (isTableOptionProperty(key)) {
                String tableOptionKey = key.substring(TABLE_OPTION_PREFIX.length());
                if (StringUtils.isBlank(tableOptionKey)) {
                    throw new IllegalArgumentException(
                            "Paimon table option name must not be empty after prefix " + TABLE_OPTION_PREFIX);
                }
                validateTableOption(tableOptionKey, value);
                tableOptions.put(tableOptionKey, value);
            }
        });
        return Collections.unmodifiableMap(tableOptions);
    }

    private void validateTableOption(String key, String value) {
        ConfigOption<?> option = SUPPORTED_TABLE_OPTIONS.find(key);
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

    private static final class SupportedTableOptions {
        /** Canonical and fallback option names which support direct lookup. */
        private final Map<String, ConfigOption<?>> exactOptions;

        private SupportedTableOptions(Map<String, ConfigOption<?>> exactOptions) {
            this.exactOptions = exactOptions;
        }

        private static SupportedTableOptions build() {
            Map<String, ConfigOption<?>> exactOptions = new HashMap<>();
            for (ConfigOption<?> option : CoreOptions.getOptions()) {
                exactOptions.put(option.key(), option);
                for (FallbackKey fallbackKey : option.fallbackKeys()) {
                    exactOptions.put(fallbackKey.getKey(), option);
                }
            }
            return new SupportedTableOptions(Collections.unmodifiableMap(exactOptions));
        }

        private ConfigOption<?> find(String key) {
            return exactOptions.get(key);
        }
    }

    /**
     * @See org.apache.paimon.s3.S3FileIO
     * Possible S3 config key prefixes:
     * 1. "s3."      - Paimon legacy custom prefix
     * 2. "s3a."     - Paimon-supported shorthand
     * 3. "fs.s3a."  - Hadoop S3A official prefix
     *
     * All of them are normalized to the Hadoop-recognized prefix "fs.s3a."
     */
    private final List<String> userStoragePrefixes = ImmutableList.of(
                    "paimon.s3.", "paimon.s3a.", "paimon.fs.s3.", "paimon.fs.oss."
    );

    /** Hadoop S3A standard prefix */
    private static final String FS_S3A_PREFIX = "fs.s3a.";

    /**
     * Normalizes user-provided S3 config keys to Hadoop S3A keys
     */
    protected Map<String, String> normalizeS3Config() {
        Map<String, String> result = new HashMap<>();
        origProps.forEach((key, value) -> {
            for (String prefix : userStoragePrefixes) {
                if (key.startsWith(prefix)) {
                    result.put(FS_S3A_PREFIX + key.substring(prefix.length()), value);
                    return; // stop after the first matching prefix
                }
            }
        });
        return result;
    }


    /**
     * Hook method for subclasses to append metastore-specific or custom catalog options.
     *
     * <p>This method is invoked after common catalog options (e.g., warehouse path,
     * metastore type, user-defined keys, and S3 compatibility mappings) have been
     * added to the {@link org.apache.paimon.options.Options} instance.
     *
     * <p>Subclasses should override this method to inject additional configuration
     * required for their specific metastore or environment. For example:
     *
     * <ul>
     *   <li>DLF-based catalog may require a custom metastore client class.</li>
     *   <li>HMS-based catalog may include URI and client pool parameters.</li>
     *   <li>Other environments may inject authentication, endpoint, or caching options.</li>
     * </ul>
     *
     * <p>If the subclass does not require any special options beyond the common ones,
     * it can safely leave this method empty.
     */
    protected abstract void appendCustomCatalogOptions();

    /**
     * Returns the metastore type identifier used by the Paimon catalog factory.
     *
     * <p>This identifier must match one of the known metastore types supported by
     * Apache Paimon. Internally, the value returned here is used to configure the
     * `metastore` option in {@code Options}, which determines the specific
     * {@link org.apache.paimon.catalog.CatalogFactory} implementation to be used
     * when instantiating the catalog.
     *
     * <p>You can find valid identifiers by reviewing implementations of the
     * {@link org.apache.paimon.catalog.CatalogFactory} interface. Each implementation
     * declares its identifier via a static {@code IDENTIFIER} field or equivalent constant.
     *
     * <p>Examples:
     * <ul>
     *   <li>{@code "filesystem"} - for {@link org.apache.paimon.catalog.FileSystemCatalogFactory}</li>
     *   <li>{@code "hive"} - for {@link org.apache.paimon.hive.HiveCatalogFactory}</li>
     * </ul>
     *
     * @return the metastore type identifier string
     */
    protected abstract String getMetastoreType();
}
