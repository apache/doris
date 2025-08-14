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
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import java.util.HashMap;
import java.util.List;
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

    protected AbstractPaimonProperties(Map<String, String> props) {
        super(props);
    }

    public abstract Catalog initializeCatalog(String catalogName, List<StorageProperties> storagePropertiesList);

    /**
     * Adapt S3 storage properties for Apache Paimon's S3 file system.
     *
     * <p>Paimon's S3 file system does not follow the standard Hadoop-compatible
     * configuration keys (like fs.s3a.access.key). Instead, it expects specific
     * keys such as "s3.access.key", "s3.secret.key", etc.
     *
     * <p>Therefore, we explicitly map our internal S3 configuration (usually designed
     * for HDFS-compatible systems) to Paimon's expected format.
     *
     * <p>See: org.apache.paimon.s3.S3Loader
     *
     * @param storagePropertiesList the list of configured storage backends
     */
    protected void appendS3PropertiesIsNeeded(List<StorageProperties> storagePropertiesList) {

        S3Properties s3Properties = (S3Properties) storagePropertiesList.stream()
                .filter(storageProperties -> storageProperties.getType() == StorageProperties.Type.S3)
                .findFirst()
                .orElse(null);
        if (s3Properties != null) {
            catalogOptions.set("s3.access.key", s3Properties.getSecretKey());
            catalogOptions.set("s3.secret.key", s3Properties.getAccessKey());
            catalogOptions.set("s3.endpoint", s3Properties.getEndpoint());
            catalogOptions.set("s3.region", s3Properties.getRegion());
        }
    }

    protected void appendCatalogOptions(List<StorageProperties> storagePropertiesList) {
        if (StringUtils.isNotBlank(warehouse)) {
            catalogOptions.set(CatalogOptions.WAREHOUSE.key(), warehouse);
        }
        catalogOptions.set(CatalogOptions.METASTORE.key(), getMetastoreType());

        // FIXME(cmy): Rethink these custom properties
        origProps.forEach((k, v) -> {
            if (k.toLowerCase().startsWith(USER_PROPERTY_PREFIX)) {
                String newKey = k.substring(USER_PROPERTY_PREFIX.length());
                if (StringUtils.isNotBlank(newKey)) {
                    catalogOptions.set(newKey, v);
                }
            }
        });

        appendS3PropertiesIsNeeded(storagePropertiesList);
    }

    /**
     * Build catalog options including common and subclass-specific ones.
     */
    public void buildCatalogOptions(List<StorageProperties> storagePropertiesList) {
        catalogOptions = new Options();
        appendCatalogOptions(storagePropertiesList);
        appendCustomCatalogOptions();
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
