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
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.metacache.CacheSpec;
import org.apache.doris.datasource.property.storage.AbstractS3CompatibleProperties;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.foundation.property.ConnectorProperty;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @See org.apache.iceberg.CatalogProperties
 */
public abstract class AbstractIcebergProperties extends MetastoreProperties {

    @Getter
    @ConnectorProperty(
            names = {CatalogProperties.WAREHOUSE_LOCATION},
            required = false,
            description = "The location of the Iceberg warehouse. This is where the tables will be stored."
    )
    protected String warehouse;

    @Getter
    @ConnectorProperty(
            names = {CatalogProperties.IO_MANIFEST_CACHE_ENABLED},
            required = false,
            description = "Controls whether to use caching during manifest reads or not. Default: false."
    )
    protected String ioManifestCacheEnabled;

    @Getter
    @ConnectorProperty(
            names = {CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS},
            required = false,
            description = "Controls the maximum duration for which an entry stays in the manifest cache. "
                    + "Must be a non-negative value. Zero means entries expire only due to memory pressure. "
                    + "Default: 60000 (60s)."
    )
    protected String ioManifestCacheExpirationIntervalMs;

    @Getter
    @ConnectorProperty(
            names = {CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES},
            required = false,
            description = "Controls the maximum total amount of bytes to cache in manifest cache. "
                    + "Must be a positive value. Default: 104857600 (100MB)."
    )
    protected String ioManifestCacheMaxTotalBytes;

    @Getter
    @ConnectorProperty(
            names = {CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH},
            required = false,
            description = "Controls the maximum length of file to be considered for caching. "
                    + "An InputFile will not be cached if the length is longer than this limit. "
                    + "Must be a positive value. Default: 8388608 (8MB)."
    )
    protected String ioManifestCacheMaxContentLength;

    @Getter
    @ConnectorProperty(
            names = {CatalogProperties.FILE_IO_IMPL},
            required = false,
            description = "Custom io impl for iceberg"
    )
    protected String ioImpl;

    @Getter
    protected ExecutionAuthenticator executionAuthenticator = new ExecutionAuthenticator(){};

    public abstract String getIcebergCatalogType();

    protected AbstractIcebergProperties(Map<String, String> props) {
        super(Type.ICEBERG, props);
    }

    /**
     * Iceberg Catalog instance responsible for managing metadata and lifecycle of Iceberg tables.
     * <p>
     * The Catalog is a core component in Iceberg that handles table registration,
     * loading, and metadata management.
     * <p>
     * It is assigned during initialization via the `initialize` method,
     * which calls the abstract `initCatalog` method to create a concrete Catalog instance.
     * This instance is typically configured based on the provided catalog name
     * and a list of storage properties.
     * <p>
     * After initialization, the catalog must not be null; otherwise,
     * an IllegalStateException is thrown to ensure that subsequent operations
     * on Iceberg tables have a valid Catalog reference.
     * <p>
     * Different Iceberg Catalog implementations (such as HadoopCatalog, HiveCatalog,
     * RESTCatalog, etc.) can be flexibly switched and configured
     * by subclasses overriding the `initCatalog` method.
     * <p>
     * This field is used to perform metadata operations like creating, querying,
     * and deleting Iceberg tables.
     */
    public final Catalog initializeCatalog(String catalogName,
                                           List<StorageProperties> storagePropertiesList) {
        Map<String, String> catalogProps = new HashMap<>(getOrigProps());
        if (StringUtils.isNotBlank(warehouse)) {
            catalogProps.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        }

        // Add manifest cache properties if configured
        addManifestCacheProperties(catalogProps);

        Catalog catalog = initCatalog(catalogName, catalogProps, storagePropertiesList);

        if (catalog == null) {
            throw new IllegalStateException("Catalog must not be null after initialization.");
        }
        return catalog;
    }

    /**
     * Add manifest cache related properties to catalog properties.
     * These properties control caching behavior during manifest reads.
     *
     * @param catalogProps the catalog properties map to add manifest cache properties to
     */
    protected void addManifestCacheProperties(Map<String, String> catalogProps) {
        boolean hasIoManifestCacheEnabled = StringUtils.isNotBlank(ioManifestCacheEnabled)
                || StringUtils.isNotBlank(catalogProps.get(CatalogProperties.IO_MANIFEST_CACHE_ENABLED));
        if (StringUtils.isNotBlank(ioManifestCacheEnabled)) {
            catalogProps.put(CatalogProperties.IO_MANIFEST_CACHE_ENABLED, ioManifestCacheEnabled);
        }
        if (StringUtils.isNotBlank(ioManifestCacheExpirationIntervalMs)) {
            catalogProps.put(CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS,
                    ioManifestCacheExpirationIntervalMs);
        }
        if (StringUtils.isNotBlank(ioManifestCacheMaxTotalBytes)) {
            catalogProps.put(CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES, ioManifestCacheMaxTotalBytes);
        }
        if (StringUtils.isNotBlank(ioManifestCacheMaxContentLength)) {
            catalogProps.put(CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH, ioManifestCacheMaxContentLength);
        }

        // default enable io manifest cache if the meta.cache.manifest is enabled
        if (!hasIoManifestCacheEnabled) {
            CacheSpec manifestCacheSpec = CacheSpec.fromProperties(catalogProps, CacheSpec.propertySpecBuilder()
                    .enable(IcebergExternalCatalog.ICEBERG_MANIFEST_CACHE_ENABLE,
                            IcebergExternalCatalog.DEFAULT_ICEBERG_MANIFEST_CACHE_ENABLE)
                    .ttl(IcebergExternalCatalog.ICEBERG_MANIFEST_CACHE_TTL_SECOND,
                            IcebergExternalCatalog.DEFAULT_ICEBERG_MANIFEST_CACHE_TTL_SECOND)
                    .capacity(IcebergExternalCatalog.ICEBERG_MANIFEST_CACHE_CAPACITY,
                            IcebergExternalCatalog.DEFAULT_ICEBERG_MANIFEST_CACHE_CAPACITY)
                    .build());
            if (CacheSpec.isCacheEnabled(manifestCacheSpec.isEnable(),
                    manifestCacheSpec.getTtlSecond(),
                    manifestCacheSpec.getCapacity())) {
                catalogProps.put(CatalogProperties.IO_MANIFEST_CACHE_ENABLED, "true");
            }
        }
    }

    /**
     * Subclasses must implement this to create the concrete Catalog instance.
     */
    protected abstract Catalog initCatalog(
            String catalogName,
            Map<String, String> catalogProps,
            List<StorageProperties> storagePropertiesList
    );

    /**
     * Unified method to configure FileIO properties for Iceberg catalog.
     * This method handles all storage types (HDFS, S3, MinIO, etc.) by:
     * 1. Adding all storage properties to Hadoop Configuration (for HadoopFileIO / S3A access).
     * 2. Extracting S3-compatible properties into fileIOProperties map (for Iceberg S3FileIO).
     *
     * @param storagePropertiesList list of storage properties
     * @param fileIOProperties options map to be populated with S3 FileIO properties
     * @return Hadoop Configuration populated with all storage properties
     */
    public void toFileIOProperties(List<StorageProperties> storagePropertiesList,
            Map<String, String> fileIOProperties, Configuration conf) {
        // We only support one S3-compatible storage property for FileIO configuration.
        // When multiple AbstractS3CompatibleProperties exist, prefer the first non-S3Properties one,
        // because a non-S3 type (e.g. OSSProperties, COSProperties) indicates the user has explicitly
        // specified a concrete S3-compatible storage, which should take priority over the generic S3Properties.
        AbstractS3CompatibleProperties s3Fallback = null;
        AbstractS3CompatibleProperties s3Target = null;
        for (StorageProperties storageProperties : storagePropertiesList) {
            if (conf != null && storageProperties.getHadoopStorageConfig() != null) {
                conf.addResource(storageProperties.getHadoopStorageConfig());
            }
            if (storageProperties instanceof AbstractS3CompatibleProperties) {
                if (s3Fallback == null) {
                    s3Fallback = (AbstractS3CompatibleProperties) storageProperties;
                }
                if (s3Target == null && !(storageProperties instanceof S3Properties)) {
                    s3Target = (AbstractS3CompatibleProperties) storageProperties;
                }
            }
        }
        AbstractS3CompatibleProperties chosen = s3Target != null ? s3Target : s3Fallback;
        if (chosen != null) {
            toS3FileIOProperties(chosen, fileIOProperties);
        } else {
            String region = AbstractS3CompatibleProperties.getRegionFromProperties(fileIOProperties);
            if (!Strings.isNullOrEmpty(region)) {
                fileIOProperties.put(AwsClientProperties.CLIENT_REGION, region);
            }
        }
    }

    /**
     * Configure S3 FileIO properties for all S3-compatible storage types (S3, MinIO, etc.)
     * This method provides a unified way to convert S3-compatible properties to Iceberg S3FileIO format.
     *
     * @param s3Properties S3-compatible properties
     * @param options Options map to be populated with S3 FileIO properties
     */
    private void toS3FileIOProperties(AbstractS3CompatibleProperties s3Properties, Map<String, String> options) {
        // Common properties - only set if not blank
        if (StringUtils.isNotBlank(s3Properties.getEndpoint())) {
            options.put(S3FileIOProperties.ENDPOINT, s3Properties.getEndpoint());
        }
        if (StringUtils.isNotBlank(s3Properties.getUsePathStyle())) {
            options.put(S3FileIOProperties.PATH_STYLE_ACCESS, s3Properties.getUsePathStyle());
        }
        if (StringUtils.isNotBlank(s3Properties.getRegion())) {
            options.put(AwsClientProperties.CLIENT_REGION, s3Properties.getRegion());
        }
        if (StringUtils.isNotBlank(s3Properties.getAccessKey())) {
            options.put(S3FileIOProperties.ACCESS_KEY_ID, s3Properties.getAccessKey());
        }
        if (StringUtils.isNotBlank(s3Properties.getSecretKey())) {
            options.put(S3FileIOProperties.SECRET_ACCESS_KEY, s3Properties.getSecretKey());
        }
        if (StringUtils.isNotBlank(s3Properties.getSessionToken())) {
            options.put(S3FileIOProperties.SESSION_TOKEN, s3Properties.getSessionToken());
        }
    }

    protected Catalog buildIcebergCatalog(String catalogName, Map<String, String> options, Configuration conf) {
        // For Iceberg SDK, "type" means catalog type, such as hive, jdbc, rest.
        // But in Doris, "type" is "iceberg".
        // And Iceberg SDK does not allow with both "type" and "catalog-impl" properties,
        // So here we remove "type" and make sure "catalog-impl" is set.
        options.remove(CatalogUtil.ICEBERG_CATALOG_TYPE);
        Preconditions.checkArgument(options.containsKey(CatalogProperties.CATALOG_IMPL));
        return CatalogUtil.buildIcebergCatalog(catalogName, options, conf);
    }
}
