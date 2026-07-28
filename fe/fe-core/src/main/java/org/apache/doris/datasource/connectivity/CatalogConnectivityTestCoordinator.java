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

package org.apache.doris.datasource.connectivity;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.property.metastore.HiveGlueMetaStoreProperties;
import org.apache.doris.datasource.property.metastore.HiveHMSProperties;
import org.apache.doris.datasource.property.metastore.IcebergGlueMetaStoreProperties;
import org.apache.doris.datasource.property.metastore.IcebergHMSMetaStoreProperties;
import org.apache.doris.datasource.property.metastore.IcebergRestProperties;
import org.apache.doris.datasource.property.metastore.IcebergS3TablesMetaStoreProperties;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.datasource.storage.StorageTypeId;
import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * Coordinator for catalog connectivity testing.
 * This class orchestrates the testing of metadata services and storage systems
 * when creating external catalogs with test_connection=true.
 */
public class CatalogConnectivityTestCoordinator {
    private static final Logger LOG = LogManager.getLogger(CatalogConnectivityTestCoordinator.class);

    private final String catalogName;
    private final MetastoreProperties metastoreProperties;
    private final Map<StorageTypeId, StorageAdapter> storageAdaptersMap;

    private String warehouseLocation;

    public CatalogConnectivityTestCoordinator(
            String catalogName,
            MetastoreProperties metastoreProperties,
            Map<StorageTypeId, StorageAdapter> storageAdaptersMap) {
        this.catalogName = catalogName;
        this.metastoreProperties = metastoreProperties;
        this.storageAdaptersMap = storageAdaptersMap;
    }

    /**
     * Run all connectivity tests for the catalog.
     *
     * @throws DdlException if any test fails
     */
    public void runTests() throws DdlException {
        // 1. Test metadata service
        testMetadataService();

        // 2. Test object storage for warehouse (if applicable)
        StorageAdapter testObjectStorageAdapter = getTestObjectStorageAdapter();
        if (testObjectStorageAdapter != null) {
            testObjectStorageForWarehouse(testObjectStorageAdapter);
        }

        // 3. Test explicitly configured HDFS (if applicable)
        if (shouldTestHdfs()) {
            testExplicitlyConfiguredHdfs();
        }
    }

    /**
     * Test metadata service connectivity (HMS, Glue, REST).
     * Also stores the warehouse location to class variable for later use.
     *
     * @throws DdlException if test fails
     */
    private void testMetadataService() throws DdlException {
        MetaConnectivityTester metaTester = createMetaTester(metastoreProperties);

        LOG.info("Testing {} connectivity for catalog '{}'", metaTester.getTestType(), catalogName);

        try {
            metaTester.testConnection();
        } catch (Exception e) {
            String hint = metaTester.getErrorHint();
            String errorMsg = metaTester.getTestType() + " connectivity test failed: " + hint
                    + " Root cause: " + Util.getRootCauseMessage(e);
            throw new DdlException(errorMsg);
        }

        // Store warehouse location for later use
        this.warehouseLocation = metaTester.getTestLocation();
        if (StringUtils.isNotBlank(this.warehouseLocation)) {
            LOG.debug("Got warehouse location from metadata service: {}", this.warehouseLocation);
        }
    }

    /**
     * Check if object storage test should be performed.
     * Also caches the matched storage for later use in testObjectStorageForWarehouse().
     */
    private StorageAdapter getTestObjectStorageAdapter() {
        if (StringUtils.isBlank(this.warehouseLocation)) {
            LOG.debug("Skip object storage test: no warehouse location from metadata service for catalog '{}'",
                    catalogName);
            return null;
        }

        StorageAdapter matchedObjectStorage = findMatchingObjectStorage(this.warehouseLocation);
        if (matchedObjectStorage == null) {
            LOG.debug("Skip object storage test: no storage configured for warehouse '{}' in catalog '{}'",
                    this.warehouseLocation, catalogName);
            return null;
        }

        return matchedObjectStorage;
    }

    /**
     * Test object storage that matches the warehouse location from metadata service.
     * Uses the cached matchedObjectStorage from shouldTestObjectStorage().
     *
     * @throws DdlException if test fails
     */
    private void testObjectStorageForWarehouse(StorageAdapter testObjectStorageAdapter) throws DdlException {
        LOG.info("Testing {} connectivity for warehouse '{}' in catalog '{}'",
                testObjectStorageAdapter.getStorageName(), this.warehouseLocation, catalogName);

        StorageConnectivityTester tester = createStorageTester(testObjectStorageAdapter, this.warehouseLocation);

        // Test FE connection
        try {
            tester.testFeConnection();
        } catch (Exception e) {
            String hint = tester.getErrorHint();
            String errorMsg = tester.getTestType() + " connectivity test failed: " + hint
                    + " Root cause: " + Util.getRootCauseMessage(e);
            throw new DdlException(errorMsg);
        }

        // Test BE connection
        try {
            tester.testBeConnection();
        } catch (Exception e) {
            String hint = tester.getErrorHint();
            String errorMsg = tester.getTestType() + " connectivity test failed (compute node): " + hint
                    + " Root cause: " + Util.getRootCauseMessage(e);
            throw new DdlException(errorMsg);
        }
    }

    /**
     * Find object storage that can handle the given warehouse location.
     *
     * @param warehouse warehouse location
     * @return matching storage properties, or null if not found
     */
    private StorageAdapter findMatchingObjectStorage(String warehouse) {
        // Check S3/Minio
        if (warehouse.startsWith("s3://") || warehouse.startsWith("s3a://")) {
            // Priority: Minio > S3 (if Minio is configured, use it for s3://)
            StorageAdapter minio = storageAdaptersMap.get(StorageTypeId.MINIO);
            if (minio != null && isConfiguredStorage(minio)) {
                return minio;
            }

            StorageAdapter s3 = storageAdaptersMap.get(StorageTypeId.S3);
            if (s3 != null && isConfiguredStorage(s3)) {
                return s3;
            }
        }
        return null;
    }

    /**
     * Check if storage has credentials configured.
     * Check for access key, IAM role, or other authentication methods.
     */
    private boolean isConfiguredStorage(StorageAdapter storage) {
        if (!(storage.getSpiProperties() instanceof S3CompatibleFileSystemProperties)) {
            // For other storage types, assume configured if exists
            return true;
        }
        S3CompatibleFileSystemProperties s3 = (S3CompatibleFileSystemProperties) storage.getSpiProperties();

        // For S3: check access key or IAM role (legacy getS3IAMRole == SPI getRoleArn)
        if (storage.getType() == StorageTypeId.S3) {
            return StringUtils.isNotBlank(s3.getAccessKey())
                    || StringUtils.isNotBlank(s3.getRoleArn());
        }

        // For Minio: check access key
        if (storage.getType() == StorageTypeId.MINIO) {
            return StringUtils.isNotBlank(s3.getAccessKey());
        }

        // For other storage types, assume configured if exists
        return true;
    }

    /**
     * Check if HDFS test should be performed.
     */
    private boolean shouldTestHdfs() {
        StorageAdapter hdfs = storageAdaptersMap.get(StorageTypeId.HDFS);
        if (hdfs == null) {
            return false;
        }

        if (!hdfs.isExplicitlyConfigured()) {
            LOG.debug("Skip HDFS test: not explicitly configured by user for catalog '{}'", catalogName);
            return false;
        }

        if (StringUtils.isBlank(getHdfsDefaultFs(hdfs))) {
            LOG.debug("Skip HDFS test: fs.defaultFS not configured for catalog '{}'", catalogName);
            return false;
        }

        return true;
    }

    /**
     * Legacy {@code HdfsProperties.getDefaultFS()}: the resolved fs.defaultFS value, which the
     * HDFS backend map carries under the same key whenever it is non-blank.
     */
    private static String getHdfsDefaultFs(StorageAdapter hdfs) {
        return hdfs.getBackendConfigProperties().get("fs.defaultFS");
    }

    /**
     * Test explicitly configured HDFS.
     *
     * @throws DdlException if test fails
     */
    private void testExplicitlyConfiguredHdfs() throws DdlException {
        StorageAdapter hdfs = storageAdaptersMap.get(StorageTypeId.HDFS);
        String defaultFS = getHdfsDefaultFs(hdfs);

        LOG.info("Testing HDFS connectivity for '{}' in catalog '{}'", defaultFS, catalogName);

        StorageConnectivityTester tester = createStorageTester(hdfs, defaultFS);

        // Test FE connection
        try {
            tester.testFeConnection();
        } catch (Exception e) {
            String hint = tester.getErrorHint();
            String errorMsg = "HDFS connectivity test failed: " + hint
                    + " Root cause: " + Util.getRootCauseMessage(e);
            throw new DdlException(errorMsg);
        }

        // Test BE connection
        try {
            tester.testBeConnection();
        } catch (Exception e) {
            String hint = tester.getErrorHint();
            String errorMsg = "HDFS connectivity test failed (compute node): " + hint
                    + " Root cause: " + Util.getRootCauseMessage(e);
            throw new DdlException(errorMsg);
        }
    }

    /**
     * Create metadata connectivity tester based on properties type.
     */
    private MetaConnectivityTester createMetaTester(MetastoreProperties props) {
        // Hive HMS
        if (props instanceof HiveHMSProperties) {
            HiveHMSProperties hiveProps = (HiveHMSProperties) props;
            return new HiveHMSConnectivityTester(hiveProps, hiveProps.getHmsBaseProperties());
        }

        // Hive Glue
        if (props instanceof HiveGlueMetaStoreProperties) {
            HiveGlueMetaStoreProperties glueProps = (HiveGlueMetaStoreProperties) props;
            return new HiveGlueMetaStoreConnectivityTester(glueProps, glueProps.getBaseProperties());
        }

        // Iceberg HMS
        if (props instanceof IcebergHMSMetaStoreProperties) {
            IcebergHMSMetaStoreProperties icebergHms = (IcebergHMSMetaStoreProperties) props;
            return new IcebergHMSConnectivityTester(icebergHms, icebergHms.getHmsBaseProperties());
        }

        // Iceberg Glue
        if (props instanceof IcebergGlueMetaStoreProperties) {
            IcebergGlueMetaStoreProperties icebergGlue = (IcebergGlueMetaStoreProperties) props;
            return new IcebergGlueMetaStoreConnectivityTester(icebergGlue, icebergGlue.getGlueProperties());
        }

        // Iceberg REST
        if (props instanceof IcebergRestProperties) {
            return new IcebergRestConnectivityTester((IcebergRestProperties) props);
        }

        // Iceberg S3Table
        if (props instanceof IcebergS3TablesMetaStoreProperties) {
            return new IcebergS3TablesMetaStoreConnectivityTester((IcebergS3TablesMetaStoreProperties) props);
        }

        // Default: no-op tester
        return new MetaConnectivityTester() {
        };
    }

    /**
     * Create storage connectivity tester based on properties type and location.
     */
    private StorageConnectivityTester createStorageTester(StorageAdapter adapter, String location) {
        // S3
        if (adapter.getType() == StorageTypeId.S3) {
            return new S3ConnectivityTester(adapter, location);
        }

        // Minio
        if (adapter.getType() == StorageTypeId.MINIO) {
            return new MinioConnectivityTester(adapter, location);
        }

        // HDFS
        if (adapter.getType() == StorageTypeId.HDFS) {
            return new HdfsConnectivityTester(adapter);
        }

        // Default: no-op tester
        return new StorageConnectivityTester() {
        };
    }
}
