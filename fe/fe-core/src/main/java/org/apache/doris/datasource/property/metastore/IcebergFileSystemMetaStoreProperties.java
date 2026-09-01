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

import org.apache.doris.common.security.authentication.HadoopExecutionAuthenticator;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.datasource.iceberg.fileio.GcsReadOnlyFileSystem;
import org.apache.doris.datasource.property.storage.GCSProperties;
import org.apache.doris.datasource.property.storage.HdfsProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;

import java.net.URI;
import java.util.List;
import java.util.Map;

public class IcebergFileSystemMetaStoreProperties extends AbstractIcebergProperties {

    @Override
    public String getIcebergCatalogType() {
        return IcebergExternalCatalog.ICEBERG_HADOOP;
    }

    public IcebergFileSystemMetaStoreProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    public Catalog initCatalog(String catalogName, Map<String, String> catalogProps,
                               List<StorageProperties> storagePropertiesList) {
        try {
            Configuration configuration = new Configuration();
            toFileIOProperties(storagePropertiesList, catalogProps, configuration);
            configureGcsWarehouseWithUnderscore(storagePropertiesList, catalogProps, configuration);
            catalogProps.put(CatalogProperties.CATALOG_IMPL, CatalogUtil.ICEBERG_CATALOG_HADOOP);
            buildExecutionAuthenticator(storagePropertiesList);
            return this.executionAuthenticator.execute(() ->
                    buildIcebergCatalog(catalogName, catalogProps, configuration));
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize iceberg filesystem catalog: "
                    + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    static boolean configureGcsWarehouseWithUnderscore(List<StorageProperties> storagePropertiesList,
            Map<String, String> catalogProps, Configuration configuration) {
        boolean hasEffectiveGcsStorage = IcebergUtils.selectEffectiveStorageProperties(storagePropertiesList)
                .stream().anyMatch(GCSProperties.class::isInstance);
        String warehouse = catalogProps.get(CatalogProperties.WAREHOUSE_LOCATION);
        if (!hasEffectiveGcsStorage || StringUtils.isBlank(warehouse)) {
            return false;
        }

        URI warehouseUri;
        try {
            warehouseUri = URI.create(warehouse);
        } catch (IllegalArgumentException e) {
            return false;
        }
        String scheme = warehouseUri.getScheme();
        String bucket = warehouseUri.getAuthority();
        if (!"s3".equalsIgnoreCase(scheme) || !isGcsBucketWithUnderscore(bucket)
                || warehouseUri.getHost() != null) {
            return false;
        }

        configuration.set("fs.s3.impl", GcsReadOnlyFileSystem.class.getName());
        configuration.setBoolean("fs.s3.impl.disable.cache", true);
        configuration.setBoolean("fs.s3a.path.style.access", true);
        catalogProps.put(S3FileIOProperties.PATH_STYLE_ACCESS, "true");
        catalogProps.putIfAbsent(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName());
        return true;
    }

    private static boolean isGcsBucketWithUnderscore(String bucket) {
        return StringUtils.isNotBlank(bucket)
                && bucket.matches("[a-z0-9][a-z0-9._-]*_[a-z0-9._-]*[a-z0-9]");
    }

    private void buildExecutionAuthenticator(List<StorageProperties> storagePropertiesList) {
        if (storagePropertiesList.size() == 1 && storagePropertiesList.get(0) instanceof HdfsProperties) {
            HdfsProperties hdfsProps = (HdfsProperties) storagePropertiesList.get(0);
            if (hdfsProps.isKerberos()) {
                // NOTE: Custom FileIO implementation (KerberizedHadoopFileIO) is commented out by default.
                // Using FileIO for Kerberos authentication may cause serialization issues when accessing
                // Iceberg system tables (e.g., history, snapshots, manifests).
                //props.put(CatalogProperties.FILE_IO_IMPL,"org.apache.doris.datasource.iceberg.fileio.DelegateFileIO");
                this.executionAuthenticator = new HadoopExecutionAuthenticator(hdfsProps.getHadoopAuthenticator());
            }
        }
    }
}
