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

import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.datasource.property.storage.AbstractS3CompatibleProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergJdbcMetaStoreProperties extends AbstractIcebergProperties {

    private static final String JDBC_PREFIX = "jdbc.";

    private Map<String, String> icebergJdbcCatalogProperties;

    @ConnectorProperty(
            names = {"uri", "iceberg.jdbc.uri"},
            required = true,
            description = "JDBC connection URI for the Iceberg JDBC catalog."
    )
    private String uri = "";

    @ConnectorProperty(
            names = {"jdbc.user"},
            required = false,
            description = "Username for the Iceberg JDBC catalog."
    )
    private String jdbcUser;

    @ConnectorProperty(
            names = {"jdbc.password"},
            required = false,
            sensitive = true,
            description = "Password for the Iceberg JDBC catalog."
    )
    private String jdbcPassword;

    @ConnectorProperty(
            names = {"jdbc.init-catalog-tables"},
            required = false,
            description = "Whether to create catalog tables if they do not exist."
    )
    private String jdbcInitCatalogTables;

    @ConnectorProperty(
            names = {"jdbc.schema-version"},
            required = false,
            description = "Iceberg JDBC catalog schema version (V0/V1)."
    )
    private String jdbcSchemaVersion;

    @ConnectorProperty(
            names = {"jdbc.strict-mode"},
            required = false,
            description = "Whether to enforce strict JDBC catalog schema checks."
    )
    private String jdbcStrictMode;

    public IcebergJdbcMetaStoreProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    public String getIcebergCatalogType() {
        return IcebergExternalCatalog.ICEBERG_JDBC;
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        initIcebergJdbcCatalogProperties();
    }

    @Override
    protected void checkRequiredProperties() {
        super.checkRequiredProperties();
        if (StringUtils.isBlank(warehouse)) {
            throw new IllegalArgumentException("Property warehouse is required.");
        }
    }

    @Override
    public Catalog initCatalog(String catalogName, Map<String, String> catalogProps,
            List<StorageProperties> storagePropertiesList) {
        Map<String, String> fileIOProperties = Maps.newHashMap();
        Configuration conf = new Configuration();
        toFileIOProperties(storagePropertiesList, fileIOProperties, conf);

        Map<String, String> options = Maps.newHashMap(getIcebergJdbcCatalogProperties());
        options.putAll(fileIOProperties);
        return CatalogUtil.buildIcebergCatalog(catalogName, options, conf);
    }

    public Map<String, String> getIcebergJdbcCatalogProperties() {
        return Collections.unmodifiableMap(icebergJdbcCatalogProperties);
    }

    private void initIcebergJdbcCatalogProperties() {
        icebergJdbcCatalogProperties = new HashMap<>();
        icebergJdbcCatalogProperties.put(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_JDBC);
        icebergJdbcCatalogProperties.put(CatalogProperties.URI, uri);
        if (StringUtils.isNotBlank(warehouse)) {
            icebergJdbcCatalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        }
        addIfNotBlank(icebergJdbcCatalogProperties, "jdbc.user", jdbcUser);
        addIfNotBlank(icebergJdbcCatalogProperties, "jdbc.password", jdbcPassword);
        addIfNotBlank(icebergJdbcCatalogProperties, "jdbc.init-catalog-tables", jdbcInitCatalogTables);
        addIfNotBlank(icebergJdbcCatalogProperties, "jdbc.schema-version", jdbcSchemaVersion);
        addIfNotBlank(icebergJdbcCatalogProperties, "jdbc.strict-mode", jdbcStrictMode);

        if (origProps != null) {
            for (Map.Entry<String, String> entry : origProps.entrySet()) {
                String key = entry.getKey();
                if (key != null && key.startsWith(JDBC_PREFIX)
                        && !icebergJdbcCatalogProperties.containsKey(key)) {
                    icebergJdbcCatalogProperties.put(key, entry.getValue());
                }
            }
        }
    }

    private static void addIfNotBlank(Map<String, String> props, String key, String value) {
        if (StringUtils.isNotBlank(value)) {
            props.put(key, value);
        }
    }

    private static void toFileIOProperties(List<StorageProperties> storagePropertiesList,
            Map<String, String> fileIOProperties, Configuration conf) {
        for (StorageProperties storageProperties : storagePropertiesList) {
            if (storageProperties instanceof AbstractS3CompatibleProperties) {
                toS3FileIOProperties((AbstractS3CompatibleProperties) storageProperties, fileIOProperties);
            } else if (storageProperties.getHadoopStorageConfig() != null) {
                conf.addResource(storageProperties.getHadoopStorageConfig());
            }
        }
    }

    private static void toS3FileIOProperties(AbstractS3CompatibleProperties s3Properties,
            Map<String, String> options) {
        // Set S3FileIO as the FileIO implementation for S3-compatible storage
        options.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");

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
}
