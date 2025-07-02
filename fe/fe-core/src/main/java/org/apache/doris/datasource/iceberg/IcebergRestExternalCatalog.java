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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.metastore.IcebergRestProperties;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;

import java.util.HashMap;
import java.util.Map;

public class IcebergRestExternalCatalog extends IcebergExternalCatalog {

    public IcebergRestExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
                                      String comment) {
        super(catalogId, name, comment);
        props = PropertyConverter.convertToMetaProperties(props);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    protected void initCatalog() {
        icebergCatalogType = ICEBERG_REST;
        // 1. Get properties for REST catalog service.
        IcebergRestProperties icebergRestProperties = (IcebergRestProperties) catalogProperty.getMetastoreProperties();
        Map<String, String> options = new HashMap<>(icebergRestProperties.getIcebergRestCatalogProperties());

        // 2. Get properties for FileIO.
        Configuration conf = new Configuration();
        for (StorageProperties storageProperties : catalogProperty.getStoragePropertiesMap().values()) {
            if (storageProperties instanceof S3Properties) {
                // For S3, properties should be in the options map for Iceberg S3FileIO.
                ((S3Properties) storageProperties).toIcebergS3FileIOProperties(options);
            } else {
                // For other file systems (like HDFS), properties should be in the hadoop conf.
                storageProperties.getBackendConfigProperties().forEach(conf::set);
            }
        }

        // 3. Build iceberg catalog
        catalog = CatalogUtil.buildIcebergCatalog(getName(),
                options,
                conf);
    }
}
