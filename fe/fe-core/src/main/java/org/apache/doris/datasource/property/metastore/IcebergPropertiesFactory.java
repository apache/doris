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

import java.util.Map;

/**
 * Factory for creating {@link MetastoreProperties} instances for Iceberg catalogs.
 * <p>
 * The required property key is "iceberg.catalog.type".
 * <p>
 * Supported subtypes include:
 * - "rest"     -> {@link IcebergRestProperties}
 * - "glue"     -> {@link IcebergGlueMetaStoreProperties}
 * - "hms"      -> {@link IcebergHMSMetaStoreProperties}
 * - "hadoop"   -> {@link IcebergFileSystemMetaStoreProperties}
 * - "s3tables" -> {@link IcebergS3TablesMetaStoreProperties}
 * - "dlf"      -> {@link IcebergAliyunDLFMetaStoreProperties}
 */
public class IcebergPropertiesFactory extends AbstractMetastorePropertiesFactory {

    private static final String KEY = "iceberg.catalog.type";

    public IcebergPropertiesFactory() {
        register("rest", IcebergRestProperties::new);
        register("glue", IcebergGlueMetaStoreProperties::new);
        register("hms", IcebergHMSMetaStoreProperties::new);
        register("hadoop", IcebergFileSystemMetaStoreProperties::new);
        register("s3tables", IcebergS3TablesMetaStoreProperties::new);
        register("dlf", IcebergAliyunDLFMetaStoreProperties::new);
    }

    @Override
    public MetastoreProperties create(Map<String, String> props) {
        return createInternal(props, KEY, null); // No default, strictly required
    }
}
