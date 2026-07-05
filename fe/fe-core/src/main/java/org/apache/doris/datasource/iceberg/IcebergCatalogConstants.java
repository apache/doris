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

/**
 * Iceberg catalog property keys and cache defaults that are still read by live code
 * (metastore property factories, {@link IcebergUtils}/{@link IcebergMetadataOps}) after the
 * native iceberg catalog was routed through the connector SPI. They used to live on the
 * now-removed native catalog classes; they are gathered here so the entity classes can be deleted.
 */
public class IcebergCatalogConstants {

    public static final String ICEBERG_REST = "rest";
    public static final String ICEBERG_HMS = "hms";
    public static final String ICEBERG_HADOOP = "hadoop";
    public static final String ICEBERG_GLUE = "glue";
    public static final String ICEBERG_DLF = "dlf";
    public static final String ICEBERG_JDBC = "jdbc";
    public static final String ICEBERG_S3_TABLES = "s3tables";
    public static final String EXTERNAL_CATALOG_NAME = "external_catalog.name";
    public static final String ICEBERG_MANIFEST_CACHE_ENABLE = "meta.cache.iceberg.manifest.enable";
    public static final String ICEBERG_MANIFEST_CACHE_TTL_SECOND = "meta.cache.iceberg.manifest.ttl-second";
    public static final String ICEBERG_MANIFEST_CACHE_CAPACITY = "meta.cache.iceberg.manifest.capacity";
    public static final boolean DEFAULT_ICEBERG_MANIFEST_CACHE_ENABLE = false;
    public static final long DEFAULT_ICEBERG_MANIFEST_CACHE_CAPACITY = 1024;
    public static final long DEFAULT_ICEBERG_MANIFEST_CACHE_TTL_SECOND = 48 * 60 * 60;

    private IcebergCatalogConstants() {}
}
