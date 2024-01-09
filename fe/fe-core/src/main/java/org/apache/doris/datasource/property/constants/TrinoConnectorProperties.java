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

package org.apache.doris.datasource.property.constants;

public class TrinoConnectorProperties {
    public static final String TRINO_CONNECTOR_NAME = "connector.name";

    // ================= hive connector =================
    public static final String TRINO_CONNECTOR_HIVE_METASTORE_URI = "hive.metastore.uri";
    public static final String TRINO_CONNECTOR_HIVE_CONFIG_RESOURCES = "hive.config.resources";

    // ================= mysql connector =================
    public static final String TRINO_MYSQL_CONNECTION_URL =  "connection-url";
    public static final String TRINO_MYSQL_CONNECTION_USER = "connection-user";
    public static final String TRINO_MYSQL_CONNECTION_PASSWORD = "connection-password";

    public static final String TRINO_CONNECTOR_PREFIX = "";
    public static final String TRINO_CONNECTOR_CATALOG_TYPE = "metastore";
    public static final String HIVE_METASTORE_URIS = "uri";
    public static final String TRINO_CONNECTOR_S3_ENDPOINT = "s3.endpoint";
    public static final String TRINO_CONNECTOR_S3_ACCESS_KEY = "s3.access-key";
    public static final String TRINO_CONNECTOR_S3_SECRET_KEY = "s3.secret-key";
    public static final String TRINO_CONNECTOR_OSS_ENDPOINT = org.apache.hadoop.fs.aliyun.oss.Constants.ENDPOINT_KEY;
    public static final String TRINO_CONNECTOR_OSS_ACCESS_KEY = org.apache.hadoop.fs.aliyun.oss.Constants.ACCESS_KEY_ID;
    public static final String TRINO_CONNECTOR_OSS_SECRET_KEY =
            org.apache.hadoop.fs.aliyun.oss.Constants.ACCESS_KEY_SECRET;
    public static final String TRINO_CONNECTOR_HMS_CATALOG = "hive";
    public static final String TRINO_CONNECTOR_FILESYSTEM_CATALOG = "filesystem";
}
