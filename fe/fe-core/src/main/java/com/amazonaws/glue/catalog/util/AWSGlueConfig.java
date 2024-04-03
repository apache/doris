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
//
// Copied from
// https://github.com/awslabs/aws-glue-data-catalog-client-for-apache-hive-metastore/blob/branch-3.4.0/
//

package com.amazonaws.glue.catalog.util;

import com.amazonaws.ClientConfiguration;

public final class AWSGlueConfig {

    private AWSGlueConfig() {
    }

    public static final String AWS_GLUE_ENDPOINT = "aws.glue.endpoint";
    public static final String AWS_REGION = "aws.region";
    public static final String AWS_CATALOG_CREDENTIALS_PROVIDER_FACTORY_CLASS
            = "aws.catalog.credentials.provider.factory.class";

    public static final String AWS_GLUE_MAX_RETRY = "aws.glue.max-error-retries";
    public static final int DEFAULT_MAX_RETRY = 5;

    public static final String AWS_GLUE_MAX_CONNECTIONS = "aws.glue.max-connections";
    public static final int DEFAULT_MAX_CONNECTIONS = ClientConfiguration.DEFAULT_MAX_CONNECTIONS;

    public static final String AWS_GLUE_CONNECTION_TIMEOUT = "aws.glue.connection-timeout";
    public static final int DEFAULT_CONNECTION_TIMEOUT = ClientConfiguration.DEFAULT_CONNECTION_TIMEOUT;

    public static final String AWS_GLUE_SOCKET_TIMEOUT = "aws.glue.socket-timeout";
    public static final int DEFAULT_SOCKET_TIMEOUT = ClientConfiguration.DEFAULT_SOCKET_TIMEOUT;

    public static final String AWS_GLUE_CATALOG_SEPARATOR = "aws.glue.catalog.separator";

    public static final String AWS_GLUE_DISABLE_UDF = "aws.glue.disable-udf";


    public static final String AWS_GLUE_DB_CACHE_ENABLE = "aws.glue.cache.db.enable";
    public static final String AWS_GLUE_DB_CACHE_SIZE = "aws.glue.cache.db.size";
    public static final String AWS_GLUE_DB_CACHE_TTL_MINS = "aws.glue.cache.db.ttl-mins";

    public static final String AWS_GLUE_TABLE_CACHE_ENABLE = "aws.glue.cache.table.enable";
    public static final String AWS_GLUE_TABLE_CACHE_SIZE = "aws.glue.cache.table.size";
    public static final String AWS_GLUE_TABLE_CACHE_TTL_MINS = "aws.glue.cache.table.ttl-mins";

    public static final String AWS_GLUE_ACCESS_KEY = "aws.glue.access-key";
    public static final String AWS_GLUE_SECRET_KEY = "aws.glue.secret-key";
    public static final String AWS_GLUE_SESSION_TOKEN = "aws.glue.session-token";
}
