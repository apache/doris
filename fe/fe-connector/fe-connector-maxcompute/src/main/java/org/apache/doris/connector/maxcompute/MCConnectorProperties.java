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

package org.apache.doris.connector.maxcompute;

/**
 * Property constants for MaxCompute catalog configuration.
 * Adapted from fe-common MCProperties — copied into plugin to avoid
 * depending on fe-common (which transitively pulls hadoop-common).
 */
public final class MCConnectorProperties {
    private MCConnectorProperties() {
    }

    // Legacy properties for backward compatibility
    public static final String REGION = "mc.region";
    public static final String PUBLIC_ACCESS = "mc.public_access";
    public static final String DEFAULT_PUBLIC_ACCESS = "false";
    public static final String ODPS_ENDPOINT = "mc.odps_endpoint";
    public static final String TUNNEL_SDK_ENDPOINT = "mc.tunnel_endpoint";

    // Core connection properties
    public static final String PROJECT = "mc.default.project";
    public static final String SESSION_TOKEN = "mc.session_token";
    public static final String ACCESS_KEY = "mc.access_key";
    public static final String SECRET_KEY = "mc.secret_key";
    public static final String ENDPOINT = "mc.endpoint";

    public static final String QUOTA = "mc.quota";
    public static final String DEFAULT_QUOTA = "pay-as-you-go";

    // Split strategy
    public static final String SPLIT_STRATEGY = "mc.split_strategy";
    public static final String SPLIT_BY_BYTE_SIZE_STRATEGY = "byte_size";
    public static final String SPLIT_BY_ROW_COUNT_STRATEGY = "row_count";
    public static final String DEFAULT_SPLIT_STRATEGY =
            SPLIT_BY_BYTE_SIZE_STRATEGY;

    public static final String SPLIT_BYTE_SIZE = "mc.split_byte_size";
    public static final String DEFAULT_SPLIT_BYTE_SIZE = "268435456";
    public static final String SPLIT_ROW_COUNT = "mc.split_row_count";
    public static final String DEFAULT_SPLIT_ROW_COUNT = "1048576";

    // Timeout and retry
    public static final String CONNECT_TIMEOUT = "mc.connect_timeout";
    public static final String READ_TIMEOUT = "mc.read_timeout";
    public static final String RETRY_COUNT = "mc.retry_count";
    public static final String DEFAULT_CONNECT_TIMEOUT = "10";
    public static final String DEFAULT_READ_TIMEOUT = "120";
    public static final String DEFAULT_RETRY_COUNT = "4";

    public static final String MAX_FIELD_SIZE = "mc.max_field_size_bytes";
    public static final String DEFAULT_MAX_FIELD_SIZE = "8388608";

    public static final String MAX_WRITE_BATCH_ROWS =
            "mc.max_write_batch_rows";
    public static final String DEFAULT_MAX_WRITE_BATCH_ROWS = "4096";

    public static final String SPLIT_CROSS_PARTITION =
            "mc.split_cross_partition";
    public static final String DEFAULT_SPLIT_CROSS_PARTITION = "true";

    public static final String DATETIME_PREDICATE_PUSH_DOWN =
            "mc.datetime_predicate_push_down";
    public static final String DEFAULT_DATETIME_PREDICATE_PUSH_DOWN = "true";

    // Account format
    public static final String ACCOUNT_FORMAT = "mc.account_format";
    public static final String ACCOUNT_FORMAT_NAME = "name";
    public static final String ACCOUNT_FORMAT_ID = "id";
    public static final String DEFAULT_ACCOUNT_FORMAT = ACCOUNT_FORMAT_NAME;

    // Namespace schema toggle
    public static final String ENABLE_NAMESPACE_SCHEMA =
            "mc.enable.namespace.schema";
    public static final String DEFAULT_ENABLE_NAMESPACE_SCHEMA = "false";

    // Authentication types
    public static final String AUTH_TYPE = "mc.auth.type";
    public static final String AUTH_TYPE_AK_SK = "ak_sk";
    public static final String AUTH_TYPE_RAM_ROLE_ARN = "ram_role_arn";
    public static final String AUTH_TYPE_ECS_RAM_ROLE = "ecs_ram_role";
    public static final String DEFAULT_AUTH_TYPE = AUTH_TYPE_AK_SK;

    public static final String RAM_ROLE_ARN = "mc.ram_role_arn";
    public static final String ECS_RAM_ROLE = "mc.ecs_ram_role";
}
