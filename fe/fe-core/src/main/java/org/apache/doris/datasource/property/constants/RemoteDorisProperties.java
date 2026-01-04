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

public class RemoteDorisProperties {
    public static final String FE_THRIFT_HOSTS = "fe_thrift_hosts";
    public static final String FE_HTTP_HOSTS = "fe_http_hosts";
    public static final String FE_ARROW_HOSTS = "fe_arrow_hosts";

    public static final String USER = "user";
    public static final String PASSWORD = "password";

    public static final String ENABLE_PARALLEL_RESULT_SINK = "enable_parallel_result_sink";

    // query remote doris use arrow flight or treat it as olap table
    public static final String USE_ARROW_FLIGHT = "use_arrow_flight";

    // Supports older versions of remote Doris; enabling this may introduce some inaccuracies in schema parsing.
    public static final String COMPATIBLE = "compatible";

    /**
     * For Arrow Flight query.
     **/
    public static final String QUERY_RETRY_COUNT = "query_retry_count";
    // Query execution is asynchronous on the server; the client does not wait for completion.
    public static final String QUERY_TIMEOUT_SEC = "query_timeout_sec";

    /**
     * For metadata HTTP synchronization.
     **/
    public static final String METADATA_HTTP_SSL_ENABLED = "metadata_http_ssl_enabled";
    public static final String METADATA_SYNC_RETRIES_COUNT = "metadata_sync_retry_count";
    public static final String METADATA_MAX_IDLE_CONNECTIONS = "metadata_max_idle_connections";
    public static final String METADATA_KEEP_ALIVE_DURATION_SEC = "metadata_keep_alive_duration_sec";
    public static final String METADATA_CONNECT_TIMEOUT_SEC = "metadata_connect_timeout_sec";
    public static final String METADATA_READ_TIMEOUT_SEC = "metadata_read_timeout_sec";
    public static final String METADATA_WRITE_TIMEOUT_SEC = "metadata_write_timeout_sec";
    public static final String METADATA_CALL_TIMEOUT_SEC = "metadata_call_timeout_sec";
}
