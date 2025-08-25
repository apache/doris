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
    public static final String FE_HTTP_HOSTS = "fe_http_hosts";
    public static final String FE_ARROW_HOSTS = "fe_arrow_hosts";

    public static final String USER = "user";
    public static final String PASSWORD = "password";

    public static final String HTTP_SSL_ENABLED = "http_ssl_enabled";
    public static final String ENABLE_PARALLEL_RESULT_SINK = "enable_parallel_result_sink";

    // Supports older versions of remote Doris; enabling this may introduce some inaccuracies in schema parsing.
    public static final String COMPATIBLE = "compatible";
}
