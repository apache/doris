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

package org.apache.doris.maxcompute;

/**
 * The catalog properties this plugin reads out of the parameter map BE forwards from FE.
 *
 * <p>They are spelled here rather than shared with FE. The names are a wire contract, and FE
 * already keeps its own copy of them in {@code MCCatalogProperties} in fe-connector-maxcompute -
 * the connector that emits them - so a shared constant would have to live in a module both sides
 * depend on. There is none, and creating one for eleven strings would put fe-common (63 jars, most
 * of them netty and the AWS SDK) inside every MaxCompute plugin directory. A plugin declares the
 * parameters it reads, the same way the jdbc and iceberg plugins do.
 */
public class MCProperties {

    public static final String ACCESS_KEY = "mc.access_key";
    public static final String SECRET_KEY = "mc.secret_key";

    public static final String AUTH_TYPE = "mc.auth.type";
    public static final String AUTH_TYPE_AK_SK = "ak_sk";
    public static final String AUTH_TYPE_RAM_ROLE_ARN = "ram_role_arn";
    public static final String AUTH_TYPE_ECS_RAM_ROLE = "ecs_ram_role";
    public static final String DEFAULT_AUTH_TYPE = AUTH_TYPE_AK_SK;

    public static final String RAM_ROLE_ARN = "mc.ram_role_arn";
    public static final String ECS_RAM_ROLE = "mc.ecs_ram_role";

    /**
     * Approximate upper bound in bytes for a single MaxCompute write block. Once a block is
     * estimated to exceed it, the writer asks FE for the next block id and starts over.
     */
    public static final String WRITE_MAX_BLOCK_BYTES = "mc.write_max_block_bytes";
    public static final String DEFAULT_WRITE_MAX_BLOCK_BYTES = "67108864"; // 64 * 1024 * 1024

    private MCProperties() {}
}
