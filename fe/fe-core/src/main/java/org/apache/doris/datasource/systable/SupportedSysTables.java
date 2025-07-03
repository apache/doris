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

package org.apache.doris.datasource.systable;

import com.google.common.collect.Lists;

import java.util.List;

public class SupportedSysTables {
    // TODO: use kv map
    public static final List<SysTable> HIVE_SUPPORTED_SYS_TABLES;
    public static final List<SysTable> ICEBERG_SUPPORTED_SYS_TABLES;
    public static final List<SysTable> PAIMON_SUPPORTED_SYS_TABLES;
    public static final List<SysTable> HUDI_SUPPORTED_SYS_TABLES;

    static {
        // hive
        HIVE_SUPPORTED_SYS_TABLES = Lists.newArrayList();
        HIVE_SUPPORTED_SYS_TABLES.add(PartitionsSysTable.INSTANCE);
        // iceberg
        ICEBERG_SUPPORTED_SYS_TABLES = Lists.newArrayList(
                IcebergSysTable.getSupportedIcebergSysTables());
        // paimon
        PAIMON_SUPPORTED_SYS_TABLES = Lists.newArrayList(
                PaimonSysTable.getSupportedPaimonSysTables());
        // hudi
        HUDI_SUPPORTED_SYS_TABLES = Lists.newArrayList();
    }
}
