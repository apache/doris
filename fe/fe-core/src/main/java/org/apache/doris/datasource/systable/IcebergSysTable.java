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

import org.apache.doris.datasource.ExternalTable;

import org.apache.iceberg.MetadataTableType;

import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * System table type for Iceberg metadata tables.
 *
 * <p>Iceberg system tables provide access to table metadata such as
 * snapshots, history, manifests, files, partitions, etc.
 *
 * @see org.apache.iceberg.MetadataTableType for all supported system table types
 */
public class IcebergSysTable extends NativeSysTable {
    /**
     * All supported Iceberg system tables.
     * Key is the system table name (e.g., "snapshots", "history").
     */
    public static final Map<String, SysTable> SUPPORTED_SYS_TABLES = Collections.unmodifiableMap(
            Arrays.stream(MetadataTableType.values())
                    .map(type -> new IcebergSysTable(type.name().toLowerCase(Locale.ROOT)))
                    .collect(Collectors.toMap(SysTable::getSysTableName, Function.identity())));

    private final String tableName;

    private IcebergSysTable(String tableName) {
        super(tableName);
        this.tableName = tableName;
    }

    @Override
    public String getSysTableName() {
        return tableName;
    }

    @Override
    public ExternalTable createSysExternalTable(ExternalTable sourceTable) {
        // Post-cutover the native IcebergExternalTable this used to wrap is gone; the only live source
        // reaching here is HMSExternalTable (HMS-iceberg), for which this path already always threw.
        throw new IllegalArgumentException(
                "Iceberg system tables are not supported for source table type: "
                        + sourceTable.getClass().getSimpleName());
    }
}
