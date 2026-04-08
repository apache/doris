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
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergSysExternalTable;
import org.apache.doris.nereids.exceptions.AnalysisException;

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
    public static final String POSITION_DELETES = MetadataTableType.POSITION_DELETES.name().toLowerCase(Locale.ROOT);

    /**
     * All supported Iceberg system tables.
     * Key is the system table name (e.g., "snapshots", "history").
     */
    public static final Map<String, SysTable> SUPPORTED_SYS_TABLES = Collections.unmodifiableMap(
            Arrays.stream(MetadataTableType.values())
                    .filter(type -> type != MetadataTableType.POSITION_DELETES)
                    .map(type -> new IcebergSysTable(type.name().toLowerCase(Locale.ROOT), true))
                    .collect(Collectors.toMap(SysTable::getSysTableName, Function.identity())));
    public static final SysTable UNSUPPORTED_POSITION_DELETES_TABLE =
            new IcebergSysTable(POSITION_DELETES, false);

    private final String tableName;
    private final boolean supported;

    private IcebergSysTable(String tableName, boolean supported) {
        super(tableName);
        this.tableName = tableName;
        this.supported = supported;
    }

    @Override
    public String getSysTableName() {
        return tableName;
    }

    @Override
    public ExternalTable createSysExternalTable(ExternalTable sourceTable) {
        if (!supported) {
            throw new AnalysisException("SysTable " + tableName + " is not supported yet");
        }
        if (!(sourceTable instanceof IcebergExternalTable)) {
            throw new IllegalArgumentException(
                    "Expected IcebergExternalTable but got " + sourceTable.getClass().getSimpleName());
        }
        return new IcebergSysExternalTable((IcebergExternalTable) sourceTable, getSysTableName());
    }
}
