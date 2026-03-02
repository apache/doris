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

import org.apache.doris.common.Pair;

/**
 * Base class defining a system table type and its metadata.
 *
 * <p>System table types describe what system tables are available for a given table type.
 * For example, Paimon tables support "snapshots", "binlog", "audit_log" system tables,
 * while Iceberg tables support "snapshots", "history", "manifests" system tables.
 *
 * <p>This class provides:
 * <ul>
 *   <li>System table name and suffix (e.g., "snapshots" → "$snapshots")</li>
 *   <li>Methods to check if a table name matches this system table type</li>
 * </ul>
 *
 * <p>Subclasses should extend one of the specialized abstract classes:
 * <ul>
 *   <li>{@link NativeSysTable} - for tables using native execution path (FileQueryScanNode)</li>
 *   <li>{@link TvfSysTable} - for tables using TVF execution path (MetadataScanNode)</li>
 * </ul>
 *
 * @see NativeSysTable
 * @see TvfSysTable
 */
public abstract class SysTable {
    // eg. table$partitions
    //  sysTableName => partitions
    //  suffix => $partitions
    protected final String sysTableName;
    protected final String suffix;

    protected SysTable(String sysTableName) {
        this.sysTableName = sysTableName;
        this.suffix = "$" + sysTableName.toLowerCase();
    }

    public String getSysTableName() {
        return sysTableName;
    }

    public String getSuffix() {
        return suffix;
    }

    public String getSourceTableName(String tableName) {
        return tableName.substring(0, tableName.length() - suffix.length());
    }

    /**
     * Returns true if this system table reads actual data files (ORC/Parquet).
     * Data-oriented system tables (e.g., binlog, audit_log, ro) benefit from
     * native vectorized readers.
     *
     * Metadata-oriented system tables (e.g., snapshots, partitions) return false
     * and use JNI readers to access metadata.
     *
     * @return true for data-oriented system tables, false for metadata-oriented
     */
    public boolean isDataTable() {
        return false;
    }

    /**
     * Returns true if this system table uses native table execution path
     * (FileQueryScanNode) instead of TVF path (MetadataScanNode).
     *
     * @return true for NativeSysTable, false for TvfSysTable
     */
    public abstract boolean useNativeTablePath();

    // table$partition => <table, partition>
    // table$xx$partition => <table$xx, partition>
    public static Pair<String, String> getTableNameWithSysTableName(String input) {
        int lastDollarIndex = input.lastIndexOf('$');
        if (lastDollarIndex == -1 || lastDollarIndex == input.length() - 1) {
            return Pair.of(input, "");
        } else {
            String before = input.substring(0, lastDollarIndex);
            String after = input.substring(lastDollarIndex + 1);
            return Pair.of(before, after);
        }
    }
}
