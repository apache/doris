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

import org.apache.doris.info.TableValuedFunctionRefInfo;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;

/**
 * Abstract base class for system tables that use the TVF (Table-Valued Function) execution path.
 *
 * <p>TVF system tables are executed through MetadataScanNode using table-valued functions.
 * This is the legacy execution path, still used for:
 * <ul>
 *   <li>Hive partition tables (partition_values TVF)</li>
 *   <li>Iceberg metadata tables (iceberg_meta TVF)</li>
 * </ul>
 *
 * <p>Subclasses must implement:
 * <ul>
 *   <li>{@link #createFunction(String, String, String)} - creates the TVF for Nereids planner</li>
 *   <li>{@link #createFunctionRef(String, String, String)} - creates the TVF ref for legacy planner</li>
 * </ul>
 *
 * @see PartitionsSysTable
 * @see IcebergSysTable
 */
public abstract class TvfSysTable extends SysTable {

    protected final String tvfName;

    protected TvfSysTable(String sysTableName, String tvfName) {
        super(sysTableName);
        this.tvfName = tvfName;
    }

    /**
     * Always returns false for TVF system tables.
     */
    @Override
    public final boolean useNativeTablePath() {
        return false;
    }

    /**
     * Get the TVF name used for this system table.
     *
     * @return the TVF name (e.g., "partition_values", "iceberg_meta")
     */
    public String getTvfName() {
        return tvfName;
    }

    /**
     * Creates a TableValuedFunction for this system table (used by Nereids planner).
     *
     * @param ctlName catalog name
     * @param dbName database name
     * @param sourceNameWithMetaName source table name with system table suffix (e.g., "table$partitions")
     * @return the TableValuedFunction instance
     */
    public abstract TableValuedFunction createFunction(String ctlName, String dbName, String sourceNameWithMetaName);

    /**
     * Creates a TableValuedFunctionRefInfo for this system table (used by legacy planner).
     *
     * @param ctlName catalog name
     * @param dbName database name
     * @param sourceNameWithMetaName source table name with system table suffix (e.g., "table$partitions")
     * @return the TableValuedFunctionRefInfo instance
     */
    public abstract TableValuedFunctionRefInfo createFunctionRef(String ctlName, String dbName,
                                                                  String sourceNameWithMetaName);
}
