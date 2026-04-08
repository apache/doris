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

/**
 * Abstract base class for system tables that use the native table execution path.
 *
 * <p>Native system tables are executed through FileQueryScanNode (e.g., PaimonScanNode,
 * IcebergScanNode) instead of MetadataScanNode. This provides:
 * <ul>
 *   <li>Unified execution path with regular tables</li>
 *   <li>Native vectorized reading for data-oriented system tables</li>
 *   <li>Better integration with query optimization</li>
 * </ul>
 *
 * <p>Subclasses must implement {@link #createSysExternalTable(ExternalTable)} to create
 * the appropriate system external table instance (e.g., PaimonSysExternalTable).
 *
 * @see PaimonSysTable
 */
public abstract class NativeSysTable extends SysTable {

    protected NativeSysTable(String sysTableName) {
        super(sysTableName);
    }

    /**
     * Always returns true for native system tables.
     */
    @Override
    public final boolean useNativeTablePath() {
        return true;
    }

    /**
     * Creates a system external table for this system table type.
     *
     * <p>The returned ExternalTable wraps the source table and provides access
     * to system table data through the native execution path.
     *
     * @param sourceTable the source external table being wrapped
     * @return the system external table instance
     */
    public abstract ExternalTable createSysExternalTable(ExternalTable sourceTable);
}
