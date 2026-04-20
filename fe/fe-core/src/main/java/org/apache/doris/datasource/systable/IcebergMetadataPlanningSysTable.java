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
import org.apache.doris.datasource.iceberg.IcebergMetadataPlanningExternalTable;

/**
 * Internal-only sys table entry for Iceberg metadata planning.
 *
 * <p>This table is intentionally not part of the public supported sys table map
 * until the dedicated planning scan path is fully implemented.
 */
public class IcebergMetadataPlanningSysTable extends NativeSysTable {
    public IcebergMetadataPlanningSysTable(String sysTableName) {
        super(sysTableName);
    }

    @Override
    public ExternalTable createSysExternalTable(ExternalTable sourceTable) {
        if (!(sourceTable instanceof IcebergExternalTable)) {
            throw new IllegalArgumentException(
                    "Expected IcebergExternalTable but got " + sourceTable.getClass().getSimpleName());
        }
        return new IcebergMetadataPlanningExternalTable((IcebergExternalTable) sourceTable, getSysTableName());
    }
}
