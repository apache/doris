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

package org.apache.doris.datasource.fluss.source;

import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.datasource.TableFormatType;

public class FlussSplit extends FileSplit {
    private final String databaseName;
    private final String tableName;
    private final long tableId;
    private final TableFormatType tableFormatType;

    public FlussSplit(String databaseName, String tableName, long tableId) {
        // Create a dummy path - actual file paths will be resolved by BE using Rust bindings
        super(LocationPath.of("/fluss-table"), 0, 0, 0, 0, null, null);
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tableId = tableId;
        this.tableFormatType = TableFormatType.FLUSS;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public long getTableId() {
        return tableId;
    }

    public TableFormatType getTableFormatType() {
        return tableFormatType;
    }

    @Override
    public String getConsistentHashString() {
        return databaseName + "." + tableName + "." + tableId;
    }
}

