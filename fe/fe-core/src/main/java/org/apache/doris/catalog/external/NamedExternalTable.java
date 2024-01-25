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

package org.apache.doris.catalog.external;

import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;

/**
 * use to save table info.
 */
public class NamedExternalTable extends ExternalTable {

    private NamedExternalTable(long id, String name, String dbName, ExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.HMS_EXTERNAL_TABLE);
    }

    public void setUpdateTime(long updateTime) {
        schemaUpdateTime = updateTime;
    }

    /**
     *
     * @param id id
     * @param tableName table name
     * @param dbName db name
     * @param catalog catalog
     * @return NamedExternalTable external table name info
     */
    public static NamedExternalTable of(long id, String tableName, String dbName, ExternalCatalog catalog) {
        return new NamedExternalTable(id, tableName, dbName, catalog);
    }
}


