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

package org.apache.doris.catalog;

// Information schema used for MySQL compatible.
public class InfoSchemaDb extends MysqlCompatibleDatabase {
    public static final String DATABASE_NAME = "information_schema";
    public static final long DATABASE_ID = 0L;

    public InfoSchemaDb() {
        super(DATABASE_ID, DATABASE_NAME);
    }

    @Override
    protected void initTables() {
        for (Table table : SchemaTable.TABLE_MAP.values()) {
            super.createTable(table);
        }
    }

    @Override
    public boolean createTable(Table table) {
        return false;
    }
}
