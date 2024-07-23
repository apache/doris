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

package org.apache.doris.mysql;

/**
 * according to mysql text protocol ColumnDefinition41. Field should be filled by these attribute.
 */
public class FieldInfo {

    private final String schema;
    private final String table;
    private final String originalTable;
    private final String name;
    private final String originalName;

    public FieldInfo(String schema, String table, String originalTable, String name, String originalName) {
        this.schema = schema;
        this.table = table;
        this.originalTable = originalTable;
        this.name = name;
        this.originalName = originalName;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getOriginalTable() {
        return originalTable;
    }

    public String getName() {
        return name;
    }

    public String getOriginalName() {
        return originalName;
    }
}
