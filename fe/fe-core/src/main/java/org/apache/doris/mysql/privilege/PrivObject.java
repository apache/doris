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

package org.apache.doris.mysql.privilege;

import java.util.List;
import java.util.Objects;

public class PrivObject {
    private String catalog;
    private String database;
    private String table;
    private String name;
    private PrivObjectType type;
    private List<String> privileges;

    public PrivObject(String catalog, String database, String table, String name, PrivObjectType type,
            List<String> privileges) {
        this.catalog = catalog;
        this.database = database;
        this.table = table;
        this.name = name;
        this.privileges = privileges;
        this.type = type;
    }

    public enum PrivObjectType {
        GLOBAL,
        CATALOG,
        DATABASE,
        TABLE,
        RESOURCE,
        CLOUD_STAGE,
        STORAGE_VAULT,
        WORKLOAD_GROUP,
        COMPUTE_GROUP,
        COL
    }

    public String getCatalog() {
        return catalog;
    }

    public String getDatabase() {
        return database;
    }

    public String getName() {
        return name;
    }

    public List<String> getPrivileges() {
        return privileges;
    }

    public PrivObjectType getType() {
        return type;
    }

    public String getTable() {
        return table;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrivObject that = (PrivObject) o;
        return Objects.equals(catalog, that.catalog) && Objects.equals(database, that.database)
                && Objects.equals(table, that.table) && Objects.equals(name, that.name)
                && type == that.type && Objects.equals(privileges, that.privileges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalog, database, table, name, type, privileges);
    }
}
