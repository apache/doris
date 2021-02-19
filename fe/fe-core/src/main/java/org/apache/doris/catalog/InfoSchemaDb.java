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

import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.SystemIdGenerator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Information schema used for MySQL compatible.
public class InfoSchemaDb extends Database {
    public static final String DATABASE_NAME = "information_schema";
    
    public InfoSchemaDb() {
        super(SystemIdGenerator.getNextId(), DATABASE_NAME);
        initTables();
    }
    
    public InfoSchemaDb(String cluster) {
        super(SystemIdGenerator.getNextId(), ClusterNamespace.getFullName(cluster, DATABASE_NAME));
        initTables();
    }

    @Override
    public boolean createTableWithLock(Table table, boolean isReplay, boolean setIfNotExist) {
        return false;
    }

    @Override
    public boolean createTable(Table table) {
        // Do nothing.
        return false;
    }

    @Override
    public void dropTable(String name) {
        // Do nothing.
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // Do nothing
    }

    public void readFields(DataInput in) throws IOException {
        throw new IOException("Not support.");
    }

    private void initTables() {
        for (Table table : SchemaTable.TABLE_MAP.values()) {
            super.createTable(table);
        }
    }

    @Override
    public Table getTable(String name) {
        return super.getTable(name.toLowerCase());
    }

    public static String getFullInfoSchemaDbName(String cluster) {
        return ClusterNamespace.getFullName(cluster, DATABASE_NAME);
    }

    public static boolean isInfoSchemaDb(String dbName) {
        if (dbName == null) {
            return false;
        }
        String[] ele = dbName.split(ClusterNamespace.CLUSTER_DELIMITER);
        String newDbName = dbName;
        if (ele.length == 2) {
            newDbName = ele[1];
        }
        return DATABASE_NAME.equalsIgnoreCase(newDbName);
    }
}
