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

import org.apache.doris.analysis.CreateViewStmt;
import org.apache.doris.common.Pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Abstract class for all databases created for mysql compatibility.
 */
public abstract class MysqlCompatibleDatabase extends Database {
    public static int COUNT = 0;

    public MysqlCompatibleDatabase(long id, String name) {
        super(id, name);
        initTables();
    }

    /**
     * Internal database is not persisted to bdb, it will be created when fe starts.
     * So driven class should implement this function to create table.
     */
    protected abstract void initTables();

    @Override
    public void unregisterTable(String name) {
        // Do nothing
    }

    /**
     * MysqlCompatibleDatabase will not be persisted to bdb.
     * It will be constructed everytime the fe starts. See
     * {@link org.apache.doris.datasource.InternalCatalog#InternalCatalog()}
     */
    @Override
    public void write(DataOutput out) throws IOException {
        throw new IOException("Not support");
    }

    /**
     * MysqlCompatibleDatabase should not be read from bdb.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        throw new IOException("Not support.");
    }

    /**
     * This method must be re-implemented since {@link Env#createView(CreateViewStmt)}
     * will call this method. And create view should not succeed under this database.
     */
    @Override
    public Pair<Boolean, Boolean> createTableWithLock(Table table, boolean isReplay, boolean setIfNotExist) {
        return Pair.of(false, false);
    }

    /**
    * All tables of mysql compatible database has case-insensitive name
    * */
    @Override
    public Table getTableNullable(String name) {
        return super.getTableNullable(name.toLowerCase());
    }
}
