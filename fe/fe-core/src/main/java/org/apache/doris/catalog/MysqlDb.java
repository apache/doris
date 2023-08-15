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

import org.apache.doris.alter.Alter;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.CreateViewStmt;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *  This class is used for MySQL compatibility.
 *  The mysqldump requires this database to make some
 *  command arguments like --all-databases work as expected.
 *  Otherwise, commands like
 *  <p>
 *  mysqldump -u root -p --all-databases
 *  </p>
 *  will dump nothing.
 *  Native mysql has many system tables like slow_log under mysql database,
 *  but currently we do not create any tables under mysql database of doris.
 *  We will add useful system tables in the future.
*/
public class MysqlDb extends Database {
    public static final String DATABASE_NAME = "mysql";
    /**
     * Database created by user will have database id starting from 10000 {@link Env#NEXT_ID_INIT_VALUE}.
     * InfoSchemaDb takes id 0, so we assign id 1 to MysqlDb.
    */
    public static final long DATABASE_ID = 1L;

    /**
     * For test
    */
    public MysqlDb() {
        super(DATABASE_ID, DATABASE_NAME);
        initTables();
    }

    public MysqlDb(String cluster) {
        super(DATABASE_ID, ClusterNamespace.getFullName(cluster, DATABASE_NAME));
        initTables();
    }

    /**
     * Do nothing for now.
     * If we need tables of mysql database in the future, create a MysqlTable class like {@link SchemaTable}
     */
    private void initTables() {
    }

    /**
     * This method must be re-implemented since {@link Env#createView(CreateViewStmt)}
     * will call this method. And create view should not succeed on this database.
     */
    @Override
    public Pair<Boolean, Boolean> createTableWithLock(Table table, boolean isReplay, boolean setIfNotExist) {
        return Pair.of(false, false);
    }


    /**
     * Currently, rename a table of InfoSchemaDb will throw exception
     * {@link Alter#processAlterTable(AlterTableStmt)}
     * so we follow this design.
     * @note: Rename a table of mysql database in MYSQL ls allowed.
     */
    @Override
    public boolean createTable(Table table) {
        return false;
    }

    @Override
    public void dropTable(String name) {
        // Do nothing.
    }

    /**
     * MysqlDb is not persistent to bdb. It will be constructed everytime the fe starts.
     * {@link org.apache.doris.datasource.InternalCatalog#InternalCatalog()}
     */
    @Override
    public void write(DataOutput out) throws IOException {
        // Do nothing
    }

    /**
     * Same with {@link InfoSchemaDb#readFields(DataInput)}
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        throw new IOException("Not support.");
    }

    /**
     * Same with {@link InfoSchemaDb#getTableNullable(String)}
     */
    @Override
    public Table getTableNullable(String name) {
        return super.getTableNullable(name.toLowerCase());
    }

    public static boolean isMysqlDb(String dbName) {
        if (dbName == null) {
            return false;
        }

        String[] elements = dbName.split(ClusterNamespace.CLUSTER_DELIMITER);
        String newDbName = dbName;
        if (elements.length == 2) {
            newDbName = elements[1];
        }
        return DATABASE_NAME.equalsIgnoreCase(newDbName);
    }
}
