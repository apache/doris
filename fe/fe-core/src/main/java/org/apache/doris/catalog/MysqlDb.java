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

import org.apache.doris.common.Config;

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
public class MysqlDb extends MysqlCompatibleDatabase {
    public static final String DATABASE_NAME = Config.mysqldb_replace_name;
    /**
     * Database created by user will have database id starting from 10000 {@link Env#NEXT_ID_INIT_VALUE}.
     * InfoSchemaDb takes id 0, so we assign id 1 to MysqlDb.
    */
    public static final long DATABASE_ID = 1L;

    public MysqlDb() {
        super(DATABASE_ID, DATABASE_NAME);
    }

    /**
     * Do nothing for now.
     * If we need tables of mysql database in the future, create a MysqlTable class like {@link SchemaTable}
     */
    @Override
    public void initTables() {
        for (Table table : MysqlDBTable.TABLE_MAP.values()) {
            super.registerTable(table);
        }
    }

    @Override
    public boolean registerTable(TableIf table) {
        return false;
    }
}
