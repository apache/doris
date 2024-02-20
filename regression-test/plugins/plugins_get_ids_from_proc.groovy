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

import org.apache.doris.regression.suite.Suite

Suite.metaClass.get_catalog_id = {String catalog_name /* param */ ->
    String catalog_id;
    def catalogs = sql """show proc '/catalogs'"""
    for (catalog in catalogs) {
        if (catalog[1].equals(catalog_name)) {
            catalog_id = catalog[0]
            break
        }
    }
    log.info("get catalogid: " + catalog_id)
    return catalog_id
}


Suite.metaClass.get_database_id = {String catalog_name, String db_name /* param */ ->
    String database_id;
    def catalog_id = get_catalog_id(catalog_name)
    def dbs = sql """show proc '/catalogs/${catalog_id}'"""
    for (db in dbs) {
        if (db[1].equals(db_name)) {
            database_id = db[0]
            break
        }
    }
    log.info("get database_id: " + database_id)
    return database_id
}


Suite.metaClass.get_table_id = {String catalog_name, String db_name, String tb_name /* param */ ->
    String table_id;
    def catalog_id = get_catalog_id(catalog_name)
    def database_id = get_database_id(catalog_name, db_name)
    def tbs = sql """show proc '/catalogs/${catalog_id}/${database_id}'"""
    for (tb in tbs) {
        if (tb[1].equals(tb_name)) {
            table_id = tb[0]
            break
        }
    }
    log.info("get table_id: " + table_id)
    return table_id
}
