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

package org.apache.doris.nereids.util;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * relation util
 */
public class RelationUtil {

    /**
     * get table qualifier
     */
    public static List<String> getQualifierName(ConnectContext context, List<String> nameParts) {
        switch (nameParts.size()) {
            case 1: { // table
                // Use current database name from catalog.
                String tableName = nameParts.get(0);
                CatalogIf catalogIf = context.getCurrentCatalog();
                if (catalogIf == null) {
                    throw new IllegalStateException("Current catalog is not set.");
                }
                String catalogName = catalogIf.getName();
                String dbName = context.getDatabase();
                if (Strings.isNullOrEmpty(dbName)) {
                    throw new IllegalStateException("Current database is not set.");
                }
                return ImmutableList.of(catalogName, dbName, tableName);
            }
            case 2: { // db.table
                // Use database name from table name parts.
                CatalogIf catalogIf = context.getCurrentCatalog();
                if (catalogIf == null) {
                    throw new IllegalStateException("Current catalog is not set.");
                }
                String catalogName = catalogIf.getName();
                // if the relation is view, nameParts.get(0) is dbName.
                String dbName = nameParts.get(0);
                String tableName = nameParts.get(1);
                return ImmutableList.of(catalogName, dbName, tableName);
            }
            case 3: { // catalog.db.table
                // Use catalog and database name from name parts.
                String catalogName = nameParts.get(0);
                String dbName = nameParts.get(1);
                String tableName = nameParts.get(2);
                return ImmutableList.of(catalogName, dbName, tableName);
            }
            default:
                throw new IllegalStateException("Table name [" + java.lang.String
                        .join(".", nameParts) + "] is invalid.");
        }
    }

    /**
     * get table
     */
    public static TableIf getTable(List<String> qualifierName, Env env) {
        return getDbAndTable(qualifierName, env).second;
    }

    /**
     * get database and table
     */
    public static Pair<DatabaseIf<?>, TableIf> getDbAndTable(List<String> qualifierName, Env env) {
        String catalogName = qualifierName.get(0);
        String dbName = qualifierName.get(1);
        String tableName = qualifierName.get(2);
        CatalogIf<?> catalog = env.getCatalogMgr().getCatalog(catalogName);
        if (catalog == null) {
            throw new AnalysisException(java.lang.String.format("Catalog %s does not exist.", catalogName));
        }
        try {
            DatabaseIf<TableIf> db = catalog.getDb(dbName).orElseThrow(() -> new AnalysisException(
                    "Database [" + dbName + "] does not exist."));
            TableIf table = db.getTable(tableName).orElseThrow(() -> new AnalysisException(
                    "Table [" + tableName + "] does not exist in database [" + dbName + "]."));
            return Pair.of(db, table);
        } catch (Throwable e) {
            throw new AnalysisException(e.getMessage(), e.getCause());
        }
    }
}
