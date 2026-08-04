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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogIf;

/** Shared locking helpers for constraint DDL commands. */
final class ConstraintCommandUtils {
    private ConstraintCommandUtils() {
    }

    /**
     * Lock and return the database currently bound to the persisted qualified name.
     *
     * <p>The caller must release the returned database's read lock. Rechecking identity after
     * locking rejects a database that was dropped and recreated with the same name between
     * resolution and lock acquisition.</p>
     */
    static DatabaseIf<? extends TableIf> lockCurrentDatabase(TableNameInfo tableNameInfo)
            throws DdlException {
        CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog = Env.getCurrentEnv()
                .getCatalogMgr().getCatalogOrDdlException(tableNameInfo.getCtl());
        DatabaseIf<? extends TableIf> database =
                catalog.getDbOrDdlException(tableNameInfo.getDb());
        database.readLock();
        if (Env.getCurrentEnv().getCatalogMgr().getCatalog(tableNameInfo.getCtl()) != catalog
                || catalog.getDbNullable(tableNameInfo.getDb()) != database) {
            database.readUnlock();
            throw new DdlException("Database changed while altering constraint on " + tableNameInfo);
        }
        return database;
    }
}
