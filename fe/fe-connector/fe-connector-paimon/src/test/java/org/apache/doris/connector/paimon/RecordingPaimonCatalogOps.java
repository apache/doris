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

package org.apache.doris.connector.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * Hand-written recording fake for {@link PaimonCatalogOps} (no Mockito), mirroring the
 * maxcompute connector's recording {@code McStructureHelper}.
 *
 * <p>Records an ordered call log, returns configurable fixed data, and can be told to throw
 * the paimon {@code DatabaseNotExistException} / {@code TableNotExistException} that the
 * production code catches. Because the seam fully covers every remote call
 * {@link PaimonConnectorMetadata} makes, the metadata under test is built with a {@code null}
 * real Catalog — the test stays entirely offline.
 */
final class RecordingPaimonCatalogOps implements PaimonCatalogOps {

    final List<String> log = new ArrayList<>();

    List<String> databases = new ArrayList<>();
    List<String> tables = new ArrayList<>();
    Table table;

    boolean throwDatabaseNotExist;
    boolean throwTableNotExist;

    @Override
    public List<String> listDatabases() {
        log.add("listDatabases");
        return databases;
    }

    @Override
    public Database getDatabase(String name) throws Catalog.DatabaseNotExistException {
        log.add("getDatabase:" + name);
        if (throwDatabaseNotExist) {
            throw new Catalog.DatabaseNotExistException(name);
        }
        // databaseExists ignores the returned Database (only the throw/no-throw matters),
        // so a null is sufficient and keeps the fake free of a Database double.
        return null;
    }

    @Override
    public List<String> listTables(String databaseName) throws Catalog.DatabaseNotExistException {
        log.add("listTables:" + databaseName);
        if (throwDatabaseNotExist) {
            throw new Catalog.DatabaseNotExistException(databaseName);
        }
        return tables;
    }

    @Override
    public Table getTable(Identifier identifier) throws Catalog.TableNotExistException {
        log.add("getTable:" + identifier.getFullName());
        if (throwTableNotExist) {
            throw new Catalog.TableNotExistException(identifier);
        }
        return table;
    }

    @Override
    public void close() {
        log.add("close");
    }
}
