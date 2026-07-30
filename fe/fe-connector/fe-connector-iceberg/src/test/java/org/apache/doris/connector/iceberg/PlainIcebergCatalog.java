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

package org.apache.doris.connector.iceberg;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.List;

/**
 * A bare {@link Catalog} with NO {@link org.apache.iceberg.catalog.SupportsNamespaces} support, for
 * exercising the "catalog does not support namespaces" guard of {@code listDatabaseNames} /
 * {@code databaseExists}. Every method fails loud — the guard must short-circuit before any call.
 */
class PlainIcebergCatalog implements Catalog {

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Table loadTable(TableIdentifier identifier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
        throw new UnsupportedOperationException();
    }
}
