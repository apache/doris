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

package org.apache.doris.nereids.analyzer;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.MysqlCompatibleDatabase;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.infoschema.ExternalInfoSchemaDatabase;
import org.apache.doris.datasource.infoschema.ExternalMysqlDatabase;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalDatabase;
import org.apache.doris.nereids.exceptions.AnalysisException;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.Supplier;

/** The physical database inferred from a view definition, scoped to analysis of that view's body. */
public final class ViewDefaultDatabase {
    private final CatalogIf<?> catalog;
    private final String remoteName;
    private final Supplier<String> localName;

    public ViewDefaultDatabase(CatalogIf<?> catalog, String remoteName) {
        this.catalog = catalog;
        this.remoteName = remoteName;
        // Local identifier mapping may initialize the target connector. Only a bare table name needs it.
        this.localName = Suppliers.memoize(() -> catalog instanceof ExternalCatalog
                ? ((ExternalCatalog) catalog).localDatabaseNameForView(remoteName) : remoteName);
    }

    public List<String> qualify(String tableName) {
        return ImmutableList.of(catalog.getName(), localName.get(), tableName);
    }

    /** Check the actual table used for planning, including a table already cached in this statement. */
    public void checkTable(TableIf table) {
        DatabaseIf<?> database = table.getDatabase();
        if (database == null || database.getCatalog() != catalog || !matchesPhysicalDatabase(database)) {
            throw new AnalysisException("Saved view namespace in catalog '" + catalog.getName()
                    + "' does not resolve to its physical database '" + remoteName + "'");
        }
    }

    private boolean matchesPhysicalDatabase(DatabaseIf<?> database) {
        if (catalog instanceof PluginDrivenExternalCatalog) {
            return database instanceof PluginDrivenExternalDatabase
                    && remoteName.equals(((PluginDrivenExternalDatabase) database).getRemoteName());
        }
        if (database instanceof ExternalDatabase) {
            return !(database instanceof ExternalInfoSchemaDatabase || database instanceof ExternalMysqlDatabase)
                    && remoteName.equals(((ExternalDatabase<?>) database).getRemoteName());
        }
        return !(database instanceof MysqlCompatibleDatabase) && remoteName.equals(database.getFullName());
    }
}
