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

package org.apache.doris.datasource.infoschema;

import org.apache.doris.analysis.SchemaTableType;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.SchemaTable;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.thrift.TSchemaTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import java.util.Optional;

public class ExternalInfoSchemaTable extends ExternalTable {

    public ExternalInfoSchemaTable(long id, String name, ExternalCatalog catalog) {
        super(id, name, catalog, InfoSchemaDb.DATABASE_NAME, TableType.SCHEMA);
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        makeSureInitialized();
        return Optional.of(new SchemaCacheValue(SchemaTable.TABLE_MAP.get(name).getFullSchema()));
    }

    @Override
    public TTableDescriptor toThrift() {
        TSchemaTable tSchemaTable = new TSchemaTable(SchemaTableType.getThriftType(this.name));
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.SCHEMA_TABLE,
                getFullSchema().size(), 0, this.name, "");
        tTableDescriptor.setSchemaTable(tSchemaTable);
        return tTableDescriptor;
    }
}
