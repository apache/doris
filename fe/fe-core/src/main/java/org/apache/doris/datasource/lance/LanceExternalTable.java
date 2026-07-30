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

package org.apache.doris.datasource.lance;

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ExternalAnalysisTask;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class LanceExternalTable extends ExternalTable {
    public LanceExternalTable(long id, String name, String remoteName, LanceExternalCatalog catalog,
            LanceExternalDatabase db) {
        super(id, name, remoteName, catalog, db, TableType.LANCE_EXTERNAL_TABLE);
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        LanceTableMetadata metadata = loadMetadata();
        List<Column> columns = new ArrayList<>(metadata.getSchema().getFields().size());
        int position = 0;
        for (Field field : metadata.getSchema().getFields()) {
            String comment = field.getMetadata() == null ? null : field.getMetadata().get("comment");
            columns.add(new Column(field.getName(), LanceTypeConverter.toDorisType(field), false,
                    null, field.isNullable(), comment, true, position++));
        }
        return Optional.of(new SchemaCacheValue(columns));
    }

    public LanceTableMetadata loadMetadata() {
        return ((LanceExternalCatalog) catalog).loadTableMetadata(db.getRemoteName(), remoteName);
    }

    @Override
    public long fetchRowCount() {
        long rowCount = loadMetadata().getRowCount();
        return rowCount > 0 ? rowCount : UNKNOWN_ROW_COUNT;
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        THiveTable thriftTable = new THiveTable(dbName, name, new HashMap<>());
        TTableDescriptor descriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE,
                schema.size(), 0, getName(), dbName);
        descriptor.setHiveTable(thriftTable);
        return descriptor;
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        makeSureInitialized();
        return new ExternalAnalysisTask(info);
    }
}
