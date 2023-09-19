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

package org.apache.doris.catalog.external;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.BigQueryExternalCatalog;

import com.google.api.client.util.Lists;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;

import java.util.List;

public class BigQueryExternalTable extends ExternalTable {
    private Table bigQueryTable;

    public BigQueryExternalTable(long id, String name, String dbName, BigQueryExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.BIGQUERY_EXTERNAL_TABLE);
    }

    @Override
    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            bigQueryTable = ((BigQueryExternalCatalog) catalog).getClient().getTable(dbName, name);
            objectCreated = true;
        }
    }

    @Override
    public List<Column> initSchema() {
        makeSureInitialized();
        Schema schema = bigQueryTable.getDefinition().getSchema();

        List<Column> result = Lists.newArrayList();
        if (schema != null) {
            for (Field field : schema.getFields()) {
                result.add(new Column(field.getName(), bigQueryTypeToDorisType(field), true, null,
                        true, field.getDescription(), true, -1));
            }
        }
        return result;
    }

    public Type bigQueryTypeToDorisType(Field field) {
        LegacySQLTypeName type = field.getType();
        Mode mode = field.getMode();
        if (mode == Mode.REPEATED) {
            switch (type.getStandardType()) {
                case BOOL:
                    return ArrayType.create(Type.BOOLEAN, true);
                case INT64:
                    return ArrayType.create(Type.BIGINT, true);
                case FLOAT64:
                    return ArrayType.create(Type.DOUBLE, true);
                case STRING:
                    return ArrayType.create(Type.VARCHAR, true);
                default:
                    return Type.UNSUPPORTED;
            }
        }
        switch (type.getStandardType()) {
            case BOOL:
                return Type.BOOLEAN;
            case INT64:
                return Type.BIGINT;
            case FLOAT64:
                return Type.DOUBLE;
            case NUMERIC:
                return ScalarType.createDecimalV3Type(38, 9);
            case DATE:
                return Type.DATEV2;
            case DATETIME:
            case TIMESTAMP:
                return ScalarType.createDatetimeV2Type(0);
            case TIME:
                return Type.TIME;
            case STRING:
            case BIGNUMERIC:
                return Type.STRING;
            default:
                return Type.UNSUPPORTED;
        }
    }
}
