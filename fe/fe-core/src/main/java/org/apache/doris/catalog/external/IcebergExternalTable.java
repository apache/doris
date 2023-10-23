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
import org.apache.doris.catalog.HiveMetaStoreClientHelper;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TIcebergTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.collect.Lists;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class IcebergExternalTable extends ExternalTable {

    // https://iceberg.apache.org/spec/#schemas-and-data-types
    // All time and timestamp values are stored with microsecond precision
    public static final int ICEBERG_DATETIME_SCALE_MS = 6;

    public IcebergExternalTable(long id, String name, String dbName, IcebergExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.ICEBERG_EXTERNAL_TABLE);
    }

    public String getIcebergCatalogType() {
        return ((IcebergExternalCatalog) catalog).getIcebergCatalogType();
    }

    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            objectCreated = true;
        }
    }

    @Override
    public List<Column> initSchema() {
        return HiveMetaStoreClientHelper.ugiDoAs(catalog.getConfiguration(), () -> {
            Schema schema = ((IcebergExternalCatalog) catalog).getIcebergTable(dbName, name).schema();
            List<Types.NestedField> columns = schema.columns();
            List<Column> tmpSchema = Lists.newArrayListWithCapacity(columns.size());
            for (Types.NestedField field : columns) {
                tmpSchema.add(new Column(field.name(),
                        icebergTypeToDorisType(field.type()), true, null, true, field.doc(), true,
                        schema.caseInsensitiveFindField(field.name()).fieldId()));
            }
            return tmpSchema;
        });
    }

    private Type icebergPrimitiveTypeToDorisType(org.apache.iceberg.types.Type.PrimitiveType primitive) {
        switch (primitive.typeId()) {
            case BOOLEAN:
                return Type.BOOLEAN;
            case INTEGER:
                return Type.INT;
            case LONG:
                return Type.BIGINT;
            case FLOAT:
                return Type.FLOAT;
            case DOUBLE:
                return Type.DOUBLE;
            case STRING:
            case BINARY:
            case UUID:
                return Type.STRING;
            case FIXED:
                Types.FixedType fixed = (Types.FixedType) primitive;
                return ScalarType.createCharType(fixed.length());
            case DECIMAL:
                Types.DecimalType decimal = (Types.DecimalType) primitive;
                return ScalarType.createDecimalV3Type(decimal.precision(), decimal.scale());
            case DATE:
                return ScalarType.createDateV2Type();
            case TIMESTAMP:
                return ScalarType.createDatetimeV2Type(ICEBERG_DATETIME_SCALE_MS);
            case TIME:
                return Type.UNSUPPORTED;
            default:
                throw new IllegalArgumentException("Cannot transform unknown type: " + primitive);
        }
    }

    protected Type icebergTypeToDorisType(org.apache.iceberg.types.Type type) {
        if (type.isPrimitiveType()) {
            return icebergPrimitiveTypeToDorisType((org.apache.iceberg.types.Type.PrimitiveType) type);
        }
        switch (type.typeId()) {
            case LIST:
                Types.ListType list = (Types.ListType) type;
                return ArrayType.create(icebergTypeToDorisType(list.elementType()), true);
            case MAP:
            case STRUCT:
                return Type.UNSUPPORTED;
            default:
                throw new IllegalArgumentException("Cannot transform unknown type: " + type);
        }
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        if (getIcebergCatalogType().equals("hms")) {
            THiveTable tHiveTable = new THiveTable(dbName, name, new HashMap<>());
            TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE, schema.size(), 0,
                    getName(), dbName);
            tTableDescriptor.setHiveTable(tHiveTable);
            return tTableDescriptor;
        } else {
            TIcebergTable icebergTable = new TIcebergTable(dbName, name, new HashMap<>());
            TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.ICEBERG_TABLE,
                    schema.size(), 0, getName(), dbName);
            tTableDescriptor.setIcebergTable(icebergTable);
            return tTableDescriptor;
        }
    }

    @Override
    public Optional<ColumnStatistic> getColumnStatistic(String colName) {
        makeSureInitialized();
        return HiveMetaStoreClientHelper.ugiDoAs(catalog.getConfiguration(),
                () -> StatisticsUtil.getIcebergColumnStats(colName,
                        ((IcebergExternalCatalog) catalog).getIcebergTable(dbName, name)));
    }
}
