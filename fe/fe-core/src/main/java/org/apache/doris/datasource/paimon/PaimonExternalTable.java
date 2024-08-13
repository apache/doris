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

package org.apache.doris.datasource.paimon;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ExternalAnalysisTask;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class PaimonExternalTable extends ExternalTable {

    private static final Logger LOG = LogManager.getLogger(PaimonExternalTable.class);

    public PaimonExternalTable(long id, String name, String dbName, PaimonExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.PAIMON_EXTERNAL_TABLE);
    }

    public String getPaimonCatalogType() {
        return ((PaimonExternalCatalog) catalog).getCatalogType();
    }

    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            objectCreated = true;
        }
    }

    public Table getPaimonTable() {
        makeSureInitialized();
        Optional<SchemaCacheValue> schemaCacheValue = getSchemaCacheValue();
        return schemaCacheValue.map(value -> ((PaimonSchemaCacheValue) value).getPaimonTable()).orElse(null);
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        Table paimonTable = ((PaimonExternalCatalog) catalog).getPaimonTable(dbName, name);
        TableSchema schema = ((FileStoreTable) paimonTable).schema();
        List<DataField> columns = schema.fields();
        List<Column> tmpSchema = Lists.newArrayListWithCapacity(columns.size());
        for (DataField field : columns) {
            tmpSchema.add(new Column(field.name().toLowerCase(),
                    paimonTypeToDorisType(field.type()), true, null, true, field.description(), true,
                    field.id()));
        }
        return Optional.of(new PaimonSchemaCacheValue(tmpSchema, paimonTable));
    }

    private Type paimonPrimitiveTypeToDorisType(org.apache.paimon.types.DataType dataType) {
        int tsScale = 3; // default
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return Type.BOOLEAN;
            case INTEGER:
                return Type.INT;
            case BIGINT:
                return Type.BIGINT;
            case FLOAT:
                return Type.FLOAT;
            case DOUBLE:
                return Type.DOUBLE;
            case SMALLINT:
                return Type.SMALLINT;
            case TINYINT:
                return Type.TINYINT;
            case VARCHAR:
            case BINARY:
            case CHAR:
            case VARBINARY:
                return Type.STRING;
            case DECIMAL:
                DecimalType decimal = (DecimalType) dataType;
                return ScalarType.createDecimalV3Type(decimal.getPrecision(), decimal.getScale());
            case DATE:
                return ScalarType.createDateV2Type();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (dataType instanceof org.apache.paimon.types.TimestampType) {
                    tsScale = ((org.apache.paimon.types.TimestampType) dataType).getPrecision();
                    if (tsScale > 6) {
                        tsScale = 6;
                    }
                } else if (dataType instanceof org.apache.paimon.types.LocalZonedTimestampType) {
                    tsScale = ((org.apache.paimon.types.LocalZonedTimestampType) dataType).getPrecision();
                    if (tsScale > 6) {
                        tsScale = 6;
                    }
                }
                return ScalarType.createDatetimeV2Type(tsScale);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (dataType instanceof org.apache.paimon.types.LocalZonedTimestampType) {
                    tsScale = ((org.apache.paimon.types.LocalZonedTimestampType) dataType).getPrecision();
                    if (tsScale > 6) {
                        tsScale = 6;
                    }
                }
                return ScalarType.createDatetimeV2Type(tsScale);
            case ARRAY:
                ArrayType arrayType = (ArrayType) dataType;
                Type innerType = paimonPrimitiveTypeToDorisType(arrayType.getElementType());
                return org.apache.doris.catalog.ArrayType.create(innerType, true);
            case MAP:
                MapType mapType = (MapType) dataType;
                return new org.apache.doris.catalog.MapType(
                        paimonTypeToDorisType(mapType.getKeyType()), paimonTypeToDorisType(mapType.getValueType()));
            case ROW:
                RowType rowType = (RowType) dataType;
                List<DataField> fields = rowType.getFields();
                return new org.apache.doris.catalog.StructType(fields.stream()
                        .map(field -> new org.apache.doris.catalog.StructField(field.name(),
                                paimonTypeToDorisType(field.type())))
                        .collect(Collectors.toCollection(ArrayList::new)));
            case TIME_WITHOUT_TIME_ZONE:
                return Type.UNSUPPORTED;
            default:
                LOG.warn("Cannot transform unknown type: " + dataType.getTypeRoot());
                return Type.UNSUPPORTED;
        }
    }

    protected Type paimonTypeToDorisType(org.apache.paimon.types.DataType type) {
        return paimonPrimitiveTypeToDorisType(type);
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        if (PaimonExternalCatalog.PAIMON_HMS.equals(getPaimonCatalogType()) || PaimonExternalCatalog.PAIMON_FILESYSTEM
                .equals(getPaimonCatalogType())) {
            THiveTable tHiveTable = new THiveTable(dbName, name, new HashMap<>());
            TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE, schema.size(), 0,
                    getName(), dbName);
            tTableDescriptor.setHiveTable(tHiveTable);
            return tTableDescriptor;
        } else {
            throw new IllegalArgumentException("Currently only supports hms/filesystem catalog,not support :"
                    + getPaimonCatalogType());
        }
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        makeSureInitialized();
        return new ExternalAnalysisTask(info);
    }

    @Override
    public long fetchRowCount() {
        makeSureInitialized();
        long rowCount = 0;
        Optional<SchemaCacheValue> schemaCacheValue = getSchemaCacheValue();
        Table paimonTable = schemaCacheValue.map(value -> ((PaimonSchemaCacheValue) value).getPaimonTable())
                .orElse(null);
        if (paimonTable == null) {
            return -1;
        }
        List<Split> splits = paimonTable.newReadBuilder().newScan().plan().splits();
        for (Split split : splits) {
            rowCount += split.rowCount();
        }
        return rowCount;
    }
}
