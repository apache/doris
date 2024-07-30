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

package org.apache.doris.datasource.lakesoul;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ExternalAnalysisTask;
import org.apache.doris.thrift.TLakeSoulTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.entity.PartitionInfo;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import com.google.common.collect.Lists;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class LakeSoulExternalTable extends ExternalTable {
    private static final Logger LOG = LogManager.getLogger(LakeSoulExternalTable.class);
    public static final int LAKESOUL_TIMESTAMP_SCALE_MS = 6;

    public final String tableId;

    public LakeSoulExternalTable(long id, String name, String dbName, LakeSoulExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.LAKESOUl_EXTERNAL_TABLE);
        tableId = getLakeSoulTableInfo().getTableId();
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        makeSureInitialized();
        return new ExternalAnalysisTask(info);
    }

    private Type arrowFiledToDorisType(Field field) {
        ArrowType dt = field.getType();
        if (dt instanceof ArrowType.Bool) {
            return Type.BOOLEAN;
        } else if (dt instanceof ArrowType.Int) {
            ArrowType.Int type = (ArrowType.Int) dt;
            switch (type.getBitWidth()) {
                case 8:
                    return Type.TINYINT;
                case 16:
                    return Type.SMALLINT;
                case 32:
                    return Type.INT;
                case 64:
                    return Type.BIGINT;
                default:
                    throw new IllegalArgumentException("Invalid integer bit width: "
                        + type.getBitWidth()
                        + " for LakeSoul table: "
                        + getTableIdentifier());
            }
        } else if (dt instanceof ArrowType.FloatingPoint) {
            ArrowType.FloatingPoint type = (ArrowType.FloatingPoint) dt;
            switch (type.getPrecision()) {
                case SINGLE:
                    return Type.FLOAT;
                case DOUBLE:
                    return Type.DOUBLE;
                default:
                    throw new IllegalArgumentException("Invalid floating point precision: "
                        + type.getPrecision()
                        + " for LakeSoul table: "
                        + getTableIdentifier());
            }
        } else if (dt instanceof ArrowType.Utf8) {
            return Type.STRING;
        } else if (dt instanceof ArrowType.Decimal) {
            ArrowType.Decimal decimalType = (ArrowType.Decimal) dt;
            return ScalarType.createDecimalType(PrimitiveType.DECIMAL64, decimalType.getPrecision(),
                decimalType.getScale());
        } else if (dt instanceof ArrowType.Date) {
            return ScalarType.createDateV2Type();
        } else if (dt instanceof ArrowType.Timestamp) {
            ArrowType.Timestamp tsType = (ArrowType.Timestamp) dt;
            int scale = LAKESOUL_TIMESTAMP_SCALE_MS;
            switch (tsType.getUnit()) {
                case SECOND:
                    scale = 0;
                    break;
                case MILLISECOND:
                    scale = 3;
                    break;
                case MICROSECOND:
                    scale = 6;
                    break;
                case NANOSECOND:
                    scale = 9;
                    break;
                default:
                    break;
            }
            return ScalarType.createDatetimeV2Type(scale);
        } else if (dt instanceof ArrowType.List) {
            List<Field> children = field.getChildren();
            Preconditions.checkArgument(children.size() == 1,
                    "Lists have one child Field. Found: %s", children.isEmpty() ? "none" : children);
            return ArrayType.create(arrowFiledToDorisType(children.get(0)), children.get(0).isNullable());
        } else if (dt instanceof ArrowType.Struct) {
            List<Field> children = field.getChildren();
            return new StructType(children.stream().map(this::arrowFiledToDorisType).collect(Collectors.toList()));
        }
        throw new IllegalArgumentException("Cannot transform type "
            + dt
            + " to doris type"
            + " for LakeSoul table "
            + getTableIdentifier());
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        TLakeSoulTable tLakeSoulTable = new TLakeSoulTable();
        tLakeSoulTable.setDbName(dbName);
        tLakeSoulTable.setTableName(name);
        tLakeSoulTable.setProperties(new HashMap<>());
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE, schema.size(), 0,
                getName(), dbName);
        tTableDescriptor.setLakesoulTable(tLakeSoulTable);
        return tTableDescriptor;

    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        TableInfo tableInfo = ((LakeSoulExternalCatalog) catalog).getLakeSoulTable(dbName, name);
        String tableSchema = tableInfo.getTableSchema();
        DBUtil.TablePartitionKeys partitionKeys = DBUtil.parseTableInfoPartitions(tableInfo.getPartitions());
        Schema schema;
        LOG.info("tableSchema={}", tableSchema);
        try {
            schema = Schema.fromJSON(tableSchema);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<Column> tmpSchema = Lists.newArrayListWithCapacity(schema.getFields().size());
        for (Field field : schema.getFields()) {
            boolean isKey =
                    partitionKeys.primaryKeys.contains(field.getName())
                    || partitionKeys.rangeKeys.contains(field.getName());
            tmpSchema.add(new Column(field.getName(), arrowFiledToDorisType(field),
                    isKey,
                    null, field.isNullable(),
                    field.getMetadata().getOrDefault("comment", null),
                    true, schema.getFields().indexOf(field)));
        }
        return Optional.of(new SchemaCacheValue(tmpSchema));
    }

    public TableInfo getLakeSoulTableInfo() {
        return ((LakeSoulExternalCatalog) catalog).getLakeSoulTable(dbName, name);
    }

    public List<PartitionInfo> listPartitionInfo() {
        return ((LakeSoulExternalCatalog) catalog).listPartitionInfo(tableId);
    }

    public String tablePath() {
        return ((LakeSoulExternalCatalog) catalog).getLakeSoulTable(dbName, name).getTablePath();
    }

    public Map<String, String> getHadoopProperties() {
        return catalog.getCatalogProperty().getHadoopProperties();
    }

    public String getTableIdentifier() {
        return dbName + "." + name;
    }
}
