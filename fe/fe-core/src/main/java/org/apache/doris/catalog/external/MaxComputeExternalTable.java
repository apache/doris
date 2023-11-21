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
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.MaxComputeExternalCatalog;
import org.apache.doris.planner.external.TablePartitionValues;
import org.apache.doris.planner.external.TablePartitionValues.TablePartitionKey;
import org.apache.doris.thrift.TMCTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.Table;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.CharTypeInfo;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.VarcharTypeInfo;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * MaxCompute external table.
 */
public class MaxComputeExternalTable extends ExternalTable {

    private static final Cache<TablePartitionKey, TablePartitionValues> partitionValuesCache;
    private Table odpsTable;
    private List<String> partitionSpecs;
    private Map<String, Column> partitionNameToColumns;
    private List<Type> partitionTypes;

    static {
        partitionValuesCache = CacheBuilder.newBuilder().maximumSize(Config.max_hive_partition_cache_num)
                .expireAfterAccess(Config.external_cache_expire_time_minutes_after_access, TimeUnit.MINUTES)
                .build();
    }

    public MaxComputeExternalTable(long id, String name, String dbName, MaxComputeExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.MAX_COMPUTE_EXTERNAL_TABLE);
    }

    @Override
    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            odpsTable = ((MaxComputeExternalCatalog) catalog).getClient().tables().get(name);
            objectCreated = true;
        }
    }

    @Override
    public Set<String> getPartitionNames() {
        makeSureInitialized();
        return partitionNameToColumns.keySet();
    }

    public List<Column> getPartitionColumns() {
        makeSureInitialized();
        return new ArrayList<>(partitionNameToColumns.values());
    }

    public TablePartitionValues getPartitionValues() {
        makeSureInitialized();
        // Make sure to call it after initSchema() completes
        String projectName = odpsTable.getProject();
        String tableName = odpsTable.getName();
        TablePartitionKey tablePartitionKey = new TablePartitionKey(projectName, tableName, partitionTypes);
        try {
            return partitionValuesCache.get(tablePartitionKey, () -> {
                TablePartitionValues partitionValues = new TablePartitionValues();
                partitionValues.addPartitions(partitionSpecs,
                        partitionSpecs.stream()
                                .map(p -> parsePartitionValues(new ArrayList<>(getPartitionNames()), p))
                                .collect(Collectors.toList()),
                        partitionTypes);
                return partitionValues;
            });
        } catch (ExecutionException e) {
            throw new RuntimeException("Fail to load partition values for table:"
                    + " '" + projectName + "." + tableName + "'");
        }
    }

    private static List<String> parsePartitionValues(List<String> partitionColumns, String partitionPath) {
        String[] partitionFragments = partitionPath.split("/");
        if (partitionFragments.length != partitionColumns.size()) {
            throw new RuntimeException("Failed to parse partition values of path: " + partitionPath);
        }
        List<String> partitionValues = new ArrayList<>(partitionFragments.length);
        for (int i = 0; i < partitionFragments.length; i++) {
            String prefix = partitionColumns.get(i) + "=";
            if (partitionFragments[i].startsWith(prefix)) {
                partitionValues.add(partitionFragments[i].substring(prefix.length()));
            } else {
                partitionValues.add(partitionFragments[i]);
            }
        }
        return partitionValues;
    }

    @Override
    public List<Column> initSchema() {
        // this method will be called at semantic parsing.
        makeSureInitialized();
        List<com.aliyun.odps.Column> columns = odpsTable.getSchema().getColumns();
        List<Column> result = Lists.newArrayListWithCapacity(columns.size());
        for (com.aliyun.odps.Column field : columns) {
            result.add(new Column(field.getName(), mcTypeToDorisType(field.getTypeInfo()), true, null,
                    true, field.getComment(), true, -1));
        }
        initTablePartitions();
        result.addAll(partitionNameToColumns.values());
        return result;
    }

    private void initTablePartitions() {
        List<com.aliyun.odps.Column> partitionColumns = odpsTable.getSchema().getPartitionColumns();
        if (!partitionColumns.isEmpty()) {
            partitionSpecs = odpsTable.getPartitions().stream()
                    .map(e -> e.getPartitionSpec().toString(false, true))
                    .collect(Collectors.toList());
        } else {
            partitionSpecs = ImmutableList.of();
        }
        // sort partition columns to align partitionTypes and partitionName.
        partitionNameToColumns = new LinkedHashMap<>();
        for (com.aliyun.odps.Column partColumn : partitionColumns) {
            Column dorisCol = new Column(partColumn.getName(),
                    mcTypeToDorisType(partColumn.getTypeInfo()), true, null,
                    true, partColumn.getComment(), true, -1);
            partitionNameToColumns.put(dorisCol.getName(), dorisCol);
        }
        partitionTypes = partitionNameToColumns.values()
                .stream()
                .map(Column::getType)
                .collect(Collectors.toList());
    }

    private Type mcTypeToDorisType(TypeInfo typeInfo) {
        OdpsType odpsType = typeInfo.getOdpsType();
        switch (odpsType) {
            case VOID: {
                return Type.NULL;
            }
            case BOOLEAN: {
                return Type.BOOLEAN;
            }
            case TINYINT: {
                return Type.TINYINT;
            }
            case SMALLINT: {
                return Type.SMALLINT;
            }
            case INT: {
                return Type.INT;
            }
            case BIGINT: {
                return Type.BIGINT;
            }
            case CHAR: {
                CharTypeInfo charType = (CharTypeInfo) typeInfo;
                return ScalarType.createChar(charType.getLength());
            }
            case STRING: {
                return ScalarType.createStringType();
            }
            case VARCHAR: {
                VarcharTypeInfo varcharType = (VarcharTypeInfo) typeInfo;
                return ScalarType.createVarchar(varcharType.getLength());
            }
            case JSON: {
                return Type.UNSUPPORTED;
                // return Type.JSONB;
            }
            case FLOAT: {
                return Type.FLOAT;
            }
            case DOUBLE: {
                return Type.DOUBLE;
            }
            case DECIMAL: {
                DecimalTypeInfo decimal = (DecimalTypeInfo) typeInfo;
                return ScalarType.createDecimalV3Type(decimal.getPrecision(), decimal.getScale());
            }
            case DATE: {
                return ScalarType.createDateV2Type();
            }
            case DATETIME:
            case TIMESTAMP: {
                return ScalarType.createDatetimeV2Type(3);
            }
            case ARRAY: {
                ArrayTypeInfo arrayType = (ArrayTypeInfo) typeInfo;
                Type innerType = mcTypeToDorisType(arrayType.getElementTypeInfo());
                return ArrayType.create(innerType, true);
            }
            case MAP: {
                MapTypeInfo mapType = (MapTypeInfo) typeInfo;
                return new MapType(mcTypeToDorisType(mapType.getKeyTypeInfo()),
                        mcTypeToDorisType(mapType.getValueTypeInfo()));
            }
            case STRUCT: {
                ArrayList<StructField> fields = new ArrayList<>();
                StructTypeInfo structType = (StructTypeInfo) typeInfo;
                List<String> fieldNames = structType.getFieldNames();
                List<TypeInfo> fieldTypeInfos = structType.getFieldTypeInfos();
                for (int i = 0; i < structType.getFieldCount(); i++) {
                    Type innerType = mcTypeToDorisType(fieldTypeInfos.get(i));
                    fields.add(new StructField(fieldNames.get(i), innerType));
                }
                return new StructType(fields);
            }
            case BINARY:
            case INTERVAL_DAY_TIME:
            case INTERVAL_YEAR_MONTH:
                return Type.UNSUPPORTED;
            default:
                throw new IllegalArgumentException("Cannot transform unknown type: " + odpsType);
        }
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        TMCTable tMcTable = new TMCTable();
        MaxComputeExternalCatalog mcCatalog = (MaxComputeExternalCatalog) catalog;
        tMcTable.setRegion(mcCatalog.getRegion());
        tMcTable.setAccessKey(mcCatalog.getAccessKey());
        tMcTable.setSecretKey(mcCatalog.getSecretKey());
        tMcTable.setPublicAccess(String.valueOf(mcCatalog.enablePublicAccess()));
        // use mc project as dbName
        tMcTable.setProject(dbName);
        tMcTable.setTable(name);
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.MAX_COMPUTE_TABLE,
                schema.size(), 0, getName(), dbName);
        tTableDescriptor.setMcTable(tMcTable);
        return tTableDescriptor;
    }

    public Table getOdpsTable() {
        return odpsTable;
    }

    @Override
    public String getMysqlType() {
        return "BASE TABLE";
    }
}

