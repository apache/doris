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

package org.apache.doris.datasource.maxcompute;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.TablePartitionValues;
import org.apache.doris.thrift.TMCTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.Table;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.CharTypeInfo;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.VarcharTypeInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MaxCompute external table.
 */
public class MaxComputeExternalTable extends ExternalTable {
    public MaxComputeExternalTable(long id, String name, String dbName, MaxComputeExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.MAX_COMPUTE_EXTERNAL_TABLE);
    }

    @Override
    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            objectCreated = true;
        }
    }

    public long getTotalRows() throws TunnelException {
        // use for non-partitioned table
        // partition table will read the entire partition on FE so get total rows is unnecessary.
        makeSureInitialized();
        MaxComputeMetadataCache metadataCache = Env.getCurrentEnv().getExtMetaCacheMgr()
                .getMaxComputeMetadataCache(catalog.getId());
        MaxComputeExternalCatalog mcCatalog = ((MaxComputeExternalCatalog) catalog);
        return metadataCache.getCachedRowCount(dbName, name, null, key -> {
            try {
                return loadRowCount(mcCatalog, key);
            } catch (TunnelException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private long loadRowCount(MaxComputeExternalCatalog catalog, MaxComputeCacheKey key) throws TunnelException {
        return catalog.getTableTunnel()
                .getDownloadSession(key.getDbName(), key.getTblName(), null)
                .getRecordCount();
    }

    @Override
    public Set<String> getPartitionNames() {
        makeSureInitialized();
        Optional<SchemaCacheValue> schemaCacheValue = getSchemaCacheValue();
        return schemaCacheValue.map(value -> ((MaxComputeSchemaCacheValue) value).getPartitionColNames())
                .orElse(Collections.emptySet());
    }

    public List<Column> getPartitionColumns() {
        makeSureInitialized();
        Optional<SchemaCacheValue> schemaCacheValue = getSchemaCacheValue();
        return schemaCacheValue.map(value -> ((MaxComputeSchemaCacheValue) value).getPartitionColumns())
                .orElse(Collections.emptyList());
    }

    public TablePartitionValues getPartitionValues() {
        makeSureInitialized();
        Optional<SchemaCacheValue> schemaCacheValue = getSchemaCacheValue();
        if (!schemaCacheValue.isPresent()) {
            return new TablePartitionValues();
        }
        Table odpsTable = ((MaxComputeSchemaCacheValue) schemaCacheValue.get()).getOdpsTable();
        String projectName = odpsTable.getProject();
        String tableName = odpsTable.getName();
        MaxComputeMetadataCache metadataCache = Env.getCurrentEnv().getExtMetaCacheMgr()
                .getMaxComputeMetadataCache(catalog.getId());
        return metadataCache.getCachedPartitionValues(
                new MaxComputeCacheKey(projectName, tableName),
                key -> loadPartitionValues((MaxComputeSchemaCacheValue) schemaCacheValue.get()));
    }

    private TablePartitionValues loadPartitionValues(MaxComputeSchemaCacheValue schemaCacheValue) {
        List<String> partitionSpecs = schemaCacheValue.getPartitionSpecs();
        List<Type> partitionTypes = schemaCacheValue.getPartitionTypes();
        TablePartitionValues partitionValues = new TablePartitionValues();
        partitionValues.addPartitions(partitionSpecs,
                partitionSpecs.stream()
                        .map(p -> parsePartitionValues(new ArrayList<>(getPartitionNames()), p))
                        .collect(Collectors.toList()),
                partitionTypes);
        return partitionValues;
    }

    /**
     * parse all values from partitionPath to a single list.
     *
     * @param partitionColumns partitionColumns can contain the part1,part2,part3...
     * @param partitionPath partitionPath format is like the 'part1=123/part2=abc/part3=1bc'
     * @return all values of partitionPath
     */
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
    public Optional<SchemaCacheValue> initSchema() {
        // this method will be called at semantic parsing.
        makeSureInitialized();
        Table odpsTable = ((MaxComputeExternalCatalog) catalog).getClient().tables().get(name);
        List<com.aliyun.odps.Column> columns = odpsTable.getSchema().getColumns();
        List<Column> schema = Lists.newArrayListWithCapacity(columns.size());
        for (com.aliyun.odps.Column field : columns) {
            schema.add(new Column(field.getName(), mcTypeToDorisType(field.getTypeInfo()), true, null,
                    true, field.getComment(), true, -1));
        }

        List<com.aliyun.odps.Column> partitionColumns = odpsTable.getSchema().getPartitionColumns();
        List<String> partitionSpecs;
        if (!partitionColumns.isEmpty()) {
            partitionSpecs = odpsTable.getPartitions().stream()
                    .map(e -> e.getPartitionSpec().toString(false, true))
                    .collect(Collectors.toList());
        } else {
            partitionSpecs = ImmutableList.of();
        }
        // sort partition columns to align partitionTypes and partitionName.
        Map<String, Column> partitionNameToColumns = Maps.newHashMap();
        for (com.aliyun.odps.Column partColumn : partitionColumns) {
            Column dorisCol = new Column(partColumn.getName(),
                    mcTypeToDorisType(partColumn.getTypeInfo()), true, null,
                    true, partColumn.getComment(), true, -1);
            partitionNameToColumns.put(dorisCol.getName(), dorisCol);
        }
        List<Type> partitionTypes = partitionNameToColumns.values()
                .stream()
                .map(Column::getType)
                .collect(Collectors.toList());

        schema.addAll(partitionNameToColumns.values());
        return Optional.of(new MaxComputeSchemaCacheValue(schema, odpsTable, partitionSpecs, partitionNameToColumns,
                partitionTypes));
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
        MaxComputeExternalCatalog mcCatalog = ((MaxComputeExternalCatalog) catalog);
        tMcTable.setRegion(mcCatalog.getRegion());
        tMcTable.setAccessKey(mcCatalog.getAccessKey());
        tMcTable.setSecretKey(mcCatalog.getSecretKey());
        tMcTable.setOdpsUrl(mcCatalog.getOdpsUrl());
        tMcTable.setTunnelUrl(mcCatalog.getTunnelUrl());
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
        makeSureInitialized();
        Optional<SchemaCacheValue> schemaCacheValue = getSchemaCacheValue();
        return schemaCacheValue.map(value -> ((MaxComputeSchemaCacheValue) value).getOdpsTable())
                .orElse(null);
    }
}
