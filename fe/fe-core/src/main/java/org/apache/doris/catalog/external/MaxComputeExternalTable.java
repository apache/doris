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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.Predicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.MaxComputeExternalCatalog;
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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;

/**
 * MaxCompute external table.
 */
public class MaxComputeExternalTable extends ExternalTable {

    private Table odpsTable;
    private Set<String> partitionKeys;
    private String partitionSpec;

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
    public List<Column> initSchema() {
        makeSureInitialized();
        List<com.aliyun.odps.Column> columns = odpsTable.getSchema().getColumns();
        List<Column> result = Lists.newArrayListWithCapacity(columns.size());
        for (com.aliyun.odps.Column field : columns) {
            result.add(new Column(field.getName(), mcTypeToDorisType(field.getTypeInfo()), true, null,
                    true, field.getComment(), true, -1));
        }
        List<com.aliyun.odps.Column> partitionColumns = odpsTable.getSchema().getPartitionColumns();
        partitionKeys = new HashSet<>();
        for (com.aliyun.odps.Column partColumn : partitionColumns) {
            result.add(new Column(partColumn.getName(), mcTypeToDorisType(partColumn.getTypeInfo()), true, null,
                    true, partColumn.getComment(), true, -1));
            partitionKeys.add(partColumn.getName());
        }
        return result;
    }

    public Optional<String> getPartitionSpec(List<Expr> conjuncts) {
        if (!partitionKeys.isEmpty()) {
            if (conjuncts.isEmpty()) {
                throw new IllegalArgumentException("Max Compute partition table need partition predicate.");
            }
            // recreate partitionSpec when conjuncts is changed.
            List<String> partitionConjuncts = parsePartitionConjuncts(conjuncts, partitionKeys);
            StringJoiner partitionSpec = new StringJoiner(",");
            partitionConjuncts.forEach(partitionSpec::add);
            this.partitionSpec = partitionSpec.toString();
            return Optional.of(this.partitionSpec);
        }
        return Optional.empty();
    }

    private static List<String> parsePartitionConjuncts(List<Expr> conjuncts, Set<String> partitionKeys) {
        List<String> partitionConjuncts = new ArrayList<>();
        Set<Predicate> predicates = Sets.newHashSet();
        for (Expr conjunct : conjuncts) {
            // collect depart predicate
            conjunct.collect(BinaryPredicate.class, predicates);
            conjunct.collect(InPredicate.class, predicates);
        }
        Map<String, Predicate> slotToConjuncts = new HashMap<>();
        for (Predicate predicate : predicates) {
            List<SlotRef> slotRefs = new ArrayList<>();
            if (predicate instanceof BinaryPredicate) {
                if (((BinaryPredicate) predicate).getOp() != BinaryPredicate.Operator.EQ) {
                    // max compute only support the EQ operator: pt='pt-value'
                    continue;
                }
                // BinaryPredicate has one left slotRef, and partition value not slotRef
                predicate.collect(SlotRef.class, slotRefs);
                slotToConjuncts.put(slotRefs.get(0).getColumnName(), predicate);
            } else if (predicate instanceof InPredicate) {
                predicate.collect(SlotRef.class, slotRefs);
                slotToConjuncts.put(slotRefs.get(0).getColumnName(), predicate);
            }
        }
        for (String partitionKey : partitionKeys) {
            Predicate partitionPredicate = slotToConjuncts.get(partitionKey);
            if (partitionPredicate == null) {
                continue;
            }
            if (partitionPredicate instanceof InPredicate) {
                List<Expr> inList = ((InPredicate) partitionPredicate).getListChildren();
                for (Expr expr : inList) {
                    String partitionConjunct = partitionKey + "=" + expr.toSql();
                    partitionConjuncts.add(partitionConjunct.replace("`", ""));
                }
            } else {
                String partitionConjunct = partitionPredicate.toSql();
                partitionConjuncts.add(partitionConjunct.replace("`", ""));
            }
        }
        return partitionConjuncts;
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
        tMcTable.setPartitionSpec(this.partitionSpec);
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

