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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.datasource.DorisTypeVisitor;

import com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.List;


/**
 * Convert Doris type to Iceberg type
 */
public class DorisTypeToIcebergType extends DorisTypeVisitor<Type> {
    private final StructType root;
    private int nextId = 0;

    public DorisTypeToIcebergType() {
        this.root = null;
    }

    public DorisTypeToIcebergType(StructType root) {
        this.root = root;
        // the root struct's fields use the first ids
        this.nextId = root.getFields().size();
    }

    private int getNextId() {
        int next = nextId;
        nextId += 1;
        return next;
    }

    @Override
    public Type struct(StructType struct, List<Type> types) {
        List<StructField> fields = struct.getFields();
        List<Types.NestedField> newFields = Lists.newArrayListWithExpectedSize(fields.size());
        boolean isRoot = root == struct;
        for (int i = 0; i < fields.size(); i++) {
            StructField field = fields.get(i);
            Type type = types.get(i);

            int id = isRoot ? i : getNextId();
            if (field.getContainsNull()) {
                newFields.add(Types.NestedField.optional(id, field.getName(), type, field.getComment()));
            } else {
                newFields.add(Types.NestedField.required(id, field.getName(), type, field.getComment()));
            }
        }
        return Types.StructType.of(newFields);
    }

    @Override
    public Type field(StructField field, Type typeResult) {
        return typeResult;
    }

    @Override
    public Type array(ArrayType array, Type elementType) {
        if (array.getContainsNull()) {
            return Types.ListType.ofOptional(getNextId(), elementType);
        } else {
            return Types.ListType.ofRequired(getNextId(), elementType);
        }
    }

    @Override
    public Type map(MapType map, Type keyType, Type valueType) {
        if (map.getIsValueContainsNull()) {
            return Types.MapType.ofOptional(getNextId(), getNextId(), keyType, valueType);
        } else {
            return Types.MapType.ofRequired(getNextId(), getNextId(), keyType, valueType);
        }
    }

    @Override
    public Type atomic(org.apache.doris.catalog.Type atomic) {
        PrimitiveType primitiveType = atomic.getPrimitiveType();
        if (primitiveType.equals(PrimitiveType.BOOLEAN)) {
            return Types.BooleanType.get();
        } else if (primitiveType.equals(PrimitiveType.INT)) {
            return Types.IntegerType.get();
        } else if (primitiveType.equals(PrimitiveType.BIGINT)) {
            return Types.LongType.get();
        } else if (primitiveType.equals(PrimitiveType.FLOAT)) {
            return Types.FloatType.get();
        } else if (primitiveType.equals(PrimitiveType.DOUBLE)) {
            return Types.DoubleType.get();
        } else if (primitiveType.equals(PrimitiveType.STRING)) {
            return Types.StringType.get();
        } else if (primitiveType.equals(PrimitiveType.DATE)
                || primitiveType.equals(PrimitiveType.DATEV2)) {
            return Types.DateType.get();
        } else if (primitiveType.equals(PrimitiveType.DECIMALV2)
                || primitiveType.isDecimalV3Type()) {
            return Types.DecimalType.of(
                    ((ScalarType) atomic).getScalarPrecision(),
                    ((ScalarType) atomic).getScalarScale());
        } else if (primitiveType.equals(PrimitiveType.DATETIME)
                || primitiveType.equals(PrimitiveType.DATETIMEV2)) {
            return Types.TimestampType.withoutZone();
        }
        // unsupported type: PrimitiveType.HLL BITMAP BINARY

        throw new UnsupportedOperationException(
                "Not a supported type: " + primitiveType);
    }
}
