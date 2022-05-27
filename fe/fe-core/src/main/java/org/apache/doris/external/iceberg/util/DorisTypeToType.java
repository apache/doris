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

package org.apache.doris.external.iceberg.util;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.List;

/**
 * Convert Doris type to Iceberg type
 */
public class DorisTypeToType extends DorisTypeVisitor<Type> {
    private final StructType root;
    private int nextId = 0;

    public DorisTypeToType() {
        this.root = null;
    }

    public DorisTypeToType(StructType root) {
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
        throw new UnsupportedOperationException(
                "Not a supported type: " + struct.toSql(0));
    }

    @Override
    public Type field(StructField field, Type typeResult) {
        return typeResult;
    }

    @Override
    public Type array(ArrayType array, Type elementType) {
        throw new UnsupportedOperationException(
                "Not a supported type: " + array.toSql(0));
    }

    @Override
    public Type map(MapType map, Type keyType, Type valueType) {
        throw new UnsupportedOperationException(
                "Not a supported type: " + map.toSql(0));
    }

    @Override
    public Type atomic(org.apache.doris.catalog.Type atomic) {
        if (atomic.getPrimitiveType().equals(PrimitiveType.BOOLEAN)) {
            return Types.BooleanType.get();
        } else if (atomic.getPrimitiveType().equals(PrimitiveType.TINYINT)
                || atomic.getPrimitiveType().equals(PrimitiveType.SMALLINT)
                || atomic.getPrimitiveType().equals(PrimitiveType.INT)) {
            return Types.IntegerType.get();
        } else if (atomic.getPrimitiveType().equals(PrimitiveType.BIGINT)
                || atomic.getPrimitiveType().equals(PrimitiveType.LARGEINT)) {
            return Types.LongType.get();
        } else if (atomic.getPrimitiveType().equals(PrimitiveType.FLOAT)) {
            return Types.FloatType.get();
        } else if (atomic.getPrimitiveType().equals(PrimitiveType.DOUBLE)) {
            return Types.DoubleType.get();
        } else if (atomic.getPrimitiveType().equals(PrimitiveType.CHAR)
                || atomic.getPrimitiveType().equals(PrimitiveType.VARCHAR)) {
            return Types.StringType.get();
        } else if (atomic.getPrimitiveType().equals(PrimitiveType.DATE)) {
            return Types.DateType.get();
        } else if (atomic.getPrimitiveType().equals(PrimitiveType.TIME)) {
            return Types.TimeType.get();
        } else if (atomic.getPrimitiveType().equals(PrimitiveType.DECIMALV2)
                || atomic.getPrimitiveType().equals(PrimitiveType.DECIMALV2)) {
            return Types.DecimalType.of(
                    ((ScalarType) atomic).getScalarPrecision(),
                    ((ScalarType) atomic).getScalarScale());
        } else if (atomic.getPrimitiveType().equals(PrimitiveType.DATETIME)) {
            return Types.TimestampType.withZone();
        }
        // unsupported type: PrimitiveType.HLL BITMAP BINARY

        throw new UnsupportedOperationException(
                "Not a supported type: " + atomic.getPrimitiveType());
    }
}
