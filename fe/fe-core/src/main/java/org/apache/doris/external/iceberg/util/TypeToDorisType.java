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

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import java.util.List;

/**
 * Convert Iceberg types to Doris type
 */
public class TypeToDorisType extends TypeUtil.SchemaVisitor<Type> {
    public TypeToDorisType() {
    }

    @Override
    public Type schema(Schema schema, Type structType) {
        return structType;
    }

    @Override
    public Type struct(Types.StructType struct, List<Type> fieldResults) {
        throw new UnsupportedOperationException(
                String.format("Cannot convert Iceberg type[%s] to Doris type.", struct));
    }

    @Override
    public Type field(Types.NestedField field, Type fieldResult) {
        return fieldResult;
    }

    @Override
    public Type list(Types.ListType list, Type elementResult) {
        throw new UnsupportedOperationException(
                String.format("Cannot convert Iceberg type[%s] to Doris type.", list));
    }

    @Override
    public Type map(Types.MapType map, Type keyResult, Type valueResult) {
        throw new UnsupportedOperationException(
                String.format("Cannot convert Iceberg type[%s] to Doris type.", map));
    }

    @Override
    public Type primitive(org.apache.iceberg.types.Type.PrimitiveType primitive) {
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
            case DECIMAL:
                Types.DecimalType decimal = (Types.DecimalType) primitive;
                return ScalarType.createDecimalV2Type(decimal.precision(), decimal.scale());
            case DATE:
                return Type.DATE;
            case TIMESTAMP:
                return Type.DATETIME;
            case STRING:
                return Type.STRING;
            // use varchar
            case UUID:
                return Type.VARCHAR;
            // unsupported primitive type
            case TIME:
            case FIXED:
            case BINARY:
            default:
                throw new UnsupportedOperationException(String.format("Cannot convert Iceberg type[%s] to Doris type.",
                        primitive));
        }
    }
}
