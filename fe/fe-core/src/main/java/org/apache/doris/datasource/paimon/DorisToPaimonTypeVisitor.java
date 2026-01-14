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

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.DorisTypeVisitor;

import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.types.VariantType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DorisToPaimonTypeVisitor extends DorisTypeVisitor<DataType> {

    @Override
    public DataType struct(StructType struct, List<DataType> fieldResults) {
        List<StructField> fields = struct.getFields();
        List<DataField> newFields = new ArrayList<>(fields.size());
        AtomicInteger atomicInteger = new AtomicInteger(-1);
        for (int i = 0; i < fields.size(); i++) {
            StructField field = fields.get(i);
            DataType fieldType = fieldResults.get(i).copy(field.getContainsNull());
            String comment = field.getComment();
            DataField dataField = new DataField(atomicInteger.incrementAndGet(), field.getName(), fieldType, comment);
            newFields.add(dataField);
        }
        return new RowType(newFields);
    }

    @Override
    public DataType field(StructField field, DataType typeResult) {
        return typeResult;
    }

    @Override
    public DataType array(ArrayType array, DataType elementResult) {
        return new org.apache.paimon.types.ArrayType(elementResult.copy(array.getContainsNull()));
    }

    @Override
    public DataType map(MapType map, DataType keyResult, DataType valueResult) {
        return new org.apache.paimon.types.MapType(keyResult.copy(false),
            valueResult.copy(map.getIsValueContainsNull()));
    }

    @Override
    public DataType atomic(Type atomic) {
        PrimitiveType primitiveType = atomic.getPrimitiveType();
        if (primitiveType.equals(PrimitiveType.BOOLEAN)) {
            return new BooleanType();
        } else if (primitiveType.equals(PrimitiveType.INT)) {
            return new IntType();
        } else if (primitiveType.equals(PrimitiveType.BIGINT)) {
            return new BigIntType();
        } else if (primitiveType.equals(PrimitiveType.FLOAT)) {
            return new FloatType();
        } else if (primitiveType.equals(PrimitiveType.DOUBLE)) {
            return new DoubleType();
        } else if (primitiveType.isCharFamily()) {
            return new VarCharType(VarCharType.MAX_LENGTH);
        } else if (primitiveType.equals(PrimitiveType.DATE) || primitiveType.equals(PrimitiveType.DATEV2)) {
            return new DateType();
        } else if (primitiveType.equals(PrimitiveType.DECIMALV2) || primitiveType.isDecimalV3Type()) {
            return new DecimalType(((ScalarType) atomic).getScalarPrecision(), ((ScalarType) atomic).getScalarScale());
        } else if (primitiveType.equals(PrimitiveType.DATETIME) || primitiveType.equals(PrimitiveType.DATETIMEV2)) {
            return new TimestampType();
        } else if (primitiveType.isVarbinaryType()) {
            return new VarBinaryType(VarBinaryType.MAX_LENGTH);
        } else if (primitiveType.isVariantType()) {
            return new VariantType();
        }
        throw new UnsupportedOperationException("Not a supported type: " + primitiveType);
    }
}
