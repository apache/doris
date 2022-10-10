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

package org.apache.doris.catalog;

import org.apache.doris.common.io.Text;

import com.google.common.base.Preconditions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 这个是对Column类型的一个封装，对于大多数类型，primitive type足够了，这里有两个例外需要用到这个信息
 * 1. 对于decimal，character这种有一些附加信息的
 * 2. 如果在未来需要增加嵌套类型，那么这个ColumnType就是必须的了
 */
public abstract class ColumnType {
    private static Boolean[][] schemaChangeMatrix;

    static {
        schemaChangeMatrix = new Boolean[PrimitiveType.BINARY.ordinal() + 1][PrimitiveType.BINARY.ordinal() + 1];

        for (int i = 0; i < schemaChangeMatrix.length; i++) {
            for (int j = 0; j < schemaChangeMatrix[i].length; j++) {
                schemaChangeMatrix[i][j] = i == j;
            }
        }

        schemaChangeMatrix[PrimitiveType.TINYINT.ordinal()][PrimitiveType.SMALLINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.TINYINT.ordinal()][PrimitiveType.INT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.TINYINT.ordinal()][PrimitiveType.BIGINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.TINYINT.ordinal()][PrimitiveType.LARGEINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.TINYINT.ordinal()][PrimitiveType.FLOAT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.TINYINT.ordinal()][PrimitiveType.DOUBLE.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.TINYINT.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.TINYINT.ordinal()][PrimitiveType.STRING.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.SMALLINT.ordinal()][PrimitiveType.INT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.SMALLINT.ordinal()][PrimitiveType.BIGINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.SMALLINT.ordinal()][PrimitiveType.LARGEINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.SMALLINT.ordinal()][PrimitiveType.FLOAT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.SMALLINT.ordinal()][PrimitiveType.DOUBLE.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.SMALLINT.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.SMALLINT.ordinal()][PrimitiveType.STRING.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.INT.ordinal()][PrimitiveType.BIGINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.INT.ordinal()][PrimitiveType.LARGEINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.INT.ordinal()][PrimitiveType.FLOAT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.INT.ordinal()][PrimitiveType.DOUBLE.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.INT.ordinal()][PrimitiveType.DATE.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.INT.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.INT.ordinal()][PrimitiveType.STRING.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.BIGINT.ordinal()][PrimitiveType.LARGEINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.BIGINT.ordinal()][PrimitiveType.FLOAT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.BIGINT.ordinal()][PrimitiveType.DOUBLE.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.BIGINT.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.BIGINT.ordinal()][PrimitiveType.STRING.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.LARGEINT.ordinal()][PrimitiveType.FLOAT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.LARGEINT.ordinal()][PrimitiveType.DOUBLE.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.LARGEINT.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.LARGEINT.ordinal()][PrimitiveType.STRING.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.FLOAT.ordinal()][PrimitiveType.DOUBLE.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.FLOAT.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.FLOAT.ordinal()][PrimitiveType.STRING.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.DOUBLE.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DOUBLE.ordinal()][PrimitiveType.STRING.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.VARCHAR.ordinal()][PrimitiveType.TINYINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.VARCHAR.ordinal()][PrimitiveType.SMALLINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.VARCHAR.ordinal()][PrimitiveType.INT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.VARCHAR.ordinal()][PrimitiveType.BIGINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.VARCHAR.ordinal()][PrimitiveType.LARGEINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.VARCHAR.ordinal()][PrimitiveType.FLOAT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.VARCHAR.ordinal()][PrimitiveType.DOUBLE.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.VARCHAR.ordinal()][PrimitiveType.DATE.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.VARCHAR.ordinal()][PrimitiveType.STRING.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.VARCHAR.ordinal()][PrimitiveType.JSONB.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.STRING.ordinal()][PrimitiveType.JSONB.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.JSONB.ordinal()][PrimitiveType.STRING.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.JSONB.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.CHAR.ordinal()][PrimitiveType.TINYINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.CHAR.ordinal()][PrimitiveType.SMALLINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.CHAR.ordinal()][PrimitiveType.INT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.CHAR.ordinal()][PrimitiveType.BIGINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.CHAR.ordinal()][PrimitiveType.LARGEINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.CHAR.ordinal()][PrimitiveType.FLOAT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.CHAR.ordinal()][PrimitiveType.DOUBLE.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.CHAR.ordinal()][PrimitiveType.DATE.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.CHAR.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.CHAR.ordinal()][PrimitiveType.CHAR.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.CHAR.ordinal()][PrimitiveType.STRING.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.DECIMALV2.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMALV2.ordinal()][PrimitiveType.STRING.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMALV2.ordinal()][PrimitiveType.DECIMAL32.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMALV2.ordinal()][PrimitiveType.DECIMAL64.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMALV2.ordinal()][PrimitiveType.DECIMAL128.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.DECIMAL32.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMAL32.ordinal()][PrimitiveType.STRING.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMAL32.ordinal()][PrimitiveType.DECIMALV2.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMAL32.ordinal()][PrimitiveType.DECIMAL64.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMAL32.ordinal()][PrimitiveType.DECIMAL128.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.DECIMAL64.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMAL64.ordinal()][PrimitiveType.STRING.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMAL64.ordinal()][PrimitiveType.DECIMAL32.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMAL64.ordinal()][PrimitiveType.DECIMALV2.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMAL64.ordinal()][PrimitiveType.DECIMAL128.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.DECIMAL128.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMAL128.ordinal()][PrimitiveType.STRING.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMAL128.ordinal()][PrimitiveType.DECIMAL32.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMAL128.ordinal()][PrimitiveType.DECIMAL64.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DECIMAL128.ordinal()][PrimitiveType.DECIMALV2.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.DATETIME.ordinal()][PrimitiveType.DATE.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DATE.ordinal()][PrimitiveType.DATETIME.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DATETIME.ordinal()][PrimitiveType.DATEV2.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DATE.ordinal()][PrimitiveType.DATETIMEV2.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DATETIME.ordinal()][PrimitiveType.DATETIMEV2.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DATE.ordinal()][PrimitiveType.DATEV2.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.DATETIMEV2.ordinal()][PrimitiveType.DATE.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DATEV2.ordinal()][PrimitiveType.DATETIME.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DATETIMEV2.ordinal()][PrimitiveType.DATEV2.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DATEV2.ordinal()][PrimitiveType.DATETIMEV2.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DATETIMEV2.ordinal()][PrimitiveType.DATETIME.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.DATEV2.ordinal()][PrimitiveType.DATE.ordinal()] = true;

        // we should support schema change between different precision
        schemaChangeMatrix[PrimitiveType.DATETIMEV2.ordinal()][PrimitiveType.DATETIMEV2.ordinal()] = true;

        // Currently, we do not support schema change between complex types with subtypes.
        schemaChangeMatrix[PrimitiveType.ARRAY.ordinal()][PrimitiveType.ARRAY.ordinal()] = false;
        schemaChangeMatrix[PrimitiveType.STRUCT.ordinal()][PrimitiveType.STRUCT.ordinal()] = false;
        schemaChangeMatrix[PrimitiveType.MAP.ordinal()][PrimitiveType.MAP.ordinal()] = false;
    }

    static boolean isSchemaChangeAllowed(Type lhs, Type rhs) {
        return schemaChangeMatrix[lhs.getPrimitiveType().ordinal()][rhs.getPrimitiveType().ordinal()];
    }

    public static void write(DataOutput out, Type type) throws IOException {
        Preconditions.checkArgument(type.isScalarType() || type.isArrayType(),
                "only support scalar type and array serialization");
        if (type.isScalarType()) {
            ScalarType scalarType = (ScalarType) type;
            Text.writeString(out, scalarType.getPrimitiveType().name());
            out.writeInt(scalarType.getScalarScale());
            out.writeInt(scalarType.getScalarPrecision());
            out.writeInt(scalarType.getLength());
            // Actually, varcharLimit need not to write here, write true to back compatible
            out.writeBoolean(true);
        } else if (type.isArrayType()) {
            ArrayType arrayType = (ArrayType) type;
            Text.writeString(out, arrayType.getPrimitiveType().name());
            write(out, arrayType.getItemType());
            out.writeBoolean(arrayType.getContainsNull());
        }
    }

    public static Type read(DataInput in) throws IOException {
        PrimitiveType primitiveType = PrimitiveType.valueOf(Text.readString(in));
        if (primitiveType == PrimitiveType.ARRAY) {
            Type itermType = read(in);
            boolean containsNull = in.readBoolean();
            return ArrayType.create(itermType, containsNull);
        } else {
            int scale = in.readInt();
            int precision = in.readInt();
            int len = in.readInt();
            // Useless, just for back compatible
            in.readBoolean();
            return ScalarType.createType(primitiveType, len, precision, scale);
        }
    }
}
