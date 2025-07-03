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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Preconditions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        schemaChangeMatrix[PrimitiveType.VARCHAR.ordinal()][PrimitiveType.DATEV2.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.VARCHAR.ordinal()][PrimitiveType.STRING.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.VARCHAR.ordinal()][PrimitiveType.JSONB.ordinal()] = true;
        // could not change varchar to char cuz varchar max length is larger than char

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
        schemaChangeMatrix[PrimitiveType.ARRAY.ordinal()][PrimitiveType.ARRAY.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.STRUCT.ordinal()][PrimitiveType.STRUCT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.MAP.ordinal()][PrimitiveType.MAP.ordinal()] = true;
    }

    static boolean isSchemaChangeAllowed(Type lhs, Type rhs) {
        return schemaChangeMatrix[lhs.getPrimitiveType().ordinal()][rhs.getPrimitiveType().ordinal()];
    }

    /**
     * Used for checking type length changing.
     * Currently, type length is only meaningful for string types{@link Type#isStringType()},
     * see {@link ScalarType#len}.
     */
    public static void checkForTypeLengthChange(Type src, Type dst) throws DdlException {
        final int srcTypeLen = src.getLength();
        final int dstTypeLen = dst.getLength();
        if (srcTypeLen < 0 || dstTypeLen < 0) {
            // type length is negative means that it is meaningless, just return
            return;
        }
        if (srcTypeLen > dstTypeLen) {
            throw new DdlException(
                    String.format("Shorten type length is prohibited, srcType=%s, dstType=%s", src.toSql(),
                            dst.toSql()));
        }
    }

    // This method defines the char type
    // to support the schema-change behavior of length growth.
    // return true if the checkType and other are both char-type otherwise return false,
    // which used in checkSupportSchemaChangeForComplexType
    private static boolean checkSupportSchemaChangeForCharType(Type checkType, Type other) throws DdlException {
        if (checkType.getPrimitiveType() == PrimitiveType.VARCHAR
                && other.getPrimitiveType() == PrimitiveType.VARCHAR) {
            // currently nested types only support light schema change for internal fields, for string types,
            // only varchar can do light schema change
            checkForTypeLengthChange(checkType, other);
            return true;
        } else {
            // types equal can return true
            return checkType.equals(other);
        }
    }

    private static void validateStructFieldCompatibility(StructField originalField, StructField newField)
            throws DdlException {
        // check field name
        if (!originalField.getName().equals(newField.getName())) {
            throw new DdlException(
                    "Cannot rename struct field from '" + originalField.getName() + "' to '" + newField.getName()
                            + "'");
        }

        Type originalType = originalField.getType();
        Type newType = newField.getType();

        // deal with type change
        if (!originalType.equals(newType)) {
            checkSupportSchemaChangeForComplexType(originalType, newType, true);
        }
    }

    // This method defines the complex type which is struct, array, map if nested char-type
    // to support the schema-change behavior of length growth.
    public static void checkSupportSchemaChangeForComplexType(Type checkType, Type other, boolean nested)
            throws DdlException {
        if (checkType.isStructType() && other.isStructType()) {
            StructType thisStructType = (StructType) checkType;
            StructType otherStructType = (StructType) other;

            // now we only support add new field for struct type
            if (thisStructType.getFields().size() > otherStructType.getFields().size()) {
                throw new DdlException("Cannot reduce struct fields from " + checkType.toSql() + " to "
                        + other.toSql());
            }

            Set<String> existingNames = new HashSet<>();
            List<StructField> originalFields = thisStructType.getFields();
            List<StructField> newFields = otherStructType.getFields();

            // check each original field compatibility
            for (int i = 0; i < originalFields.size(); i++) {
                StructField originalField = originalFields.get(i);
                StructField newField = newFields.get(i);

                validateStructFieldCompatibility(originalField, newField);
                existingNames.add(originalField.getName());
            }

            // check new field name is not conflict with old field name
            for (int i = originalFields.size(); i < otherStructType.getFields().size(); i++) {
                // to check new field name is not conflict with old field name
                String newFieldName = otherStructType.getFields().get(i).getName();
                if (existingNames.contains(newFieldName)) {
                    throw new DdlException("Added struct field '" + newFieldName + "' conflicts with existing field");
                }
            }
        } else if (checkType.isArrayType()) {
            if (!other.isArrayType()) {
                throw new DdlException("Cannot change " + checkType.toSql() + " to " + other.toSql());
            }
            checkSupportSchemaChangeForComplexType(((ArrayType) checkType).getItemType(),
                    ((ArrayType) other).getItemType(), true);
        } else if (checkType.isMapType() && other.isMapType()) {
            checkSupportSchemaChangeForComplexType(((MapType) checkType).getKeyType(),
                    ((MapType) other).getKeyType(), true);
            checkSupportSchemaChangeForComplexType(((MapType) checkType).getValueType(),
                    ((MapType) other).getValueType(), true);
        } else {
            // only support char-type schema change behavior for nested complex type
            // if nested is false, we do not check return value.
            if (nested && !checkSupportSchemaChangeForCharType(checkType, other)) {
                throw new DdlException(
                        "Cannot change " + checkType.toSql() + " to " + other.toSql() + " in nested types");
            }
        }
    }

    public static void write(DataOutput out, Type type) throws IOException {
        Preconditions.checkArgument(type.isScalarType() || type.isAggStateType()
                        || type.isArrayType() || type.isMapType() || type.isStructType(),
                "not support serialize this type " + type.toSql());
        Text.writeString(out, GsonUtils.GSON.toJson(type));
    }

    public static Type read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), Type.class);
    }
}
