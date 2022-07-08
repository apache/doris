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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/TypeDef.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import java.util.ArrayList;

/**
 * Represents an anonymous type definition, e.g., used in DDL and CASTs.
 */
public class TypeDef implements ParseNode {
    private boolean isAnalyzed;
    private final Type parsedType;

    public TypeDef(Type parsedType) {
        this.parsedType = parsedType;
    }

    public static TypeDef create(PrimitiveType type) {
        return new TypeDef(ScalarType.createType(type));
    }

    public static TypeDef createDecimal(int precision, int scale) {
        return new TypeDef(ScalarType.createDecimalType(precision, scale));
    }

    public static TypeDef createDatetimeV2(int scale) {
        return new TypeDef(ScalarType.createDatetimeV2Type(scale));
    }

    public static TypeDef createTimeV2(int scale) {
        return new TypeDef(ScalarType.createTimeV2Type(scale));
    }

    public static TypeDef createVarchar(int len) {
        return new TypeDef(ScalarType.createVarchar(len));
    }

    public static TypeDef createChar(int len) {
        return new TypeDef(ScalarType.createChar(len));
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (isAnalyzed) {
            return;
        }
        // Check the max nesting depth before calling the recursive analyze() to avoid
        // a stack overflow.
        if (parsedType.exceedsMaxNestingDepth()) {
            throw new AnalysisException(String.format(
                    "Type exceeds the maximum nesting depth of %s:\n%s",
                    Type.MAX_NESTING_DEPTH, parsedType.toSql()));
        }
        analyze(parsedType);
        isAnalyzed = true;
    }

    private void analyze(Type type) throws AnalysisException {
        if (!type.isSupported()) {
            throw new AnalysisException("Unsupported data type: " + type.toSql());
        }
        if (type.isScalarType()) {
            analyzeScalarType((ScalarType) type);
        }

        if (type.isComplexType()) {
            if (type.isArrayType()) {
                Type itemType = ((ArrayType) type).getItemType();
                if (itemType instanceof ScalarType) {
                    analyzeNestedType((ScalarType) itemType);
                }
            }
            if (type.isMapType()) {
                ScalarType keyType = (ScalarType) ((MapType) type).getKeyType();
                ScalarType valueType = (ScalarType) ((MapType) type).getKeyType();
                analyzeNestedType(keyType);
                analyzeNestedType(valueType);
            }
            if (type.isStructType()) {
                ArrayList<StructField> fields = ((StructType) type).getFields();
                for (int i = 0; i < fields.size(); i++) {
                    ScalarType filedType = (ScalarType) fields.get(i).getType();
                    analyzeNestedType(filedType);
                }
            }
        }
    }

    private void analyzeNestedType(ScalarType type) throws AnalysisException {
        if (type.isNull()) {
            throw new AnalysisException("Unsupported data type: " + type.toSql());
        }
        if (!type.getPrimitiveType().isIntegerType()
                && !type.getPrimitiveType().isCharFamily()) {
            throw new AnalysisException("Array column just support INT/VARCHAR sub-type");
        }
        if (type.getPrimitiveType().isStringType()
                && !type.isAssignedStrLenInColDefinition()) {
            type.setLength(1);
        }
        analyze(type);
    }

    private void analyzeScalarType(ScalarType scalarType)
            throws AnalysisException {
        PrimitiveType type = scalarType.getPrimitiveType();
        switch (type) {
            case CHAR:
            case VARCHAR: {
                String name;
                int maxLen;
                if (type == PrimitiveType.VARCHAR) {
                    name = "VARCHAR";
                    maxLen = ScalarType.MAX_VARCHAR_LENGTH;
                } else {
                    name = "CHAR";
                    maxLen = ScalarType.MAX_CHAR_LENGTH;
                    return;
                }
                int len = scalarType.getLength();
                // len is decided by child, when it is -1.

                if (len <= 0) {
                    throw new AnalysisException(name + " size must be > 0: " + len);
                }
                if (scalarType.getLength() > maxLen) {
                    throw new AnalysisException(
                            name + " size must be <= " + maxLen + ": " + len);
                }
                break;
            }
            case DECIMALV2: {
                int precision = scalarType.decimalPrecision();
                int scale = scalarType.decimalScale();
                // precision: [1, 27]
                if (precision < 1 || precision > 27) {
                    throw new AnalysisException("Precision of decimal must between 1 and 27."
                            + " Precision was set to: " + precision + ".");
                }
                // scale: [0, 9]
                if (scale < 0 || scale > 9) {
                    throw new AnalysisException(
                            "Scale of decimal must between 0 and 9." + " Scale was set to: " + scale + ".");
                }
                break;
            }
            case DECIMAL32: {
                int decimal32Precision = scalarType.decimalPrecision();
                int decimal32Scale = scalarType.decimalScale();
                if (decimal32Precision < 1 || decimal32Precision > ScalarType.MAX_DECIMAL32_PRECISION) {
                    throw new AnalysisException("Precision of decimal must between 1 and 9."
                            + " Precision was set to: " + decimal32Precision + ".");
                }
                // scale >= 0
                if (decimal32Scale < 0) {
                    throw new AnalysisException(
                            "Scale of decimal must not be less than 0." + " Scale was set to: " + decimal32Scale + ".");
                }
                break;
            }
            case DECIMAL64: {
                int decimal64Precision = scalarType.decimalPrecision();
                int decimal64Scale = scalarType.decimalScale();
                if (decimal64Precision < 1 || decimal64Precision > ScalarType.MAX_DECIMAL64_PRECISION) {
                    throw new AnalysisException("Precision of decimal64 must between 1 and 18."
                            + " Precision was set to: " + decimal64Precision + ".");
                }
                // scale >= 0
                if (decimal64Scale < 0) {
                    throw new AnalysisException(
                            "Scale of decimal must not be less than 0." + " Scale was set to: " + decimal64Scale + ".");
                }
                break;
            }
            case DECIMAL128: {
                int decimal128Precision = scalarType.decimalPrecision();
                int decimal128Scale = scalarType.decimalScale();
                if (decimal128Precision < 1 || decimal128Precision > ScalarType.MAX_DECIMAL128_PRECISION) {
                    throw new AnalysisException("Precision of decimal128 must between 1 and 38."
                            + " Precision was set to: " + decimal128Precision + ".");
                }
                // scale >= 0
                if (decimal128Scale < 0) {
                    throw new AnalysisException("Scale of decimal must not be less than 0." + " Scale was set to: "
                            + decimal128Scale + ".");
                }
                break;
            }
            case TIMEV2:
            case DATETIMEV2: {
                int precision = scalarType.decimalPrecision();
                int scale = scalarType.decimalScale();
                // precision: [1, 27]
                if (precision != ScalarType.DATETIME_PRECISION) {
                    throw new AnalysisException("Precision of Datetime/Time must be " + ScalarType.DATETIME_PRECISION
                            + "." + " Precision was set to: " + precision + ".");
                }
                // scale: [0, 9]
                if (scale < 0 || scale > 6) {
                    throw new AnalysisException("Scale of Datetime/Time must between 0 and 6."
                            + " Scale was set to: " + scale + ".");
                }
                break;
            }
            case INVALID_TYPE:
                throw new AnalysisException("Invalid type.");
            default: break;
        }
    }

    public Type getType() {
        return parsedType;
    }

    @Override
    public String toString() {
        return parsedType.toSql();
    }

    @Override
    public String toSql() {
        return parsedType.toSql();
    }
}
