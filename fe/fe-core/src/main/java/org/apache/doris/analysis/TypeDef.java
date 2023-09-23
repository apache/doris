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
import org.apache.doris.common.Config;
import org.apache.doris.thrift.TColumnDesc;
import org.apache.doris.thrift.TPrimitiveType;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents an anonymous type definition, e.g., used in DDL and CASTs.
 */
public class TypeDef implements ParseNode {
    private boolean isAnalyzed;
    private final Type parsedType;
    private boolean isNullable = false;

    public TypeDef(Type parsedType) {
        this.parsedType = parsedType;
    }

    public TypeDef(Type parsedType, boolean isNullable) {
        this.parsedType = parsedType;
        this.isNullable = isNullable;
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

    public static Type createType(TColumnDesc tColumnDesc) {
        TPrimitiveType tPrimitiveType = tColumnDesc.getColumnType();
        PrimitiveType ptype = PrimitiveType.fromThrift(tPrimitiveType);
        if (ptype.isArrayType()) {
            // just support array for now
            Preconditions.checkState(tColumnDesc.getChildren().size() == 1);
            return new ArrayType(createType(tColumnDesc.getChildren().get(0)),
                        tColumnDesc.getChildren().get(0).isIsAllowNull());
        }
        // scarlar type
        int columnLength = tColumnDesc.getColumnLength();
        int columnPrecision = tColumnDesc.getColumnPrecision();
        int columnScale = tColumnDesc.getColumnScale();
        return ScalarType.createType(ptype, columnLength, columnPrecision, columnScale);
    }

    public static TypeDef createTypeDef(TColumnDesc tcolumnDef) {
        return new TypeDef(createType(tcolumnDef));
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
            // now we not support array / map / struct nesting complex type
            if (type.isArrayType()) {
                Type itemType = ((ArrayType) type).getItemType();
                if (itemType instanceof ScalarType) {
                    analyzeNestedType(type, (ScalarType) itemType);
                } else if (Config.disable_nested_complex_type && !(itemType instanceof ArrayType)) {
                    // now we can array nesting array
                    throw new AnalysisException("Unsupported data type: ARRAY<" + itemType.toSql() + ">");
                }
            }
            if (type.isMapType()) {
                MapType mt = (MapType) type;
                if (Config.disable_nested_complex_type && (!(mt.getKeyType() instanceof ScalarType)
                        || !(mt.getValueType() instanceof ScalarType))) {
                    throw new AnalysisException("Unsupported data type: MAP<" + mt.getKeyType().toSql() + ","
                        + mt.getValueType().toSql() + ">");
                }
                if (mt.getKeyType() instanceof ScalarType) {
                    analyzeNestedType(type, (ScalarType) mt.getKeyType());
                }
                if (mt.getValueType() instanceof ScalarType) {
                    analyzeNestedType(type, (ScalarType) mt.getValueType());
                }
            }
            if (type.isStructType()) {
                ArrayList<StructField> fields = ((StructType) type).getFields();
                Set<String> fieldNames = new HashSet<>();
                for (StructField field : fields) {
                    Type fieldType = field.getType();
                    if (fieldType instanceof ScalarType) {
                        analyzeNestedType(type, (ScalarType) fieldType);
                        if (!fieldNames.add(field.getName())) {
                            throw new AnalysisException("Duplicate field name "
                                    + field.getName() + " in struct " + type.toSql());
                        }
                    } else if (Config.disable_nested_complex_type) {
                        throw new AnalysisException("Unsupported field type: " + fieldType.toSql() + " for STRUCT");
                    }
                }
            }
        }
    }

    private void analyzeNestedType(Type parent, ScalarType child) throws AnalysisException {
        if (child.isNull()) {
            throw new AnalysisException("Unsupported data type: " + child.toSql());
        }
        // check whether the sub-type is supported
        if (!parent.supportSubType(child)) {
            throw new AnalysisException(
                    parent.getPrimitiveType() + " unsupported sub-type: " + child.toSql());
        }

        if (child.getPrimitiveType().isStringType() && !child.isLengthSet()) {
            child.setLength(1);
        }
        analyze(child);
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
                if (precision < 1 || precision > ScalarType.MAX_DECIMALV2_PRECISION) {
                    throw new AnalysisException("Precision of decimal must between 1 and 27."
                            + " Precision was set to: " + precision + ".");
                }
                // scale: [0, 9]
                if (scale < 0 || scale > ScalarType.MAX_DECIMALV2_SCALE) {
                    throw new AnalysisException(
                            "Scale of decimal must between 0 and 9." + " Scale was set to: " + scale + ".");
                }
                if (precision - scale > ScalarType.MAX_DECIMALV2_PRECISION - ScalarType.MAX_DECIMALV2_SCALE) {
                    throw new AnalysisException("Invalid decimal type with precision = " + precision + ", scale = "
                            + scale);
                }
                // scale < precision
                if (scale > precision) {
                    throw new AnalysisException("Scale of decimal must be smaller than precision."
                            + " Scale is " + scale + " and precision is " + precision);
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
                // scale < precision
                if (decimal32Scale > decimal32Precision) {
                    throw new AnalysisException("Scale of decimal must be smaller than precision."
                            + " Scale is " + decimal32Scale + " and precision is " + decimal32Precision);
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
                // scale < precision
                if (decimal64Scale > decimal64Precision) {
                    throw new AnalysisException("Scale of decimal must be smaller than precision."
                            + " Scale is " + decimal64Scale + " and precision is " + decimal64Precision);
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
                // scale < precision
                if (decimal128Scale > decimal128Precision) {
                    throw new AnalysisException("Scale of decimal must be smaller than precision."
                            + " Scale is " + decimal128Scale + " and precision is " + decimal128Precision);
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

    public boolean getNullable() {
        return isNullable;
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
