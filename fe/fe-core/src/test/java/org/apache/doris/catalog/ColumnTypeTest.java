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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.SessionVariable;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class ColumnTypeTest {
    private FakeEnv fakeEnv;

    @Before
    public void setUp() {
        fakeEnv = new FakeEnv();
        FakeEnv.setMetaVersion(FeConstants.meta_version);
    }

    @Test
    public void testPrimitiveType() throws AnalysisException {
        Type type = ScalarType.createType(PrimitiveType.INT);

        analyze(type);

        Assert.assertEquals(PrimitiveType.INT, type.getPrimitiveType());
        Assert.assertEquals("int", type.toSql());

        // equal type
        Type type2 = ScalarType.createType(PrimitiveType.INT);
        Assert.assertEquals(type, type2);

        // not equal type
        Type type3 = ScalarType.createType(PrimitiveType.BIGINT);
        Assert.assertNotSame(type, type3);
    }

    @Test(expected = AnalysisException.class)
    public void testInvalidType() throws AnalysisException {
        Type type = ScalarType.createType(PrimitiveType.INVALID_TYPE);
        analyze(type);
    }

    @Test
    public void testCharType() throws AnalysisException {
        Type type = ScalarType.createVarchar(10);
        analyze(type);
        Assert.assertEquals("varchar(10)", type.toSql());
        Assert.assertEquals(PrimitiveType.VARCHAR, type.getPrimitiveType());
        Assert.assertEquals(10, type.getLength());

        // equal type
        Type type2 = ScalarType.createVarchar(10);
        Assert.assertEquals(type, type2);

        // different type
        Type type3 = ScalarType.createVarchar(3);
        Assert.assertNotEquals(type, type3);

        // different type
        Type type4 = ScalarType.createType(PrimitiveType.BIGINT);
        Assert.assertNotEquals(type, type4);
    }

    @Test
    public void testDecimal() throws AnalysisException {
        Type type = ScalarType.createDecimalType(12, 5);
        analyze(type);
        if (Config.enable_decimal_conversion) {
            Assert.assertEquals("decimalv3(12,5)", type.toSql());
            Assert.assertEquals(PrimitiveType.DECIMAL64, type.getPrimitiveType());
        } else {
            Assert.assertEquals("decimalv2(12,5)", type.toSql());
            Assert.assertEquals(PrimitiveType.DECIMALV2, type.getPrimitiveType());
        }
        Assert.assertEquals(12, ((ScalarType) type).getScalarPrecision());
        Assert.assertEquals(5, ((ScalarType) type).getScalarScale());

        // equal type
        Type type2 = ScalarType.createDecimalType(12, 5);
        Assert.assertEquals(type, type2);

        // different type
        Type type3 = ScalarType.createDecimalType(11, 5);
        Assert.assertNotEquals(type, type3);
        type3 = ScalarType.createDecimalType(12, 4);
        Assert.assertNotEquals(type, type3);

        // different type
        Type type4 = ScalarType.createType(PrimitiveType.BIGINT);
        Assert.assertNotEquals(type, type4);
    }

    @Test
    public void testDatetimeV2() throws AnalysisException {
        Type type = ScalarType.createDatetimeV2Type(3);
        analyze(type);
        Assert.assertEquals("datetimev2(3)", type.toSql());
        Assert.assertEquals(PrimitiveType.DATETIMEV2, type.getPrimitiveType());
        Assert.assertEquals(ScalarType.DATETIME_PRECISION, ((ScalarType) type).getScalarPrecision());
        Assert.assertEquals(3, ((ScalarType) type).getScalarScale());

        // equal type
        Type type2 = ScalarType.createDatetimeV2Type(3);
        Assert.assertEquals(type, type2);

        // different type
        Type type3 = ScalarType.createDatetimeV2Type(6);
        Assert.assertNotEquals(type, type3);
        type3 = ScalarType.createDatetimeV2Type(0);
        Assert.assertNotEquals(type, type3);

        // different type
        Type type4 = ScalarType.createType(PrimitiveType.BIGINT);
        Assert.assertNotEquals(type, type4);

        Type type5 = ScalarType.createDatetimeV2Type(0);
        Type type6 = ScalarType.createType(PrimitiveType.DATETIME);
        Assert.assertNotEquals(type5, type6);
        Assert.assertNotEquals(type, type6);
    }

    @Test
    public void testDateV2() throws AnalysisException {
        Type type = ScalarType.createType(PrimitiveType.DATE);
        Type type2 = ScalarType.createType(PrimitiveType.DATEV2);
        analyze(type);
        Assert.assertNotEquals(type, type2);

        // different type
        Type type3 = ScalarType.createDatetimeV2Type(6);
        Assert.assertNotEquals(type2, type3);
    }

    @Test
    public void testTimeV2() throws AnalysisException {
        Type type = ScalarType.createTimeV2Type(3);
        analyze(type);
        Assert.assertEquals("time(3)", type.toSql());
        Assert.assertEquals(PrimitiveType.TIMEV2, type.getPrimitiveType());
        Assert.assertEquals(ScalarType.DATETIME_PRECISION, ((ScalarType) type).getScalarPrecision());
        Assert.assertEquals(3, ((ScalarType) type).getScalarScale());

        // equal type
        Type type2 = ScalarType.createTimeV2Type(3);
        Assert.assertEquals(type, type2);

        // different type
        Type type3 = ScalarType.createTimeV2Type(6);
        Assert.assertNotEquals(type, type3);
        type3 = ScalarType.createTimeV2Type(0);
        Assert.assertNotEquals(type, type3);

        // different type
        Type type4 = ScalarType.createType(PrimitiveType.BIGINT);
        Assert.assertNotEquals(type, type4);
    }

    @Test(expected = AnalysisException.class)
    public void testDecimalPreFail() throws AnalysisException {
        Type type;
        if (Config.enable_decimal_conversion) {
            type = ScalarType.createDecimalType(77, 3);
        } else {
            type = ScalarType.createDecimalType(28, 3);
        }
        analyze(type);
    }

    @Test(expected = AnalysisException.class)
    public void testDecimalScaleFail() throws AnalysisException {
        Type type;
        if (Config.enable_decimal_conversion) {
            type = ScalarType.createDecimalType(27, 28);
        } else {
            type = ScalarType.createDecimalType(27, 10);
        }
        analyze(type);
    }

    @Test(expected = AnalysisException.class)
    public void testDecimalScaleLargeFail() throws AnalysisException {
        Type type = ScalarType.createDecimalType(8, 9);
        analyze(type);
    }

    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        Path path = Files.createFile(Paths.get("./columnType"));
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));

        ScalarType type1 = Type.NULL;
        ColumnType.write(dos, type1);

        ScalarType type2 = ScalarType.createType(PrimitiveType.BIGINT);
        ColumnType.write(dos, type2);

        ScalarType type3 = ScalarType.createDecimalType(1, 1);
        ColumnType.write(dos, type3);

        ScalarType type4 = ScalarType.createDecimalType(1, 1);
        ColumnType.write(dos, type4);

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(Files.newInputStream(path));
        Type rType1 = ColumnType.read(dis);
        Assert.assertEquals(rType1, type1);

        Type rType2 = ColumnType.read(dis);
        Assert.assertEquals(rType2, type2);

        Type rType3 = ColumnType.read(dis);

        // Change it when remove DecimalV2
        Assert.assertTrue(rType3.equals(type3) || rType3.equals(type4));

        Assert.assertNotEquals(type1, this);

        // 3. delete files
        dis.close();
        Files.deleteIfExists(path);
    }

    private void analyze(Type type) throws AnalysisException {
        // Check the max nesting depth before calling the recursive analyze() to avoid
        // a stack overflow.
        if (type.exceedsMaxNestingDepth()) {
            throw new AnalysisException(String.format(
                "Type exceeds the maximum nesting depth of %s:\n%s",
                Type.MAX_NESTING_DEPTH, type.toSql()));
        }

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
                }
            }
            if (type.isMapType()) {
                MapType mt = (MapType) type;
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

        analyze(child);
    }

    private void analyzeScalarType(ScalarType scalarType)
            throws AnalysisException {
        PrimitiveType type = scalarType.getPrimitiveType();
        // When string type length is not assigned, it needs to be assigned to 1.
        if (scalarType.getPrimitiveType().isStringType() && !scalarType.isLengthSet()) {
            if (scalarType.getPrimitiveType() == PrimitiveType.VARCHAR) {
                // always set varchar length MAX_VARCHAR_LENGTH
                scalarType.setLength(ScalarType.MAX_VARCHAR_LENGTH);
            } else if (scalarType.getPrimitiveType() == PrimitiveType.STRING) {
                // always set text length MAX_STRING_LENGTH
                scalarType.setLength(ScalarType.MAX_STRING_LENGTH);
            } else {
                scalarType.setLength(1);
            }
        }
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
            case DECIMAL256: {
                if (SessionVariable.getEnableDecimal256()) {
                    int precision = scalarType.decimalPrecision();
                    int scale = scalarType.decimalScale();
                    if (precision < 1 || precision > ScalarType.MAX_DECIMAL256_PRECISION) {
                        throw new AnalysisException("Precision of decimal256 must between 1 and 76."
                            + " Precision was set to: " + precision + ".");
                    }
                    // scale >= 0
                    if (scale < 0) {
                        throw new AnalysisException("Scale of decimal must not be less than 0." + " Scale was set to: "
                            + scale + ".");
                    }
                    // scale < precision
                    if (scale > precision) {
                        throw new AnalysisException("Scale of decimal must be smaller than precision."
                            + " Scale is " + scale + " and precision is " + precision);
                    }
                    break;
                } else {
                    int precision = scalarType.decimalPrecision();
                    throw new AnalysisException(
                        "Column of type Decimal256 with precision " + precision + " in not supported.");
                }
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
}
