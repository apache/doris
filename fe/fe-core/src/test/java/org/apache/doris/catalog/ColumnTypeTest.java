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

import org.apache.doris.analysis.TypeDef;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ColumnTypeTest {
    private FakeEnv fakeEnv;

    @Before
    public void setUp() {
        fakeEnv = new FakeEnv();
        FakeEnv.setMetaVersion(FeConstants.meta_version);
    }

    @Test
    public void testPrimitiveType() throws AnalysisException {
        TypeDef type = TypeDef.create(PrimitiveType.INT);

        type.analyze(null);

        Assert.assertEquals(PrimitiveType.INT, type.getType().getPrimitiveType());
        Assert.assertEquals("INT", type.toSql());

        // equal type
        TypeDef type2 = TypeDef.create(PrimitiveType.INT);
        Assert.assertEquals(type.getType(), type2.getType());

        // not equal type
        TypeDef type3 = TypeDef.create(PrimitiveType.BIGINT);
        Assert.assertNotSame(type.getType(), type3.getType());
    }

    @Test(expected = AnalysisException.class)
    public void testInvalidType() throws AnalysisException {
        TypeDef type = TypeDef.create(PrimitiveType.INVALID_TYPE);
        type.analyze(null);
    }

    @Test
    public void testCharType() throws AnalysisException {
        TypeDef type = TypeDef.createVarchar(10);
        type.analyze(null);
        Assert.assertEquals("VARCHAR(10)", type.toString());
        Assert.assertEquals(PrimitiveType.VARCHAR, type.getType().getPrimitiveType());
        Assert.assertEquals(10, type.getType().getLength());

        // equal type
        TypeDef type2 = TypeDef.createVarchar(10);
        Assert.assertEquals(type.getType(), type2.getType());

        // different type
        TypeDef type3 = TypeDef.createVarchar(3);
        Assert.assertNotEquals(type.getType(), type3.getType());

        // different type
        TypeDef type4 = TypeDef.create(PrimitiveType.BIGINT);
        Assert.assertNotEquals(type.getType(), type4.getType());
    }

    @Test
    public void testDecimal() throws AnalysisException {
        TypeDef type = TypeDef.createDecimal(12, 5);
        type.analyze(null);
        if (Config.enable_decimal_conversion) {
            Assert.assertEquals("DECIMALV3(12, 5)", type.toString());
            Assert.assertEquals(PrimitiveType.DECIMAL64, type.getType().getPrimitiveType());
        } else {
            Assert.assertEquals("DECIMAL(12, 5)", type.toString());
            Assert.assertEquals(PrimitiveType.DECIMALV2, type.getType().getPrimitiveType());
        }
        Assert.assertEquals(12, ((ScalarType) type.getType()).getScalarPrecision());
        Assert.assertEquals(5, ((ScalarType) type.getType()).getScalarScale());

        // equal type
        TypeDef type2 = TypeDef.createDecimal(12, 5);
        Assert.assertEquals(type.getType(), type2.getType());

        // different type
        TypeDef type3 = TypeDef.createDecimal(11, 5);
        Assert.assertNotEquals(type.getType(), type3.getType());
        type3 = TypeDef.createDecimal(12, 4);
        Assert.assertNotEquals(type.getType(), type3.getType());

        // different type
        TypeDef type4 = TypeDef.create(PrimitiveType.BIGINT);
        Assert.assertNotEquals(type.getType(), type4.getType());
    }

    @Test
    public void testDatetimeV2() throws AnalysisException {
        TypeDef type = TypeDef.createDatetimeV2(3);
        type.analyze(null);
        Assert.assertEquals("DATETIMEV2(3)", type.toString());
        Assert.assertEquals(PrimitiveType.DATETIMEV2, type.getType().getPrimitiveType());
        Assert.assertEquals(ScalarType.DATETIME_PRECISION, ((ScalarType) type.getType()).getScalarPrecision());
        Assert.assertEquals(3, ((ScalarType) type.getType()).getScalarScale());

        // equal type
        TypeDef type2 = TypeDef.createDatetimeV2(3);
        Assert.assertEquals(type.getType(), type2.getType());

        // different type
        TypeDef type3 = TypeDef.createDatetimeV2(6);
        Assert.assertNotEquals(type.getType(), type3.getType());
        type3 = TypeDef.createDatetimeV2(0);
        Assert.assertNotEquals(type.getType(), type3.getType());

        // different type
        TypeDef type4 = TypeDef.create(PrimitiveType.BIGINT);
        Assert.assertNotEquals(type.getType(), type4.getType());

        TypeDef type5 = TypeDef.createDatetimeV2(0);
        TypeDef type6 = TypeDef.create(PrimitiveType.DATETIME);
        Assert.assertNotEquals(type5.getType(), type6.getType());
        Assert.assertNotEquals(type.getType(), type6.getType());
    }

    @Test
    public void testDateV2() throws AnalysisException {
        TypeDef type = TypeDef.create(PrimitiveType.DATE);
        TypeDef type2 = TypeDef.create(PrimitiveType.DATEV2);
        type.analyze(null);
        Assert.assertNotEquals(type.getType(), type2.getType());

        // different type
        TypeDef type3 = TypeDef.createDatetimeV2(6);
        Assert.assertNotEquals(type2.getType(), type3.getType());
    }

    @Test
    public void testTimeV2() throws AnalysisException {
        TypeDef type = TypeDef.createTimeV2(3);
        type.analyze(null);
        Assert.assertEquals("TIME(3)", type.toString());
        Assert.assertEquals(PrimitiveType.TIMEV2, type.getType().getPrimitiveType());
        Assert.assertEquals(ScalarType.DATETIME_PRECISION, ((ScalarType) type.getType()).getScalarPrecision());
        Assert.assertEquals(3, ((ScalarType) type.getType()).getScalarScale());

        // equal type
        TypeDef type2 = TypeDef.createTimeV2(3);
        Assert.assertEquals(type.getType(), type2.getType());

        // different type
        TypeDef type3 = TypeDef.createTimeV2(6);
        Assert.assertNotEquals(type.getType(), type3.getType());
        type3 = TypeDef.createTimeV2(0);
        Assert.assertNotEquals(type.getType(), type3.getType());

        // different type
        TypeDef type4 = TypeDef.create(PrimitiveType.BIGINT);
        Assert.assertNotEquals(type.getType(), type4.getType());

        TypeDef type5 = TypeDef.createTimeV2(0);
        TypeDef type6 = TypeDef.create(PrimitiveType.TIME);
        Assert.assertNotEquals(type5.getType(), type6.getType());
        Assert.assertNotEquals(type.getType(), type6.getType());
    }

    @Test(expected = AnalysisException.class)
    public void testDecimalPreFail() throws AnalysisException {
        TypeDef type;
        if (Config.enable_decimal_conversion) {
            type = TypeDef.createDecimal(77, 3);
        } else {
            type = TypeDef.createDecimal(28, 3);
        }
        type.analyze(null);
    }

    @Test(expected = AnalysisException.class)
    public void testDecimalScaleFail() throws AnalysisException {
        TypeDef type;
        if (Config.enable_decimal_conversion) {
            type = TypeDef.createDecimal(27, 28);
        } else {
            type = TypeDef.createDecimal(27, 10);
        }
        type.analyze(null);
    }

    @Test(expected = AnalysisException.class)
    public void testDecimalScaleLargeFail() throws AnalysisException {
        TypeDef type = TypeDef.createDecimal(8, 9);
        type.analyze(null);
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
}
