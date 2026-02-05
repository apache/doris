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
    public void testPrimitiveType() {
        Type type = ScalarType.createType(PrimitiveType.INT);
        Assert.assertEquals(PrimitiveType.INT, type.getPrimitiveType());
        Assert.assertEquals("int", type.toSql());

        // equal type
        Type type2 = ScalarType.createType(PrimitiveType.INT);
        Assert.assertEquals(type, type2);

        // not equal type
        Type type3 = ScalarType.createType(PrimitiveType.BIGINT);
        Assert.assertNotSame(type, type3);
    }

    @Test
    public void testCharType() {
        Type type = ScalarType.createVarchar(10);
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
    public void testDecimal() {
        Type type = ScalarType.createDecimalType(12, 5);
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
    public void testDatetimeV2() {
        Type type = ScalarType.createDatetimeV2Type(3);
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
    public void testDateV2() {
        Type type = ScalarType.createType(PrimitiveType.DATE);
        Type type2 = ScalarType.createType(PrimitiveType.DATEV2);
        Assert.assertNotEquals(type, type2);

        // different type
        Type type3 = ScalarType.createDatetimeV2Type(6);
        Assert.assertNotEquals(type2, type3);
    }

    @Test
    public void testTimeV2() {
        Type type = ScalarType.createTimeV2Type(3);
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
