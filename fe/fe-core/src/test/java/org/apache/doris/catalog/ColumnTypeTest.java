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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ColumnTypeTest {
    private FakeEnv fakeEnv;

    @BeforeEach
    public void setUp() {
        fakeEnv = new FakeEnv();
        FakeEnv.setMetaVersion(FeConstants.meta_version);
    }

    @AfterEach
    public void tearDown() {
        if (fakeEnv != null) {
            fakeEnv.close();
        }
    }

    @Test
    public void testPrimitiveType() {
        Type type = ScalarType.createType(PrimitiveType.INT);
        Assertions.assertEquals(PrimitiveType.INT, type.getPrimitiveType());
        Assertions.assertEquals("int", type.toSql());

        // equal type
        Type type2 = ScalarType.createType(PrimitiveType.INT);
        Assertions.assertEquals(type, type2);

        // not equal type
        Type type3 = ScalarType.createType(PrimitiveType.BIGINT);
        Assertions.assertNotSame(type, type3);
    }

    @Test
    public void testCharType() {
        Type type = ScalarType.createVarchar(10);
        Assertions.assertEquals("varchar(10)", type.toSql());
        Assertions.assertEquals(PrimitiveType.VARCHAR, type.getPrimitiveType());
        Assertions.assertEquals(10, type.getLength());

        // equal type
        Type type2 = ScalarType.createVarchar(10);
        Assertions.assertEquals(type, type2);

        // different type
        Type type3 = ScalarType.createVarchar(3);
        Assertions.assertNotEquals(type, type3);

        // different type
        Type type4 = ScalarType.createType(PrimitiveType.BIGINT);
        Assertions.assertNotEquals(type, type4);
    }

    @Test
    public void testDecimal() {
        Type type = ScalarType.createDecimalType(12, 5);
        if (Config.enable_decimal_conversion) {
            Assertions.assertEquals("decimalv3(12,5)", type.toSql());
            Assertions.assertEquals(PrimitiveType.DECIMAL64, type.getPrimitiveType());
        } else {
            Assertions.assertEquals("decimalv2(12,5)", type.toSql());
            Assertions.assertEquals(PrimitiveType.DECIMALV2, type.getPrimitiveType());
        }
        Assertions.assertEquals(12, ((ScalarType) type).getScalarPrecision());
        Assertions.assertEquals(5, ((ScalarType) type).getScalarScale());

        // equal type
        Type type2 = ScalarType.createDecimalType(12, 5);
        Assertions.assertEquals(type, type2);

        // different type
        Type type3 = ScalarType.createDecimalType(11, 5);
        Assertions.assertNotEquals(type, type3);
        type3 = ScalarType.createDecimalType(12, 4);
        Assertions.assertNotEquals(type, type3);

        // different type
        Type type4 = ScalarType.createType(PrimitiveType.BIGINT);
        Assertions.assertNotEquals(type, type4);
    }

    @Test
    public void testDatetimeV2() {
        Type type = ScalarType.createDatetimeV2Type(3);
        Assertions.assertEquals("datetimev2(3)", type.toSql());
        Assertions.assertEquals(PrimitiveType.DATETIMEV2, type.getPrimitiveType());
        Assertions.assertEquals(ScalarType.DATETIME_PRECISION, ((ScalarType) type).getScalarPrecision());
        Assertions.assertEquals(3, ((ScalarType) type).getScalarScale());

        // equal type
        Type type2 = ScalarType.createDatetimeV2Type(3);
        Assertions.assertEquals(type, type2);

        // different type
        Type type3 = ScalarType.createDatetimeV2Type(6);
        Assertions.assertNotEquals(type, type3);
        type3 = ScalarType.createDatetimeV2Type(0);
        Assertions.assertNotEquals(type, type3);

        // different type
        Type type4 = ScalarType.createType(PrimitiveType.BIGINT);
        Assertions.assertNotEquals(type, type4);

        Type type5 = ScalarType.createDatetimeV2Type(0);
        Type type6 = ScalarType.createType(PrimitiveType.DATETIME);
        Assertions.assertNotEquals(type5, type6);
        Assertions.assertNotEquals(type, type6);
    }

    @Test
    public void testDateV2() {
        Type type = ScalarType.createType(PrimitiveType.DATE);
        Type type2 = ScalarType.createType(PrimitiveType.DATEV2);
        Assertions.assertNotEquals(type, type2);

        // different type
        Type type3 = ScalarType.createDatetimeV2Type(6);
        Assertions.assertNotEquals(type2, type3);
    }

    @Test
    public void testTimeV2() {
        Type type = ScalarType.createTimeV2Type(3);
        Assertions.assertEquals("time(3)", type.toSql());
        Assertions.assertEquals(PrimitiveType.TIMEV2, type.getPrimitiveType());
        Assertions.assertEquals(ScalarType.DATETIME_PRECISION, ((ScalarType) type).getScalarPrecision());
        Assertions.assertEquals(3, ((ScalarType) type).getScalarScale());

        // equal type
        Type type2 = ScalarType.createTimeV2Type(3);
        Assertions.assertEquals(type, type2);

        // different type
        Type type3 = ScalarType.createTimeV2Type(6);
        Assertions.assertNotEquals(type, type3);
        type3 = ScalarType.createTimeV2Type(0);
        Assertions.assertNotEquals(type, type3);

        // different type
        Type type4 = ScalarType.createType(PrimitiveType.BIGINT);
        Assertions.assertNotEquals(type, type4);
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
        Assertions.assertEquals(rType1, type1);

        Type rType2 = ColumnType.read(dis);
        Assertions.assertEquals(rType2, type2);

        Type rType3 = ColumnType.read(dis);

        // Change it when remove DecimalV2
        Assertions.assertTrue(rType3.equals(type3) || rType3.equals(type4));

        Assertions.assertNotEquals(type1, this);

        // 3. delete files
        dis.close();
        Files.deleteIfExists(path);
    }
}
