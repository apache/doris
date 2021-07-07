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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import org.apache.doris.analysis.TypeDef;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ColumnTypeTest {
    private FakeCatalog fakeCatalog;
    @Before
    public void setUp() {
        fakeCatalog = new FakeCatalog();
        FakeCatalog.setMetaVersion(FeConstants.meta_version);
    }

    @Test
    public void testPrimitiveType() throws AnalysisException {
        TypeDef type = TypeDef.create(PrimitiveType.INT);

        type.analyze(null);

        Assert.assertEquals(PrimitiveType.INT, type.getType().getPrimitiveType());
        Assert.assertEquals("int(11)", type.toSql());

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
        Assert.assertEquals("varchar(10)", type.toString());
        Assert.assertEquals(PrimitiveType.VARCHAR, type.getType().getPrimitiveType());
        Assert.assertEquals(10, ((ScalarType) type.getType()).getLength());

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

    @Test(expected = AnalysisException.class)
    public void testCharInvalid() throws AnalysisException {
        TypeDef type = TypeDef.createVarchar(-1);
        type.analyze(null);
        Assert.fail("No Exception throws");
    }

    @Test
    public void testDecimal() throws AnalysisException {
        TypeDef type = TypeDef.createDecimal(12, 5);
        type.analyze(null);
        Assert.assertEquals("decimal(12, 5)", type.toString());
        Assert.assertEquals(PrimitiveType.DECIMALV2, type.getType().getPrimitiveType());
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

    @Test(expected = AnalysisException.class)
    public void testDecimalPreFail() throws AnalysisException {
        TypeDef type = TypeDef.createDecimal(28, 3);
        type.analyze(null);
    }

    @Test(expected = AnalysisException.class)
    public void testDecimalScaleFail() throws AnalysisException {
        TypeDef type = TypeDef.createDecimal(27, 10);
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
        File file = new File("./columnType");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        ScalarType type1 = Type.NULL;
        ColumnType.write(dos, type1);
        
        ScalarType type2 = ScalarType.createType(PrimitiveType.BIGINT);
        ColumnType.write(dos, type2);

        ScalarType type3 = ScalarType.createDecimalV2Type(1, 1);
        ColumnType.write(dos, type3);

        ScalarType type4 = ScalarType.createDecimalV2Type(1, 1);
        ColumnType.write(dos, type4);

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        Type rType1 = ColumnType.read(dis);
        Assert.assertTrue(rType1.equals(type1));
        
        Type rType2 = ColumnType.read(dis);
        Assert.assertTrue(rType2.equals(type2));
        
        Type rType3 = ColumnType.read(dis);

        // Change it when remove DecimalV2
        Assert.assertTrue(rType3.equals(type3) || rType3.equals(type4));

        Assert.assertFalse(type1.equals(this));
        
        // 3. delete files
        dis.close();
        file.delete();
    }
}
