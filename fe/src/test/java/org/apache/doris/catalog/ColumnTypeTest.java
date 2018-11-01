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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("org.apache.log4j.*")
@PrepareForTest(Catalog.class)
public class ColumnTypeTest {
    @Before
    public void setUp() {
        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getCurrentCatalogJournalVersion()).andReturn(FeConstants.meta_version).anyTimes();
        PowerMock.replay(Catalog.class);
    }

    @Test
    public void testPrimitiveType() throws AnalysisException {
        ColumnType type = ColumnType.createType(PrimitiveType.INT);

        type.analyze();

        Assert.assertEquals(PrimitiveType.INT, type.getType());
        Assert.assertEquals("int(11)", type.toSql());

        // equal type
        ColumnType type2 = ColumnType.createType(PrimitiveType.INT);
        Assert.assertEquals(type, type2);

        // not equal type
        ColumnType type3 = ColumnType.createType(PrimitiveType.BIGINT);
        Assert.assertNotSame(type, type3);
    }

    @Test(expected = AnalysisException.class)
    public void testInvalidType() throws AnalysisException {
        ColumnType type = ColumnType.createType(PrimitiveType.INVALID_TYPE);
        type.analyze();
    }

    @Test
    public void testCharType() throws AnalysisException {
        ColumnType type = ColumnType.createVarchar(10);
        type.analyze();
        Assert.assertEquals("varchar(10)", type.toString());
        Assert.assertEquals(PrimitiveType.VARCHAR, type.getType());
        Assert.assertEquals(10, type.getLen());

        // equal type
        ColumnType type2 = ColumnType.createVarchar(10);
        Assert.assertEquals(type, type2);

        // different type
        ColumnType type3 = ColumnType.createVarchar(3);
        Assert.assertNotSame(type, type3);

        // different type
        ColumnType type4 = ColumnType.createType(PrimitiveType.BIGINT);
        Assert.assertNotSame(type, type4);
    }

    @Test(expected = AnalysisException.class)
    public void testCharInvalid() throws AnalysisException {
        ColumnType type = ColumnType.createVarchar(0);
        type.analyze();
        Assert.fail("No Exception throws");
    }

    @Test
    public void testDecimal() throws AnalysisException {
        ColumnType type = ColumnType.createDecimal(12, 5);
        type.analyze();
        Assert.assertEquals("decimal(12, 5)", type.toString());
        Assert.assertEquals(PrimitiveType.DECIMAL, type.getType());
        Assert.assertEquals(12, type.getPrecision());
        Assert.assertEquals(5, type.getScale());

        // equal type
        ColumnType type2 = ColumnType.createDecimal(12, 5);
        Assert.assertEquals(type, type2);

        // different type
        ColumnType type3 = ColumnType.createDecimal(11, 5);
        Assert.assertNotSame(type, type3);
        type3 = ColumnType.createDecimal(12, 4);
        Assert.assertNotSame(type, type3);

        // different type
        ColumnType type4 = ColumnType.createType(PrimitiveType.BIGINT);
        Assert.assertNotSame(type, type4);
    }

    @Test(expected = AnalysisException.class)
    public void testDecimalPreFail() throws AnalysisException {
        ColumnType type = ColumnType.createDecimal(28, 3);
        type.analyze();
    }

    @Test(expected = AnalysisException.class)
    public void testDecimalScaleFail() throws AnalysisException {
        ColumnType type = ColumnType.createDecimal(27, 10);
        type.analyze();
    }

    @Test(expected = AnalysisException.class)
    public void testDecimalScaleLargeFial() throws AnalysisException {
        ColumnType type = ColumnType.createDecimal(8, 9);
        type.analyze();
    }
    
    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        File file = new File("./columnType");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        
        ColumnType type1 = new ColumnType();
        type1.write(dos);
        
        ColumnType type2 = new ColumnType(PrimitiveType.BIGINT);
        type2.write(dos);
        
        ColumnType type3 = new ColumnType(PrimitiveType.DECIMAL, 1, 1, 1);
        type3.write(dos);
        
        ColumnType type4 = new ColumnType(null, 1, 1, 1);
        type4.write(dos);
        
        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        ColumnType rType1 = new ColumnType();
        rType1.readFields(dis);
        Assert.assertTrue(rType1.equals(type1));
        
        ColumnType rType2 = new ColumnType();
        rType2.readFields(dis);
        Assert.assertTrue(rType2.equals(type2));
        
        ColumnType rType3 = new ColumnType();
        rType3.readFields(dis);
        Assert.assertTrue(rType3.equals(type3));
        
        ColumnType rType4 = new ColumnType();
        rType4.readFields(dis);
        Assert.assertTrue(rType4.equals(type4));
        
        Assert.assertFalse(type1.equals(this));
        
        // 3. delete files
        dis.close();
        file.delete();
    }
}
