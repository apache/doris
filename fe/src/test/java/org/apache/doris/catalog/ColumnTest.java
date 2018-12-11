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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "org.apache.log4j.*", "javax.management.*" })
@PrepareForTest(Catalog.class)
public class ColumnTest {
    
    private Catalog catalog;

    @Before
    public void setUp() {
        catalog = EasyMock.createMock(Catalog.class);

        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        EasyMock.expect(Catalog.getCurrentCatalogJournalVersion()).andReturn(FeConstants.meta_version).anyTimes();
        PowerMock.replay(Catalog.class);
    }

    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        File file = new File("./columnTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        
        Column column1 = new Column("user", 
                                ScalarType.createChar(20), false, AggregateType.SUM, "", "");
        column1.write(dos);
        Column column2 = new Column("age", 
                                ScalarType.createType(PrimitiveType.INT), false, AggregateType.REPLACE, "20", "");
        column2.write(dos);
        
        Column column3 = new Column("name", PrimitiveType.BIGINT);
        column3.setIsKey(true);
        column3.write(dos);
        
        Column column4 = new Column("age",
                                ScalarType.createType(PrimitiveType.INT), false, AggregateType.REPLACE, "20",
                                    "");
        column4.write(dos);

        dos.flush();
        dos.close();
        
        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        Column rColumn1 = new Column();
        rColumn1.readFields(dis);
        Assert.assertEquals("user", rColumn1.getName());
        Assert.assertEquals(PrimitiveType.CHAR, rColumn1.getDataType());
        Assert.assertEquals(AggregateType.SUM, rColumn1.getAggregationType());
        Assert.assertEquals("", rColumn1.getDefaultValue());
        Assert.assertEquals(0, rColumn1.getScale());
        Assert.assertEquals(0, rColumn1.getPrecision());
        Assert.assertEquals(20, rColumn1.getStrLen());
        Assert.assertFalse(rColumn1.isAllowNull());
        
        // 3. Test read()
        Column rColumn2 = Column.read(dis);
        Assert.assertEquals("age", rColumn2.getName());
        Assert.assertEquals(PrimitiveType.INT, rColumn2.getDataType());
        Assert.assertEquals(AggregateType.REPLACE, rColumn2.getAggregationType());
        Assert.assertEquals("20", rColumn2.getDefaultValue());

        Column rColumn3 = Column.read(dis);
        Assert.assertTrue(rColumn3.equals(column3));

        Column rColumn4 = Column.read(dis);
        Assert.assertTrue(rColumn4.equals(column4));
        
        Assert.assertEquals(rColumn2.toString(), column2.toString());
        Assert.assertTrue(column1.equals(column1));
        Assert.assertFalse(column1.equals(this));

        // 4. delete files
        dis.close();
        file.delete();
    }

    @Test(expected = DdlException.class)
    public void testSchemaChangeAllowed() throws DdlException {
        Column oldColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, true, "0", "");
        Column newColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false, "0", "");
        oldColumn.checkSchemaChangeAllowed(newColumn);
        Assert.fail("No exception throws.");
    }
    
}
