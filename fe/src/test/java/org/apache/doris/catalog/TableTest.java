// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

import org.apache.doris.common.FeConstants;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

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
public class TableTest {

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
        File file = new File("./tableFamilyGroup");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("column2", 
                        ColumnType.createType(PrimitiveType.TINYINT), false, AggregateType.MIN, "", ""));
        columns.add(new Column("column3", 
                        ColumnType.createType(PrimitiveType.SMALLINT), false, AggregateType.SUM, "", ""));
        columns.add(new Column("column4", 
                        ColumnType.createType(PrimitiveType.INT), false, AggregateType.REPLACE, "", ""));
        columns.add(new Column("column5", 
                        ColumnType.createType(PrimitiveType.BIGINT), false, AggregateType.REPLACE, "", ""));
        columns.add(new Column("column6", 
                        ColumnType.createType(PrimitiveType.FLOAT), false, AggregateType.REPLACE, "", ""));
        columns.add(new Column("column7", 
                        ColumnType.createType(PrimitiveType.DOUBLE), false, AggregateType.REPLACE, "", ""));
        columns.add(new Column("column8", ColumnType.createChar(10), true, null, "", ""));
        columns.add(new Column("column9", ColumnType.createVarchar(10), true, null, "", ""));
        columns.add(new Column("column10", ColumnType.createType(PrimitiveType.DATE), true, null, "", ""));
        columns.add(new Column("column11", ColumnType.createType(PrimitiveType.DATETIME), true, null, "", ""));

        Table table1 = new OlapTable(1000L, "group1", columns, KeysType.AGG_KEYS,
                                     new SinglePartitionInfo(), new RandomDistributionInfo(10));
        table1.write(dos);
        dos.flush();
        dos.close();
        
        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));

        Table rFamily1 = Table.read(dis);
        Assert.assertTrue(table1.equals(rFamily1));
        
        // 3. delete files
        dis.close();
        file.delete();
    }
}
