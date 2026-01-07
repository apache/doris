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

package org.apache.doris.datasource.fluss;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableType;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;

public class FlussExternalTableTest {

    @Mock
    private FlussExternalCatalog mockCatalog;

    @Mock
    private FlussExternalDatabase mockDatabase;

    private FlussExternalTable table;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        table = new FlussExternalTable(1L, "test_table", "test_table", mockCatalog, mockDatabase);
    }

    @Test
    public void testTableCreation() {
        Assert.assertNotNull(table);
        Assert.assertEquals("test_table", table.getName());
        Assert.assertEquals(1L, table.getId());
        Assert.assertEquals(mockCatalog, table.getCatalog());
        Assert.assertEquals(mockDatabase, table.getDb());
    }

    @Test
    public void testTableType() {
        Assert.assertEquals(TableType.FLUSS_EXTERNAL_TABLE, table.getType());
    }

    @Test
    public void testToThrift() {
        FlussExternalTable spyTable = Mockito.spy(table);
        Mockito.when(spyTable.getDbName()).thenReturn("test_db");
        Mockito.when(spyTable.getName()).thenReturn("test_table");
        List<Column> emptySchema = Lists.newArrayList();
        Mockito.when(spyTable.getFullSchema()).thenReturn(emptySchema);

        TTableDescriptor descriptor = spyTable.toThrift();
        Assert.assertNotNull(descriptor);
        Assert.assertEquals(TTableType.FLUSS_EXTERNAL_TABLE, descriptor.getTableType());
        Assert.assertEquals("test_table", descriptor.getTableName());
        Assert.assertEquals("test_db", descriptor.getDbName());
        Assert.assertNotNull(descriptor.getFlussTable());
    }

    @Test
    public void testGetRemoteDbName() {
        FlussExternalTable spyTable = Mockito.spy(table);
        Mockito.when(spyTable.getRemoteDbName()).thenReturn("remote_db");
        String remoteDbName = spyTable.getRemoteDbName();
        Assert.assertEquals("remote_db", remoteDbName);
    }

    @Test
    public void testGetRemoteName() {
        FlussExternalTable spyTable = Mockito.spy(table);
        Mockito.when(spyTable.getRemoteName()).thenReturn("remote_table");
        String remoteName = spyTable.getRemoteName();
        Assert.assertEquals("remote_table", remoteName);
    }
}

