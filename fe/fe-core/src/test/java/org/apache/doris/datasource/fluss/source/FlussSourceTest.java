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

package org.apache.doris.datasource.fluss.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.fluss.FlussExternalCatalog;
import org.apache.doris.datasource.fluss.FlussExternalTable;

import org.apache.fluss.client.table.Table;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class FlussSourceTest {

    @Mock
    private TupleDescriptor mockTupleDesc;

    @Mock
    private FlussExternalTable mockTable;

    @Mock
    private FlussExternalCatalog mockCatalog;

    @Mock
    private Table mockFlussTable;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        Mockito.when(mockTupleDesc.getTable()).thenReturn(mockTable);
        Mockito.when(mockTable.getCatalog()).thenReturn(mockCatalog);
        Mockito.when(mockTable.getRemoteDbName()).thenReturn("test_db");
        Mockito.when(mockTable.getRemoteName()).thenReturn("test_table");
    }

    @Test
    public void testFlussSourceCreation() {
        FlussSource source = new FlussSource(mockTupleDesc);
        Assert.assertNotNull(source);
        Assert.assertEquals(mockTable, source.getTargetTable());
        Assert.assertEquals(mockCatalog, source.getCatalog());
    }

    @Test
    public void testGetTargetTable() {
        FlussSource source = new FlussSource(mockTupleDesc);
        FlussExternalTable targetTable = source.getTargetTable();
        Assert.assertNotNull(targetTable);
        Assert.assertEquals(mockTable, targetTable);
    }

    @Test
    public void testGetCatalog() {
        FlussSource source = new FlussSource(mockTupleDesc);
        FlussExternalCatalog catalog = source.getCatalog();
        Assert.assertNotNull(catalog);
        Assert.assertEquals(mockCatalog, catalog);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFlussSourceWithNonFlussTable() {
        ExternalTable nonFlussTable = Mockito.mock(ExternalTable.class);
        Mockito.when(mockTupleDesc.getTable()).thenReturn(nonFlussTable);
        new FlussSource(mockTupleDesc);
    }
}

