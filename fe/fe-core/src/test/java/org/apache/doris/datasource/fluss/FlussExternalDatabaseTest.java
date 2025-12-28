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

import org.apache.doris.datasource.ExternalCatalog;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class FlussExternalDatabaseTest {

    @Mock
    private FlussExternalCatalog mockCatalog;

    private FlussExternalDatabase database;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        database = new FlussExternalDatabase(mockCatalog, 1L, "test_db", "test_db");
    }

    @Test
    public void testDatabaseCreation() {
        Assert.assertNotNull(database);
        Assert.assertEquals("test_db", database.getName());
        Assert.assertEquals(1L, database.getId());
        Assert.assertEquals(mockCatalog, database.getCatalog());
    }

    @Test
    public void testBuildTableInternal() {
        FlussExternalTable table = database.buildTableInternal(
                "remote_table", "local_table", 1L, mockCatalog, database);

        Assert.assertNotNull(table);
        Assert.assertEquals("local_table", table.getName());
        Assert.assertEquals(1L, table.getId());
        Assert.assertEquals(mockCatalog, table.getCatalog());
        Assert.assertEquals(database, table.getDb());
    }
}

