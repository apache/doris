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

import org.apache.doris.datasource.TableFormatType;

import org.junit.Assert;
import org.junit.Test;

public class FlussSplitTest {

    @Test
    public void testFlussSplitCreation() {
        FlussSplit split = new FlussSplit("test_db", "test_table", 123L);
        
        Assert.assertNotNull(split);
        Assert.assertEquals("test_db", split.getDatabaseName());
        Assert.assertEquals("test_table", split.getTableName());
        Assert.assertEquals(123L, split.getTableId());
        Assert.assertEquals(TableFormatType.FLUSS, split.getTableFormatType());
    }

    @Test
    public void testGetConsistentHashString() {
        FlussSplit split = new FlussSplit("test_db", "test_table", 123L);
        String hashString = split.getConsistentHashString();
        
        Assert.assertNotNull(hashString);
        Assert.assertEquals("test_db.test_table.123", hashString);
    }

    @Test
    public void testGetters() {
        FlussSplit split = new FlussSplit("db1", "table1", 456L);
        
        Assert.assertEquals("db1", split.getDatabaseName());
        Assert.assertEquals("table1", split.getTableName());
        Assert.assertEquals(456L, split.getTableId());
        Assert.assertEquals(TableFormatType.FLUSS, split.getTableFormatType());
    }
}

