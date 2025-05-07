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

package org.apache.doris.datasource.systable;

import org.apache.doris.common.Pair;

import org.junit.Assert;
import org.junit.Test;

public class SysTableTest {
    // Mock implementation of SysTable for testing
    private static class MockSysTable extends SysTable {
        public MockSysTable(String sysTableName, String tvfName) {
            super(sysTableName, tvfName);
        }

        @Override
        public org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction createFunction(
                String ctlName, String dbName, String sourceNameWithMetaName) {
            return null;
        }

        @Override
        public org.apache.doris.analysis.TableValuedFunctionRef createFunctionRef(
                String ctlName, String dbName, String sourceNameWithMetaName) {
            return null;
        }
    }

    @Test
    public void testBasicProperties() {
        MockSysTable sysTable = new MockSysTable("test_table", "test_tvf");
        Assert.assertEquals("test_table", sysTable.getSysTableName());
        Assert.assertEquals("$test_table", sysTable.suffix);
        Assert.assertEquals("test_tvf", sysTable.tvfName);
    }

    @Test
    public void testContainsMetaTable() {
        MockSysTable sysTable = new MockSysTable("partitions", "partition_values");

        // Positive cases
        Assert.assertTrue(sysTable.containsMetaTable("mytable$partitions"));
        Assert.assertTrue(sysTable.containsMetaTable("my_complex_table$partitions"));

        // Negative cases
        Assert.assertFalse(sysTable.containsMetaTable("mytable"));
        Assert.assertFalse(sysTable.containsMetaTable("$partitions")); // No table name
        Assert.assertFalse(sysTable.containsMetaTable("mytable$other"));
        Assert.assertFalse(sysTable.containsMetaTable(""));
    }

    @Test
    public void testGetSourceTableName() {
        MockSysTable sysTable = new MockSysTable("partitions", "partition_values");

        Assert.assertEquals("mytable", sysTable.getSourceTableName("mytable$partitions"));
        Assert.assertEquals("complex_table", sysTable.getSourceTableName("complex_table$partitions"));
        Assert.assertEquals("table$with$dollar", sysTable.getSourceTableName("table$with$dollar$partitions"));
    }

    @Test
    public void testGetTableNameWithSysTableName() {
        // Test normal case
        Pair<String, String> result1 = SysTable.getTableNameWithSysTableName("table$partitions");
        Assert.assertEquals("table", result1.first);
        Assert.assertEquals("partitions", result1.second);

        // Test with multiple $ symbols
        Pair<String, String> result2 = SysTable.getTableNameWithSysTableName("table$with$dollar$partitions");
        Assert.assertEquals("table$with$dollar", result2.first);
        Assert.assertEquals("partitions", result2.second);

        // Test edge cases
        Pair<String, String> result3 = SysTable.getTableNameWithSysTableName("table");
        Assert.assertEquals("table", result3.first);
        Assert.assertEquals("", result3.second);

        Pair<String, String> result4 = SysTable.getTableNameWithSysTableName("$");
        Assert.assertEquals("$", result4.first);
        Assert.assertEquals("", result4.second);

        Pair<String, String> result5 = SysTable.getTableNameWithSysTableName("");
        Assert.assertEquals("", result5.first);
        Assert.assertEquals("", result5.second);
    }
}
