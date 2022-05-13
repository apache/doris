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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class HiveTableTest {
    private String hiveDb;
    private String hiveTable;
    private List<Column> columns;
    private Map<String, String> properties;

    @Before
    public void setUp() {
        hiveDb = "db0";
        hiveTable = "table0";

        columns = Lists.newArrayList();
        Column column = new Column("col1", PrimitiveType.BIGINT);
        columns.add(column);

        properties = Maps.newHashMap();
        properties.put("database", hiveDb);
        properties.put("table", hiveTable);
        properties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");
    }

    @Test
    public void testNormal() throws DdlException {
        HiveTable table = new HiveTable(1000, "hive_table", columns, properties);
        Assert.assertEquals(String.format("%s.%s", hiveDb, hiveTable), table.getHiveDbTable());
        // HiveProperties={hadoop.security.authentication=simple, hive.metastore.uris=thrift://127.0.0.1:9083}
        Assert.assertEquals(2, table.getHiveProperties().size());
    }

    @Test(expected = DdlException.class)
    public void testNoDb() throws DdlException {
        properties.remove("database");
        new HiveTable(1000, "hive_table", columns, properties);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNoTbl() throws DdlException {
        properties.remove("table");
        new HiveTable(1000, "hive_table", columns, properties);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNoHiveMetastoreUris() throws DdlException {
        properties.remove("hive.metastore.uris");
        new HiveTable(1000, "hive_table", columns, properties);
        Assert.fail("No exception throws.");
    }
}
