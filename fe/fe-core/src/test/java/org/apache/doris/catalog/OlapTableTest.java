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

import org.apache.doris.analysis.IndexDef;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.FastByteArrayOutputStream;
import org.apache.doris.common.util.UnitTestUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class OlapTableTest {

    @Test
    public void test() throws IOException {

        new MockUp<Env>() {
            @Mock
            int getCurrentEnvJournalVersion() {
                return FeConstants.meta_version;
            }
        };

        Database db = UnitTestUtil.createDb(1, 2, 3, 4, 5, 6, 7);
        List<Table> tables = db.getTables();

        for (Table table : tables) {
            if (table.getType() != TableType.OLAP) {
                continue;
            }
            OlapTable tbl = (OlapTable) table;
            tbl.setIndexes(Lists.newArrayList(new Index("index", Lists.newArrayList("col"),
                    IndexDef.IndexType.BITMAP, "xxxxxx")));
            System.out.println("orig table id: " + tbl.getId());

            FastByteArrayOutputStream byteArrayOutputStream = new FastByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(byteArrayOutputStream);
            tbl.write(out);

            out.flush();
            out.close();

            DataInputStream in = new DataInputStream(byteArrayOutputStream.getInputStream());
            Table copiedTbl = OlapTable.read(in);
            System.out.println("copied table id: " + copiedTbl.getId());
            in.close();
        }

    }

    @Test
    public void testResetPropertiesForRestore() {
        // restore with other key
        String otherKey = "other_key";
        String otherValue = "other_value";

        Map<String, String> properties = Maps.newHashMap();
        properties.put(otherKey, otherValue);
        TableProperty tableProperty = new TableProperty(properties);

        OlapTable olapTable = new OlapTable();
        olapTable.setTableProperty(tableProperty);
        olapTable.setColocateGroup("test_group");
        Assert.assertTrue(olapTable.isColocateTable());

        olapTable.resetPropertiesForRestore(false);
        Assert.assertEquals(tableProperty.getProperties(), olapTable.getTableProperty().getProperties());
        Assert.assertFalse(tableProperty.getDynamicPartitionProperty().isExist());
        Assert.assertFalse(olapTable.isColocateTable());

        // restore with dynamic partition keys
        properties = Maps.newHashMap();
        properties.put(DynamicPartitionProperty.ENABLE, "true");
        properties.put(DynamicPartitionProperty.TIME_UNIT, "HOUR");
        properties.put(DynamicPartitionProperty.TIME_ZONE, "Asia/Shanghai");
        properties.put(DynamicPartitionProperty.START, "-2147483648");
        properties.put(DynamicPartitionProperty.END, "3");
        properties.put(DynamicPartitionProperty.PREFIX, "dynamic");
        properties.put(DynamicPartitionProperty.BUCKETS, "10");
        properties.put(DynamicPartitionProperty.REPLICATION_NUM, "3");
        properties.put(DynamicPartitionProperty.CREATE_HISTORY_PARTITION, "false");

        tableProperty = new TableProperty(properties);
        olapTable.setTableProperty(tableProperty);
        olapTable.resetPropertiesForRestore(false);

        Map<String, String> expectedProperties = Maps.newHashMap(properties);
        expectedProperties.put(DynamicPartitionProperty.ENABLE, "false");
        Assert.assertEquals(expectedProperties, olapTable.getTableProperty().getProperties());
        Assert.assertTrue(olapTable.getTableProperty().getDynamicPartitionProperty().isExist());
        Assert.assertFalse(olapTable.getTableProperty().getDynamicPartitionProperty().getEnable());
    }
}
