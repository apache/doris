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

package org.apache.doris.planner;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.datasource.hive.HMSCachedClient;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.datasource.hive.ThriftHMSCachedClient;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

public class ListPartitionPrunerV2Test {
    @Test
    public void testPartitionValuesMap() throws AnalysisException {
        List<PartitionValue> partitionValues = new ArrayList<>();
        partitionValues.add(new PartitionValue("1.123000"));
        ArrayList<Type> types = new ArrayList<>();
        types.add(ScalarType.DOUBLE);

        // for hive table
        PartitionKey key = PartitionKey.createListPartitionKeyWithTypes(partitionValues, types, true);
        ListPartitionItem listPartitionItem = new ListPartitionItem(Lists.newArrayList(key));
        Map<Long, PartitionItem> idToPartitionItem = Maps.newHashMapWithExpectedSize(partitionValues.size());
        idToPartitionItem.put(1L, listPartitionItem);

        // for olap table
        PartitionKey key2 = PartitionKey.createListPartitionKeyWithTypes(partitionValues, types, false);
        ListPartitionItem listPartitionItem2 = new ListPartitionItem(Lists.newArrayList(key2));
        idToPartitionItem.put(2L, listPartitionItem2);

        Map<Long, List<String>> partitionValuesMap = ListPartitionPrunerV2.getPartitionValuesMap(idToPartitionItem);
        Assert.assertEquals("1.123000", partitionValuesMap.get(1L).get(0));
        Assert.assertEquals("1.123", partitionValuesMap.get(2L).get(0));
    }

    @Test
    public void testInvalidateTable() {

        new MockUp<HMSExternalCatalog>(HMSExternalCatalog.class) {
            @Mock
            public HMSCachedClient getClient() {
                return new ThriftHMSCachedClient(new HiveConf(), 2);
            }
        };

        new MockUp<ThriftHMSCachedClient>(ThriftHMSCachedClient.class) {
            @Mock
            public List<String> listPartitionNames(String dbName, String tblName) {
                // Mock is used here to represent the existence of a partition in the original table
                return new ArrayList<String>() {{
                        add("c1=1.234000");
                    }};
            }
        };

        ThreadPoolExecutor executor = ThreadPoolManager.newDaemonFixedThreadPool(
                10, 10, "mgr", 120, false);
        ThreadPoolExecutor listExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
                10, 10, "mgr", 120, false);
        HiveMetaStoreCache cache = new HiveMetaStoreCache(
                new HMSExternalCatalog(1L, "catalog", null, new HashMap<>(), null), executor, listExecutor);
        ArrayList<Type> types = new ArrayList<>();
        types.add(ScalarType.DOUBLE);

        // test cache
        // the original partition of the table (in mock) will be loaded here
        String dbName = "db";
        String tblName = "tb";
        HiveMetaStoreCache.HivePartitionValues partitionValues = cache.getPartitionValues(dbName, tblName, types);
        Assert.assertEquals(1, partitionValues.getIdToPartitionItem().size());
        Assert.assertTrue(partitionValues.getIdToPartitionItem().containsKey(8882801933302843777L));
        List<PartitionKey> items = partitionValues.getIdToPartitionItem().get(8882801933302843777L).getItems();
        Assert.assertEquals(1, items.size());
        PartitionKey partitionKey = items.get(0);
        Assert.assertEquals("1.234", partitionKey.getKeys().get(0).toString());
        Assert.assertEquals("1.234000", partitionKey.getOriginHiveKeys().get(0));

        // test add cache
        ArrayList<String> values = new ArrayList<>();
        values.add("c1=5.678000");
        cache.addPartitionsCache(dbName, tblName, values, types);
        HiveMetaStoreCache.HivePartitionValues partitionValues2 = cache.getPartitionValues(dbName, tblName, types);
        Assert.assertEquals(2, partitionValues2.getIdToPartitionItem().size());
        Assert.assertTrue(partitionValues2.getIdToPartitionItem().containsKey(7070400225537799947L));
        List<PartitionKey> items2 = partitionValues2.getIdToPartitionItem().get(7070400225537799947L).getItems();
        Assert.assertEquals(1, items2.size());
        PartitionKey partitionKey2 = items2.get(0);
        Assert.assertEquals("5.678", partitionKey2.getKeys().get(0).toString());
        Assert.assertEquals("5.678000", partitionKey2.getOriginHiveKeys().get(0));

        // test refresh table
        // simulates the manually added partition table being deleted, leaving only one original partition in mock
        cache.invalidateTableCache(dbName, tblName);
        HiveMetaStoreCache.HivePartitionValues partitionValues3 = cache.getPartitionValues(dbName, tblName, types);
        Assert.assertEquals(1, partitionValues3.getIdToPartitionItem().size());
        Assert.assertTrue(partitionValues3.getIdToPartitionItem().containsKey(8882801933302843777L));
        List<PartitionKey> items3 = partitionValues3.getIdToPartitionItem().get(8882801933302843777L).getItems();
        Assert.assertEquals(1, items3.size());
        PartitionKey partitionKey3 = items3.get(0);
        Assert.assertEquals("1.234", partitionKey3.getKeys().get(0).toString());
        Assert.assertEquals("1.234000", partitionKey3.getOriginHiveKeys().get(0));
    }
}
