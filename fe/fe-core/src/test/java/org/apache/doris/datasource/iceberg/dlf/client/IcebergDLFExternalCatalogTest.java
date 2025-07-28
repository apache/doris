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

package org.apache.doris.datasource.iceberg.dlf.client;

import org.apache.doris.datasource.iceberg.IcebergDLFExternalCatalog;
import org.apache.doris.nereids.exceptions.NotSupportedException;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class IcebergDLFExternalCatalogTest {
    @Test
    public void testDatabaseList() {
        HashMap<String, String> props = new HashMap<>();
        Configuration conf = new Configuration();

        DLFCachedClientPool cachedClientPool1 = new DLFCachedClientPool(conf, props);
        DLFCachedClientPool cachedClientPool2 = new DLFCachedClientPool(conf, props);
        DLFClientPool dlfClientPool1 = cachedClientPool1.clientPool();
        DLFClientPool dlfClientPool2 = cachedClientPool2.clientPool();
        // This cache should belong to the catalog level,
        // so the object addresses of clients in different pools must be different
        Assert.assertNotSame(dlfClientPool1, dlfClientPool2);
    }

    @Test
    public void testNotSupportOperation() {
        HashMap<String, String> props = new HashMap<>();
        IcebergDLFExternalCatalog catalog = new IcebergDLFExternalCatalog(1, "test", "test", props, "test");
        Assert.assertThrows(NotSupportedException.class, () -> catalog.createDb("db1", true, Maps.newHashMap()));
        Assert.assertThrows(NotSupportedException.class, () -> catalog.dropDb("", true, true));
        Assert.assertThrows(NotSupportedException.class, () -> catalog.dropTable("", "", true, true, true, true));
        Assert.assertThrows(NotSupportedException.class, () -> catalog.truncateTable("", "", null, true, ""));
    }
}
