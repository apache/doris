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

package org.apache.hadoop.hive.metastore;

import org.apache.doris.datasource.hive.HmsRawPartitionFilterPageSource;
import org.apache.doris.datasource.hive.HiveVersionUtil.HiveVersion;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HiveMetaStoreClientTest {
    @Test
    public void testPartitionFilterDatabaseNameMatchesHiveVersion() throws Exception {
        HiveConf hiveConf = new HiveConf();
        String prefixed = MetaStoreUtils.prependCatalogToDbName("catalog", "database", hiveConf);

        Assertions.assertEquals("database", HiveMetaStoreClient.databaseNameForPartitionFilter(
                HiveVersion.V1_0, "catalog", "database", hiveConf));
        Assertions.assertEquals("database", HiveMetaStoreClient.databaseNameForPartitionFilter(
                HiveVersion.V2_0, "catalog", "database", hiveConf));
        Assertions.assertEquals("database", HiveMetaStoreClient.databaseNameForPartitionFilter(
                HiveVersion.V2_3, "catalog", "database", hiveConf));
        Assertions.assertEquals(prefixed, HiveMetaStoreClient.databaseNameForPartitionFilter(
                HiveVersion.V3_0, "catalog", "database", hiveConf));
    }

    @Test
    public void testVendoredClientExposesRawPartitionFilterPageContract() {
        Assertions.assertTrue(HmsRawPartitionFilterPageSource.class.isAssignableFrom(HiveMetaStoreClient.class));
    }
}
