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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.ConnectorTableKey;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheGovernance;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Memory limits reach the two caches the hive connector owns itself, the directory listings and the derived
 * partition view, and closing the connector gives every reservation back. The HMS caches wrapped around the
 * metastore client are pinned by {@code CachingHmsClientTest} and the hudi sibling's
 * {@code HudiConnectorHmsCacheTest}.
 */
public class HiveConnectorWeightGovernanceTest {
    private static final String PARTITION_DIR = "file:///wh/db/t/dt=1";
    private static final ConnectorTableKey PARTITION_VIEW_KEY = new ConnectorTableKey("db", "t", -1L, -1L);

    @Test
    public void catalogLimitBoundsListingAndPartitionViewUntilTheConnectorCloses() throws Exception {
        long catalogId = Long.MIN_VALUE + 81L;
        long previousGlobalWeight = MetaCacheGovernance.globalEstimatedWeight();
        HiveConnector connector = new HiveConnector(HiveTestProperties.mapWith("meta.cache.max-weight", "1MB"),
                new FakeConnectorContext("weighted", catalogId, Collections.emptyMap()));
        try {
            List<CatalogMetaCache> owners = MetaCacheGovernance.catalogCaches(catalogId);
            Assertions.assertEquals(1, owners.size());
            MetaCache<?, ?> file = owners.get(0).entries().get("hive-file");
            MetaCache<?, ?> partitionView = owners.get(0).entries().get("hive-partition-view");
            Assertions.assertTrue(file.isWeightBounded());
            Assertions.assertTrue(partitionView.isWeightBounded());
            Assertions.assertEquals(1024L * 1024L, file.metrics().getMaxWeight());
            Assertions.assertEquals(1024L * 1024L, partitionView.metrics().getMaxWeight());

            FakeFileSystem fs = new FakeFileSystem().withEntries(
                    FakeFileSystem.file(PARTITION_DIR + "/data1", 100L, 1L),
                    FakeFileSystem.file(PARTITION_DIR + "/data2", 200L, 1L));
            List<HiveFileStatus> files = connector.fileListingCacheForTest().listDataFiles("db", "t", PARTITION_DIR, fs);
            Assertions.assertSame(files,
                    connector.fileListingCacheForTest().listDataFiles("db", "t", PARTITION_DIR, fs));
            List<ConnectorPartitionInfo> partitions = partitions(100);
            Assertions.assertSame(partitions, connector.partitionViewCacheForTest().get(PARTITION_VIEW_KEY,
                    () -> partitions));
            Assertions.assertSame(partitions, connector.partitionViewCacheForTest().get(PARTITION_VIEW_KEY,
                    () -> partitions(100)), "an admitted partition view must be served from the cache");

            long fileWeight = file.metrics().getEstimatedWeight();
            long partitionViewWeight = partitionView.metrics().getEstimatedWeight();
            Assertions.assertTrue(fileWeight > 0L);
            Assertions.assertTrue(partitionViewWeight > 0L);
            Assertions.assertEquals(0L, file.metrics().getWeightRejectCount());
            Assertions.assertEquals(0L, partitionView.metrics().getWeightRejectCount());
            Assertions.assertEquals(previousGlobalWeight + fileWeight + partitionViewWeight,
                    MetaCacheGovernance.globalEstimatedWeight());
        } finally {
            connector.close();
        }
        Assertions.assertTrue(MetaCacheGovernance.catalogCaches(catalogId).isEmpty());
        Assertions.assertEquals(previousGlobalWeight, MetaCacheGovernance.globalEstimatedWeight());
    }

    @Test
    public void entryLimitBoundsOnlyItsOwnEntry() throws Exception {
        long catalogId = Long.MIN_VALUE + 82L;
        HiveConnector connector = new HiveConnector(
                HiveTestProperties.mapWith("meta.cache.hive.partition_view.max-weight", "1MB"),
                new FakeConnectorContext("weighted_entry", catalogId, Collections.emptyMap()));
        try {
            CatalogMetaCache owner = MetaCacheGovernance.catalogCaches(catalogId).get(0);
            Assertions.assertTrue(owner.entries().get("hive-partition-view").isWeightBounded());
            Assertions.assertFalse(owner.entries().get("hive-file").isWeightBounded(),
                    "without a global or catalog limit the listing cache stays count-based");
        } finally {
            connector.close();
        }
    }

    private static List<ConnectorPartitionInfo> partitions(int count) {
        List<ConnectorPartitionInfo> partitions = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String value = String.valueOf(i);
            partitions.add(new ConnectorPartitionInfo("dt=" + value,
                    Collections.singletonMap("dt", value), Collections.emptyMap()));
        }
        return partitions;
    }
}
