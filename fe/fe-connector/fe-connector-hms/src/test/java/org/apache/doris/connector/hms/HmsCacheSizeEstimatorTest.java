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

package org.apache.doris.connector.hms;

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheBudgetManager;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.MetaCacheSizeEstimate;
import org.apache.doris.connector.cache.ReflectiveObjectSizeEstimator;
import org.apache.doris.connector.cache.ScopePath;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

class HmsCacheSizeEstimatorTest {

    @Test
    void wideTablesAreWeighedCompletelyAndLinearly() {
        // A walk of the whole table ran out of the 10,000-visit budget at 995 INT or 382 struct columns, so a wider
        // table never entered a weighted cache. MUTATION: estimating the table as one graph -> incomplete -> red.
        for (String hiveType : List.of("int", "struct<a:int,b:string>")) {
            MetaCacheSizeEstimate half = HmsCacheSizeEstimator.estimateTable("db.wide", table(1_500, hiveType));
            HmsTableInfo table = table(3_000, hiveType);
            MetaCacheSizeEstimate full = HmsCacheSizeEstimator.estimateTable("db.wide", table);

            Assertions.assertTrue(full.isComplete(), hiveType + ": " + full.getIncompleteReason());
            // Never below an unbounded walk of the value; above it only by the objects that columns share.
            long walked = ReflectiveObjectSizeEstimator.estimateComplete("db.wide", Integer.MAX_VALUE, 20)
                    + ReflectiveObjectSizeEstimator.estimateComplete(table, Integer.MAX_VALUE, 20);
            Assertions.assertTrue(full.getBytes() >= walked, hiveType + ": " + full.getBytes() + " < " + walked);
            Assertions.assertTrue(full.getBytes() <= walked * 2,
                    hiveType + ": " + full.getBytes() + " > 2 * " + walked);
            double growth = (double) full.getBytes() / half.getBytes();
            Assertions.assertTrue(growth > 1.8 && growth < 2.2, hiveType + ": doubling the columns grew it " + growth);
        }
    }

    @Test
    void wideTableIsCachedUnderAWeightLimit() {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.of(128L << 20));
        try (CatalogMetaCache owner = new CatalogMetaCache(manager, 1L, "hive", Collections.emptyMap())) {
            MetaCache<String, HmsTableInfo> entry = owner.create(MetaCacheDefinition
                    .<String, HmsTableInfo>builder("hms-table", CacheSpec.of(true, -1L, 100L),
                            ignored -> ScopePath.catalog())
                    .sizeEstimator(HmsCacheSizeEstimator::estimateTable)
                    .build());
            HmsTableInfo wide = table(3_000, "struct<a:int,b:string>");
            AtomicInteger loads = new AtomicInteger();
            for (int i = 0; i < 3; i++) {
                Assertions.assertSame(wide, entry.get("db.wide", ignored -> {
                    loads.incrementAndGet();
                    return wide;
                }));
            }

            Assertions.assertEquals(1, loads.get());
            Assertions.assertSame(wide, entry.getIfPresent("db.wide"));
            Assertions.assertEquals(0L, entry.metrics().getWeightRejectCount());
            Assertions.assertTrue(entry.metrics().getEstimatedWeight() > 0L);
        }
    }

    private static HmsTableInfo table(int columns, String hiveType) {
        List<ConnectorColumn> dataColumns = new ArrayList<>(columns);
        for (int i = 0; i < columns; i++) {
            dataColumns.add(new ConnectorColumn("col_" + i, HmsTypeMapping.toConnectorType(hiveType), "comment " + i,
                    true, null, true));
        }
        Map<String, String> parameters = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            parameters.put("param_" + i, "value_" + i);
        }
        return HmsTableInfo.builder().dbName("db").tableName("wide").tableType("EXTERNAL_TABLE")
                .parameters(parameters).columns(dataColumns)
                .partitionKeys(Collections.singletonList(
                        new ConnectorColumn("dt", ConnectorType.of("STRING"), null, true, null, true)))
                .location("hdfs://nn/warehouse/db/wide")
                .inputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
                .outputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")
                .serializationLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
                .build();
    }
}
