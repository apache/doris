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

package org.apache.doris.alter;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.common.Config;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

public class VariantValuesIndexChangeTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("values_index_change");
        connectContext.getSessionVariable().setEnableAddIndexForNewData(true);
    }

    private OlapTable table(String name) throws Exception {
        return (OlapTable) Env.getCurrentInternalCatalog().getDbOrMetaException("values_index_change")
                .getTableOrMetaException(name);
    }

    private void assertRejected(String sql, String message) {
        Exception error = Assertions.assertThrows(Exception.class, () -> executeNereidsSql(sql));
        Assertions.assertTrue(error.getMessage().contains(message), error.getMessage());
    }

    @Test
    public void testAddDropAndReaddBesidePathIndex() throws Exception {
        createTable("CREATE TABLE values_index_change.lifecycle (id INT, v VARIANT, other VARIANT, "
                + "INDEX path_idx(v) USING INVERTED PROPERTIES('parser'='english')) "
                + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1','inverted_index_storage_format'='SNII')");
        OlapTable table = table("lifecycle");
        long pathId = table.getIndexes().get(0).getIndexId();
        executeNereidsSql("ALTER TABLE values_index_change.lifecycle ADD INDEX root_idx(v) USING INVERTED "
                + "PROPERTIES('variant_index_scope'='values','parser'='english')");
        Index root = table.getIndexes().stream().filter(Index::isVariantValuesIndex).findFirst().orElseThrow();
        Assertions.assertEquals("false", root.getProperties().get("support_phrase"));
        Assertions.assertEquals("true", root.getProperties().get("lower_case"));
        Assertions.assertEquals(2, table.getIndexes().size());
        Assertions.assertTrue(Env.getCurrentEnv().getSchemaChangeHandler().getIndexChangeJobs().values().stream()
                .noneMatch(job -> job.getTableId() == table.getId()));
        assertRejected("CREATE INDEX duplicate_root ON values_index_change.lifecycle(v) USING INVERTED "
                + "PROPERTIES('variant_index_scope'='values','parser'='unicode')", "only one values index");
        executeNereidsSql("CREATE INDEX other_root ON values_index_change.lifecycle(other) USING INVERTED "
                + "PROPERTIES('variant_index_scope'='values','analyzer'='english')");
        Assertions.assertEquals(3, table.getIndexes().size());
        executeNereidsSql("DROP INDEX root_idx ON values_index_change.lifecycle");
        Assertions.assertEquals(2, table.getIndexes().size());
        Assertions.assertTrue(table.getIndexes().stream().anyMatch(index -> index.getIndexId() == pathId));
        executeNereidsSql("CREATE INDEX root_idx ON values_index_change.lifecycle(v) USING INVERTED "
                + "PROPERTIES('variant_index_scope'='values','parser'='unicode')");
        Index replacement = table.getIndexes().stream().filter(index -> index.getIndexName().equals("root_idx"))
                .findFirst().orElseThrow();
        Assertions.assertNotEquals(root.getIndexId(), replacement.getIndexId());
        Assertions.assertEquals("unicode", replacement.getProperties().get("parser"));
        assertRejected("BUILD INDEX root_idx ON values_index_change.lifecycle",
                "BUILD INDEX is not supported on a table with a VARIANT values index");
    }

    @Test
    public void testRejectHistoricalAndMixedChangesBeforeMutation() throws Exception {
        createTable("CREATE TABLE values_index_change.guards (id INT, aux INT, v VARIANT) "
                + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1','inverted_index_storage_format'='SNII',"
                + "'bloom_filter_columns'='aux')");
        String add = "ADD INDEX root_idx(v) USING INVERTED "
                + "PROPERTIES('variant_index_scope'='values','parser'='english')";
        OlapTable table = table("guards");
        connectContext.getSessionVariable().setEnableAddIndexForNewData(false);
        try {
            assertRejected("ALTER TABLE values_index_change.guards " + add, "enable_add_index_for_new_data=true");
        } finally {
            connectContext.getSessionVariable().setEnableAddIndexForNewData(true);
        }
        boolean original = Config.enable_light_index_change;
        try {
            Config.enable_light_index_change = false;
            assertRejected("ALTER TABLE values_index_change.guards " + add, "enable_light_index_change=true");
        } finally {
            Config.enable_light_index_change = original;
        }
        for (String clauses : List.of("DROP COLUMN aux, " + add, add + ", DROP COLUMN aux")) {
            assertRejected("ALTER TABLE values_index_change.guards " + clauses,
                    "can not be combined with other schema changes");
        }
        Assertions.assertNotNull(table.getColumn("aux"));
        Assertions.assertTrue(table.getCopiedBfColumns().contains("aux"));
        Assertions.assertTrue(table.getIndexes().isEmpty());
    }

    @Test
    public void testCloudAddRequiresSniiPartitions() throws Exception {
        createTable("CREATE TABLE values_index_change.formats (id INT, v VARIANT) "
                + "DUPLICATE KEY(id) PARTITION BY RANGE(id)(PARTITION p1 VALUES LESS THAN(100)) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1','inverted_index_storage_format'='SNII')");
        OlapTable table = table("formats");
        boolean original = Config.enable_partition_inverted_index_storage_format_rollout;
        try (MockedStatic<Config> config = Mockito.mockStatic(Config.class, Mockito.CALLS_REAL_METHODS)) {
            config.when(Config::isCloudMode).thenReturn(true);
            Config.enable_partition_inverted_index_storage_format_rollout = true;
            table.getTableProperty().getProperties().put("partition.inverted_index_storage_format", "V2");
            assertRejected("ALTER TABLE values_index_change.formats ADD INDEX root_idx(v) USING INVERTED "
                    + "PROPERTIES('variant_index_scope'='values','parser'='english')",
                    "requires SNII for all existing and future partitions");
            table.getTableProperty().getProperties().remove("partition.inverted_index_storage_format");
            table.getPartitionInfo().setInvertedIndexFileStorageFormat(table.getPartition("p1").getId(),
                    TInvertedIndexFileStorageFormat.V2);
            assertRejected("ALTER TABLE values_index_change.formats ADD INDEX root_idx(v) USING INVERTED "
                    + "PROPERTIES('variant_index_scope'='values','parser'='english')",
                    "requires SNII for all existing and future partitions");
        } finally {
            Config.enable_partition_inverted_index_storage_format_rollout = original;
        }
        Assertions.assertTrue(table.getIndexes().isEmpty());
    }

    @Test
    public void testRejectValuesIndexAlterAddOnMor() throws Exception {
        createTable("CREATE TABLE values_index_change.mor_alter (id INT, v VARIANT) "
                + "UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1','inverted_index_storage_format'='SNII',"
                + "'enable_unique_key_merge_on_write'='false')");
        for (String selector : List.of("parser", "analyzer", "built_in_analyzer")) {
            assertRejected("ALTER TABLE values_index_change.mor_alter ADD INDEX root_idx(v) USING INVERTED "
                    + "PROPERTIES('variant_index_scope'='values','" + selector + "'='english')",
                    "UNIQUE_KEYS table with merge_on_write disable");
        }
        Assertions.assertTrue(table("mor_alter").getIndexes().isEmpty());
    }

    @Test
    public void testRejectValuesIndexCreateOnMor() throws Exception {
        createTable("CREATE TABLE values_index_change.mor_create (id INT, v VARIANT) "
                + "UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1','inverted_index_storage_format'='SNII',"
                + "'enable_unique_key_merge_on_write'='false')");
        for (String selector : List.of("parser", "analyzer", "built_in_analyzer")) {
            assertRejected("CREATE INDEX root_idx ON values_index_change.mor_create(v) USING INVERTED "
                    + "PROPERTIES('variant_index_scope'='values','" + selector + "'='english')",
                    "UNIQUE_KEYS table with merge_on_write disable");
        }
        Assertions.assertTrue(table("mor_create").getIndexes().isEmpty());
    }

    @Test
    public void testNewDataOnlyAddInBothModes() {
        for (boolean cloud : List.of(false, true)) {
            try (MockedStatic<Config> config = Mockito.mockStatic(Config.class, Mockito.CALLS_REAL_METHODS)) {
                config.when(Config::isCloudMode).thenReturn(cloud);
                for (String selector : List.of("parser", "analyzer")) {
                    Index index = new Index(1, "root", List.of("v"), IndexType.INVERTED,
                            Map.of("variant_index_scope", "values", selector, "english"), "");
                    Assertions.assertTrue(index.isLightAddIndexSupported(true));
                    Assertions.assertFalse(index.isLightAddIndexSupported(false));
                }
            }
        }
    }
}
