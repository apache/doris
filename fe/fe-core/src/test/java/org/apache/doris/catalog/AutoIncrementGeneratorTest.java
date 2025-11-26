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

import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;

public class AutoIncrementGeneratorTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test_db");
        createTable("CREATE TABLE test_db.test_auto_inc_table (\n"
                + "    id BIGINT NOT NULL AUTO_INCREMENT,\n"
                + "    name VARCHAR(100)\n"
                + ") DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES (\"replication_num\" = \"1\");");
    }

    /**
     * Test to reproduce the bug where AutoIncrementGenerator.gsonPostProcess
     * sets nextId to batchEndId (-1 by default) after checkpoint and restart.
     *
     * Scenario:
     * 1. Create a table with auto-increment column (batchEndId = -1 by default)
     * 2. Master FE does checkpoint (serializes the table to JSON)
     * 3. Master FE restarts and deserializes the table from JSON
     * 4. gsonPostProcess() is called on AutoIncrementGenerator, setting nextId = batchEndId = -1
     * 5. getAutoIncrementRange() returns id starting from -1, which is WRONG
     */
    @Test
    public void testBugAfterCheckpointRestart() throws Exception {
        // Get the table and its AutoIncrementGenerator
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test_db");
        OlapTable table = (OlapTable) db.getTableOrMetaException("test_auto_inc_table");
        AutoIncrementGenerator generator = table.getAutoIncrementGenerator();

        Assertions.assertNotNull(generator, "AutoIncrementGenerator should not be null");

        long initialBatchEndId = getBatchEndId(generator);
        System.out.println("Initial batchEndId: " + initialBatchEndId);
        Assertions.assertTrue(initialBatchEndId >= 0, "Initial batchEndId should be non-negative");

        // Step 2: Serialize the table to JSON (simulating checkpoint)
        String tableJson = GsonUtils.GSON.toJson(table);
        System.out.println("Serialized table to JSON");

        // Step 3: Deserialize the table from JSON (simulating restart)
        // This will trigger gsonPostProcess() which sets nextId = batchEndId
        OlapTable deserializedTable = GsonUtils.GSON.fromJson(tableJson, OlapTable.class);
        AutoIncrementGenerator deserializedGenerator = deserializedTable.getAutoIncrementGenerator();

        Assertions.assertNotNull(deserializedGenerator,
                "AutoIncrementGenerator should not be null after deserialization");

        // Step 4: Try to get auto-increment range
        // This should trigger the bug where startId is -1
        long columnId = getAutoIncrementColumnId(table);
        Pair<Long, Long> range = deserializedGenerator.getAutoIncrementRange(columnId, 10, 0);

        long startId = range.first;
        System.out.println("Start ID after deserialization: " + startId);

        // BUG REPRODUCED: The startId should be >= 0, but due to gsonPostProcess()
        // setting nextId = batchEndId = -1, we get a negative startId
        if (startId < 0) {
            Assertions.fail("Bug reproduced: startId should not be negative, but got " + startId
                    + ". This is because gsonPostProcess() set nextId to batchEndId (-1)");
        }

        Assertions.assertTrue(startId >= 0, "Start ID should be non-negative, but got " + startId);
    }

    /**
     * Test the normal case without checkpoint/restart - using simple generator
     */
    @Test
    public void testNormalGetAutoIncrementRange() throws UserException {
        long dbId = 1L;
        long tableId = 2L;
        long columnId = 3L;
        long initialNextId = 100L;

        AutoIncrementGenerator generator = new AutoIncrementGenerator(dbId, tableId, columnId, initialNextId);
        EditLog mockEditLog = Mockito.mock(EditLog.class);
        generator.setEditLog(mockEditLog);

        // Get a range without checkpoint/restart
        Pair<Long, Long> range = generator.getAutoIncrementRange(columnId, 10, 0);

        Assertions.assertEquals(100L, range.first.longValue(), "Start ID should be 100");
        Assertions.assertEquals(10L, range.second.longValue(), "Length should be 10");
    }

    /**
     * Test gsonPostProcess behavior when batchEndId is already set
     */
    @Test
    public void testGsonPostProcessWithValidBatchEndId() throws UserException, IOException {
        long dbId = 1L;
        long tableId = 2L;
        long columnId = 3L;
        long initialNextId = 100L;

        AutoIncrementGenerator generator = new AutoIncrementGenerator(dbId, tableId, columnId, initialNextId);
        EditLog mockEditLog = Mockito.mock(EditLog.class);
        generator.setEditLog(mockEditLog);

        // First, get a range to trigger batchEndId update
        generator.getAutoIncrementRange(columnId, 10, 0);

        // At this point, batchEndId should be > 0
        long batchEndId = getBatchEndId(generator);
        Assertions.assertTrue(batchEndId > 0, "batchEndId should be positive after getAutoIncrementRange");

        // Serialize and deserialize to simulate checkpoint/restart
        String json = GsonUtils.GSON.toJson(generator);
        AutoIncrementGenerator deserializedGenerator = GsonUtils.GSON.fromJson(json, AutoIncrementGenerator.class);
        deserializedGenerator.setEditLog(mockEditLog);

        // After deserialization, gsonPostProcess sets nextId = batchEndId
        // This should work correctly when batchEndId > 0
        Pair<Long, Long> range = deserializedGenerator.getAutoIncrementRange(columnId, 10, 0);

        Assertions.assertTrue(range.first >= 0, "Start ID should be positive");
        Assertions.assertEquals(batchEndId, range.first.longValue(), "Start ID should equal previous batchEndId");
    }

    /**
     * Helper method to access private batchEndId field via reflection
     */
    private long getBatchEndId(AutoIncrementGenerator generator) {
        try {
            java.lang.reflect.Field field = AutoIncrementGenerator.class.getDeclaredField("batchEndId");
            field.setAccessible(true);
            return (long) field.get(generator);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get batchEndId", e);
        }
    }

    /**
     * Helper method to get auto-increment column ID from table
     */
    private long getAutoIncrementColumnId(OlapTable table) {
        for (Column column : table.getBaseSchema()) {
            if (column.isAutoInc()) {
                return column.getUniqueId();
            }
        }
        throw new RuntimeException("No auto-increment column found in table");
    }
}
