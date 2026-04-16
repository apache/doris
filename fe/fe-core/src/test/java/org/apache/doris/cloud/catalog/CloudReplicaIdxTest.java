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

package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Replica;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.JsonObject;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class CloudReplicaIdxTest {

    @Mocked
    Env env;

    @Test
    public void testGsonDeserializeLegacyLongIdx() {
        // Simulate a legacy JSON payload where idx was serialized as a long value.
        // Before this PR, idx was declared as `long`. Gson must coerce long -> int
        // on deserialization for backward compatibility with existing images.
        JsonObject json = new JsonObject();
        json.addProperty("clazz", "CloudReplica");
        json.addProperty("idx", 42L);
        json.addProperty("dbId", 10001L);
        json.addProperty("tableId", 20001L);
        json.addProperty("partitionId", 30001L);
        json.addProperty("indexId", 40001L);

        Replica replica = GsonUtils.GSON.fromJson(json.toString(), Replica.class);
        Assert.assertTrue(replica instanceof CloudReplica);
        Assert.assertEquals(42L, ((CloudReplica) replica).getIdx());
    }

    @Test
    public void testGsonDeserializeNegativeOneIdx() {
        // The default sentinel value for idx is -1.
        JsonObject json = new JsonObject();
        json.addProperty("clazz", "CloudReplica");
        json.addProperty("idx", -1L);

        Replica replica = GsonUtils.GSON.fromJson(json.toString(), Replica.class);
        Assert.assertTrue(replica instanceof CloudReplica);
        Assert.assertEquals(-1L, ((CloudReplica) replica).getIdx());
    }

    @Test
    public void testGsonDeserializeMaxIntIdx() {
        // Verify that Integer.MAX_VALUE (a valid int) deserializes correctly.
        JsonObject json = new JsonObject();
        json.addProperty("clazz", "CloudReplica");
        json.addProperty("idx", (long) Integer.MAX_VALUE);

        Replica replica = GsonUtils.GSON.fromJson(json.toString(), Replica.class);
        Assert.assertTrue(replica instanceof CloudReplica);
        Assert.assertEquals((long) Integer.MAX_VALUE, ((CloudReplica) replica).getIdx());
    }

    @Test
    public void testConstructorOverflowThrows() {
        // Math.toIntExact must throw ArithmeticException for out-of-range values.
        Assert.assertThrows(ArithmeticException.class, () -> {
            new CloudReplica(1L, 1L, Replica.ReplicaState.NORMAL, 1L, 0,
                    1L, 1L, 1L, 1L, (long) Integer.MAX_VALUE + 1);
        });
    }
}
