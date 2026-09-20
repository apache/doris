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

package org.apache.doris.catalog.constraint;

import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test that a PrimaryKeyConstraint serialized by an old version (without the
 * "ft" collection field) can be deserialized without throwing a
 * NullPointerException.
 */
public class PrimaryKeyConstraintTest {

    @Test
    public void testDeserializeOldMetadataWithoutForeignTables() {
        PrimaryKeyConstraint constraint = new PrimaryKeyConstraint("pk", ImmutableSet.of("c1"));
        JsonObject json = JsonParser.parseString(GsonUtils.GSON.toJson(constraint)).getAsJsonObject();
        // Old journal entries were written before the foreign table collection existed, so the
        // field is absent from the serialized form. Gson uses Unsafe to instantiate the object and
        // bypasses field initializers, so the collection is null unless gsonPostProcess fixes it.
        json.remove("ft");
        PrimaryKeyConstraint restored = GsonUtils.GSON.fromJson(json.toString(), PrimaryKeyConstraint.class);
        Assertions.assertNotNull(restored);
        Assertions.assertEquals(ImmutableSet.of("c1"), restored.getPrimaryKeyNames());
        // Must not throw NullPointerException on old metadata.
        Assertions.assertTrue(restored.getForeignTables().isEmpty());
    }
}
