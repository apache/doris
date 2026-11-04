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

import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * A PrimaryKeyConstraint written before its foreign table collections existed must load: FE replays every journal
 * from the first one without a checkpoint image or with metadata_failure_recovery.
 */
public class PrimaryKeyConstraintTest {

    @Test
    public void testDeserializeOldMetadataWithoutForeignTables() {
        PrimaryKeyConstraint constraint = new PrimaryKeyConstraint("pk", ImmutableSet.of("c1"));
        JsonObject json = JsonParser.parseString(GsonUtils.GSON.toJson(constraint)).getAsJsonObject();
        // Gson instantiates the object without its field initializers, so a collection absent from the JSON stays
        // null unless gsonPostProcess fills it in.
        json.remove("ft");
        json.remove("ftn");
        json.remove("ftni");

        PrimaryKeyConstraint restored = GsonUtils.GSON.fromJson(json.toString(), PrimaryKeyConstraint.class);
        Assertions.assertEquals(ImmutableSet.of("c1"), restored.getPrimaryKeyNames());
        Assertions.assertTrue(restored.getForeignTableInfos().isEmpty());

        // The restored constraint keeps track of its foreign tables like a new one.
        TableNameInfo foreignTable = new TableNameInfo("internal", "db1", "t1");
        restored.addForeignTable(foreignTable);
        Assertions.assertEquals(1, restored.getForeignTableInfos().size());
        TableNameInfo renamed = new TableNameInfo("internal", "db1", "t2");
        restored.renameForeignTable(foreignTable, renamed);
        Assertions.assertEquals("t2", restored.getForeignTableInfos().get(0).getTbl());
        restored.removeForeignTable(renamed);
        Assertions.assertTrue(restored.getForeignTableInfos().isEmpty());
    }
}
