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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.View;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

/**
 * Regression for CREATE ROW POLICY column resolution on a VIEW whose body reads an external
 * catalog. A table keys its nameToColumn map by {@link Column#getDefineName()}; after FE metadata
 * replay such a view can carry columns whose defineName diverges from their name, so the map-based
 * {@code getColumn(name)} misses a column that genuinely exists — while DESCRIBE, query planning
 * and the row-policy runtime all resolve it via the full schema. The CREATE ROW POLICY predicate
 * guard must resolve the same way, so it uses {@link CreatePolicyCommand#columnExistsInSchema}
 * (a getFullSchema() scan) rather than the defineName-keyed map lookup.
 */
public class CreatePolicyColumnResolutionTest {

    private Column columnWithDefineName(String name, String defineName) throws Exception {
        Column c = new Column(name, PrimitiveType.INT);
        // There is no public setter; the divergent defineName is a replay/deserialize artifact,
        // so set it directly to reproduce the reloaded cross-catalog-view state.
        Field f = Column.class.getDeclaredField("defineName");
        f.setAccessible(true);
        f.set(c, defineName);
        return c;
    }

    @Test
    public void testDefineNameDivergentColumnResolvesViaFullSchema() throws Exception {
        // A reloaded view over an external catalog: the column's defineName is the qualified
        // source name, so the nameToColumn map is keyed by it and getColumn("region") misses.
        Column region = columnWithDefineName("region", "ext_catalog.some_db.some_table.region");
        View view = new View(1L, "v_over_external", Lists.newArrayList(region));

        // The defineName-keyed map lookup (getColumn) misses the real column...
        Assertions.assertNull(view.getColumn("region"));
        // ...while the full-schema scan the guard uses resolves it, case-insensitively...
        Assertions.assertTrue(CreatePolicyCommand.columnExistsInSchema(view, "region"));
        Assertions.assertTrue(CreatePolicyCommand.columnExistsInSchema(view, "REGION"));
        // ...and a genuinely absent column is still rejected (no silent acceptance).
        Assertions.assertFalse(CreatePolicyCommand.columnExistsInSchema(view, "no_such_column"));
    }

    @Test
    public void testNormallyKeyedColumnResolvesBothWays() {
        // A normal column (defineName == name): both the map lookup and the full-schema scan find it.
        Column id = new Column("id", PrimitiveType.INT);
        View view = new View(2L, "plain_view", Lists.newArrayList(id));
        Assertions.assertNotNull(view.getColumn("id"));
        Assertions.assertTrue(CreatePolicyCommand.columnExistsInSchema(view, "id"));
        Assertions.assertFalse(CreatePolicyCommand.columnExistsInSchema(view, "missing"));
    }
}
