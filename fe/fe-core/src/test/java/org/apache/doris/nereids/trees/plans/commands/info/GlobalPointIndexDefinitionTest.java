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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.AlterTableCommand;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * DDL parsing and validation of the GLOBAL_POINT index type.
 */
public class GlobalPointIndexDefinitionTest {

    private static IndexDefinition newIndexDef(String colName, Map<String, String> properties) {
        return new IndexDefinition("idx_gp", false, Lists.newArrayList(colName), "GLOBAL_POINT",
                properties, "");
    }

    private static void check(IndexDefinition indexDef, Column column) {
        indexDef.checkColumn(column, KeysType.DUP_KEYS, false, TInvertedIndexFileStorageFormat.V2);
    }

    @Test
    public void testDefaultFppIsFilledIn() {
        IndexDefinition indexDef = newIndexDef("event_id", new HashMap<>());
        check(indexDef, new Column("event_id", ScalarType.createVarchar(64), false));
        Assertions.assertEquals("0.01", indexDef.getProperties().get(IndexDefinition.GLOBAL_POINT_FPP_KEY));
    }

    @Test
    public void testAcceptsSupportedTypes() {
        check(newIndexDef("c", new HashMap<>()), new Column("c", PrimitiveType.BIGINT));
        check(newIndexDef("c", new HashMap<>()), new Column("c", PrimitiveType.LARGEINT));
        check(newIndexDef("c", new HashMap<>()), new Column("c", PrimitiveType.DATEV2));
        check(newIndexDef("c", new HashMap<>()), new Column("c", ScalarType.createStringType(), true));
    }

    @Test
    public void testRejectsUnsupportedTypes() {
        Assertions.assertThrows(AnalysisException.class, () -> check(newIndexDef("c", new HashMap<>()),
                new Column("c", new ArrayType(ScalarType.createVarchar(64)), true)));
        Assertions.assertThrows(AnalysisException.class, () -> check(newIndexDef("c", new HashMap<>()),
                new Column("c", PrimitiveType.DOUBLE)));
        Assertions.assertThrows(AnalysisException.class, () -> check(newIndexDef("c", new HashMap<>()),
                new Column("c", ScalarType.createDecimalV3Type(10, 2))));
    }

    @Test
    public void testRejectsMultiColumn() {
        IndexDefinition indexDef = new IndexDefinition("idx_gp", false, Lists.newArrayList("a", "b"),
                "GLOBAL_POINT", new HashMap<>(), "");
        Assertions.assertThrows(AnalysisException.class, indexDef::validate);
    }

    @Test
    public void testRejectsBadProperties() {
        Map<String, String> outOfRange = new HashMap<>();
        outOfRange.put(IndexDefinition.GLOBAL_POINT_FPP_KEY, "0.9");
        Assertions.assertThrows(AnalysisException.class, () -> check(newIndexDef("c", outOfRange),
                new Column("c", ScalarType.createVarchar(64), false)));

        Map<String, String> notANumber = new HashMap<>();
        notANumber.put(IndexDefinition.GLOBAL_POINT_FPP_KEY, "abc");
        Assertions.assertThrows(AnalysisException.class, () -> check(newIndexDef("c", notANumber),
                new Column("c", ScalarType.createVarchar(64), false)));

        Map<String, String> unknownKey = new HashMap<>();
        unknownKey.put("gram_size", "3");
        Assertions.assertThrows(AnalysisException.class, () -> check(newIndexDef("c", unknownKey),
                new Column("c", ScalarType.createVarchar(64), false)));
    }

    @Test
    public void testLightIndexChangeSupported() {
        Index index = new Index(0L, "idx_gp", Lists.newArrayList("event_id"), IndexType.GLOBAL_POINT,
                new HashMap<>(), "");
        Assertions.assertTrue(index.isLightIndexChangeSupported());
        Assertions.assertTrue(index.isLightAddIndexSupported(true));
        Assertions.assertFalse(index.isLightAddIndexSupported(false));
    }

    @Test
    public void testOnlyOneGlobalPointIndexPerColumn() {
        Index first = new Index(1L, "idx_gp1", Lists.newArrayList("event_id"), IndexType.GLOBAL_POINT,
                new HashMap<>(), "");
        Index second = new Index(2L, "idx_gp2", Lists.newArrayList("EVENT_ID"), IndexType.GLOBAL_POINT,
                new HashMap<>(), "");
        Assertions.assertThrows(org.apache.doris.common.AnalysisException.class,
                () -> Index.checkConflict(Lists.newArrayList(first, second), null));
    }

    @Test
    public void testCreateIndexStatementKeepsType() {
        Plan plan = new NereidsParser().parseSingle(
                "CREATE INDEX idx_gp ON t_evt(event_id) USING GLOBAL_POINT PROPERTIES (\"fpp\" = \"0.001\")");
        CreateIndexOp op = (CreateIndexOp) ((AlterTableCommand) plan).getOps().get(0);
        Assertions.assertEquals(IndexType.GLOBAL_POINT, op.getIndexDef().getIndexType());
        Assertions.assertEquals("0.001", op.getIndexDef().getProperties().get(IndexDefinition.GLOBAL_POINT_FPP_KEY));
    }

    @Test
    public void testAlterTableAddIndexKeepsType() {
        Plan plan = new NereidsParser().parseSingle(
                "ALTER TABLE t_evt ADD INDEX idx_gp (event_id) USING GLOBAL_POINT");
        CreateIndexOp op = (CreateIndexOp) ((AlterTableCommand) plan).getOps().get(0);
        Assertions.assertEquals(IndexType.GLOBAL_POINT, op.getIndexDef().getIndexType());
    }
}
