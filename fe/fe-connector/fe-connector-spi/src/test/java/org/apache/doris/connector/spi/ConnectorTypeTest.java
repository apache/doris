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

package org.apache.doris.connector.spi;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Pins the complex-type shape contract enforced by the {@link ConnectorType} constructor.
 *
 * <p><b>WHY this matters:</b> a complex type is described by PARALLEL lists — one of child types, one
 * of STRUCT field names, plus optional per-child metadata. Nothing downstream can detect that they
 * disagree: fe-core's converter fills a missing STRUCT field name with {@code "col" + index} and turns
 * a childless ARRAY/MAP into {@code ARRAY<NULL>}/{@code MAP<NULL,NULL>}, all silently. The connector
 * that got it wrong compiles, the table loads, DESCRIBE prints something — and the user only finds out
 * when a query that names a real sub-field is rejected as "field name not found", by which point the
 * error site is nowhere near the type mapping that caused it. So the invariant is enforced where the
 * mistake is made: at construction.
 */
public class ConnectorTypeTest {

    private static final ConnectorType INT = ConnectorType.of("INT");
    private static final ConnectorType STR = ConnectorType.of("STRING");

    private static ConnectorType struct(List<ConnectorType> children, List<String> names) {
        return new ConnectorType("STRUCT", -1, -1, children, names);
    }

    // ---------- STRUCT: field names are mandatory and parallel ----------

    @Test
    public void testStructWithoutFieldNamesRejected() {
        // Exactly what the trino ROW mapping used to build.
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ConnectorType("STRUCT", -1, -1, Arrays.asList(INT, STR)));
        // Both counts belong in the message: "they disagree" without the numbers does not locate the bug.
        Assertions.assertTrue(e.getMessage().contains("(0)") && e.getMessage().contains("(2)"), e.getMessage());
    }

    @Test
    public void testStructWithTooFewFieldNamesRejected() {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> struct(Arrays.asList(INT, STR), Collections.singletonList("a")));
        Assertions.assertTrue(e.getMessage().contains("(1)") && e.getMessage().contains("(2)"), e.getMessage());
    }

    @Test
    public void testStructWithTooManyFieldNamesRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> struct(Collections.singletonList(INT), Arrays.asList("a", "b")));
    }

    @Test
    public void testStructWithNoChildrenRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> struct(Collections.emptyList(), Collections.emptyList()));
    }

    @Test
    public void testStructWithNullFieldNameRejected() {
        // A null name is unresolvable in exactly the same way a missing one is.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> struct(Arrays.asList(INT, STR), Arrays.asList("a", null)));
    }

    @Test
    public void testLowercaseStructTagStillValidated() {
        // The tag is compared case-insensitively so a spelling variant cannot dodge the check.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ConnectorType("struct", -1, -1, Arrays.asList(INT, STR)));
    }

    // ---------- ARRAY / MAP: fixed arity ----------

    @Test
    public void testArrayArityEnforced() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ConnectorType("ARRAY", -1, -1, Collections.emptyList()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ConnectorType("ARRAY", -1, -1, Arrays.asList(INT, STR)));
    }

    @Test
    public void testMapArityEnforced() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ConnectorType("MAP", -1, -1, Collections.singletonList(INT)));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ConnectorType("MAP", -1, -1, Arrays.asList(INT, STR, INT)));
    }

    // ---------- optional per-child lists ----------

    @Test
    public void testOptionalListLongerThanChildrenRejected() {
        // An entry with no child to belong to cannot be interpreted at all.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ConnectorType("STRUCT", -1, -1, Collections.singletonList(INT),
                        Collections.singletonList("a"), Arrays.asList(true, false),
                        Collections.emptyList(), Collections.emptyList()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ConnectorType("ARRAY", -1, -1, Collections.singletonList(INT),
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
                        Arrays.asList(1, 2)));
    }

    @Test
    public void testOptionalListShorterThanChildrenStillLegal() {
        // Deliberately NOT rejected: these four are read index-tolerantly ("not carried for that index"
        // -> default), which is the documented behavior every legacy factory relies on. Tightening this
        // to exact-length would hard-fail a supported state.
        ConnectorType ct = new ConnectorType("STRUCT", -1, -1, Arrays.asList(INT, STR),
                Arrays.asList("a", "b"), Collections.singletonList(false),
                Collections.emptyList(), Collections.emptyList());
        Assertions.assertFalse(ct.isChildNullable(0));
        Assertions.assertTrue(ct.isChildNullable(1));
        Assertions.assertNull(ct.getChildComment(0));
        Assertions.assertEquals(-1, ct.getChildFieldId(0));
    }

    // ---------- every legal construction path must stay legal ----------

    @Test
    public void testFactoriesRemainValid() {
        Assertions.assertEquals(1, ConnectorType.arrayOf(INT).getChildren().size());
        Assertions.assertEquals(1, ConnectorType.arrayOf(INT, false).getChildren().size());
        Assertions.assertEquals(2, ConnectorType.mapOf(STR, INT).getChildren().size());
        Assertions.assertEquals(2, ConnectorType.mapOf(STR, INT, false).getChildren().size());

        List<String> names = Arrays.asList("a", "b");
        List<ConnectorType> types = Arrays.asList(INT, STR);
        Assertions.assertEquals(names, ConnectorType.structOf(names, types).getFieldNames());
        Assertions.assertEquals(names, ConnectorType.structOf(names, types,
                Arrays.asList(true, false), Arrays.asList("c1", "c2")).getFieldNames());
        Assertions.assertEquals(names, ConnectorType.structOf(names, types,
                Arrays.asList(true, false), Arrays.asList("c1", "c2"),
                Arrays.asList(true, true)).getFieldNames());
    }

    @Test
    public void testWithChildrenFieldIdsRemainsValid() {
        // The iceberg usage: one id per child on ARRAY (1), MAP (2) and STRUCT (N).
        Assertions.assertEquals(7, ConnectorType.arrayOf(INT)
                .withChildrenFieldIds(Collections.singletonList(7)).getChildFieldId(0));
        Assertions.assertEquals(9, ConnectorType.mapOf(STR, INT)
                .withChildrenFieldIds(Arrays.asList(8, 9)).getChildFieldId(1));
        Assertions.assertEquals(3, ConnectorType.structOf(Arrays.asList("a", "b"), Arrays.asList(INT, STR))
                .withChildrenFieldIds(Arrays.asList(2, 3)).getChildFieldId(1));
    }

    @Test
    public void testNonComplexTypesUnaffected() {
        // typeName is a free-form string with no vocabulary, so an unrecognized tag must not be
        // second-guessed - we cannot conclude "then it has no children".
        Assertions.assertEquals("JSONB", ConnectorType.of("JSONB").getTypeName());
        Assertions.assertEquals(10, ConnectorType.of("DECIMALV3", 10, 2).getPrecision());
        Assertions.assertEquals("SOMETHING",
                new ConnectorType("SOMETHING", -1, -1, Arrays.asList(INT, STR)).getTypeName());
    }
}
