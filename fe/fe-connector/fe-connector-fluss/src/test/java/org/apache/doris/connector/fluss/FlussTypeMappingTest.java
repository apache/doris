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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.api.ConnectorType;

import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pins what a fluss column looks like once Doris describes it.
 *
 * <p>Two properties are worth more than the individual rows. The first is completeness: a fluss
 * release that adds a type must not slip through as a silently degraded column, so
 * {@link #everyFlussTypeRootIsMapped} fails the build when a {@link DataTypeRoot} has no case here.
 * The second is that these rules are not free choices — they are the composition of fluss's own
 * fluss-to-paimon conversion with the paimon connector's mapping, because a datalake table is
 * readable both as {@code tbl} (this mapping) and as {@code tbl$lake} (the paimon connector), and one
 * table must not show two schemas.
 */
public class FlussTypeMappingTest {

    private static ConnectorType map(DataType type) {
        return FlussTypeMapping.toConnectorType(type, FlussTypeMapping.Options.DEFAULT);
    }

    private static ConnectorType map(DataType type, boolean varbinary, boolean timestampTz) {
        return FlussTypeMapping.toConnectorType(type, new FlussTypeMapping.Options(varbinary, timestampTz));
    }

    @Test
    public void everyFlussTypeRootIsMapped() {
        Map<DataType, ConnectorType> expected = new LinkedHashMap<>();
        expected.put(DataTypes.CHAR(10), ConnectorType.of("CHAR", 10, 0));
        expected.put(DataTypes.STRING(), ConnectorType.of("STRING"));
        expected.put(DataTypes.BOOLEAN(), ConnectorType.of("BOOLEAN"));
        expected.put(DataTypes.BINARY(16), ConnectorType.of("STRING"));
        expected.put(DataTypes.BYTES(), ConnectorType.of("STRING"));
        expected.put(DataTypes.DECIMAL(20, 4), ConnectorType.of("DECIMALV3", 20, 4));
        expected.put(DataTypes.TINYINT(), ConnectorType.of("TINYINT"));
        expected.put(DataTypes.SMALLINT(), ConnectorType.of("SMALLINT"));
        expected.put(DataTypes.INT(), ConnectorType.of("INT"));
        expected.put(DataTypes.BIGINT(), ConnectorType.of("BIGINT"));
        expected.put(DataTypes.FLOAT(), ConnectorType.of("FLOAT"));
        expected.put(DataTypes.DOUBLE(), ConnectorType.of("DOUBLE"));
        expected.put(DataTypes.DATE(), ConnectorType.of("DATEV2"));
        expected.put(DataTypes.TIME(3), ConnectorType.of("UNSUPPORTED"));
        expected.put(DataTypes.TIMESTAMP(6), ConnectorType.of("DATETIMEV2", 6, 0));
        expected.put(DataTypes.TIMESTAMP_LTZ(6), ConnectorType.of("DATETIMEV2", 6, 0));
        expected.put(DataTypes.ARRAY(DataTypes.INT()), ConnectorType.arrayOf(ConnectorType.of("INT")));
        expected.put(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                ConnectorType.mapOf(ConnectorType.of("STRING"), ConnectorType.of("INT")));
        expected.put(DataTypes.ROW(DataTypes.FIELD("f", DataTypes.INT())),
                ConnectorType.structOf(singleton("f"), singleton(ConnectorType.of("INT"))));

        Set<DataTypeRoot> covered = EnumSet.noneOf(DataTypeRoot.class);
        for (Map.Entry<DataType, ConnectorType> entry : expected.entrySet()) {
            covered.add(entry.getKey().getTypeRoot());
            Assertions.assertEquals(entry.getValue(), map(entry.getKey()),
                    "mapping of fluss " + entry.getKey());
        }
        // The gate: fluss owns this enum, so a new root appearing here means fluss grew a type that
        // nobody has decided how to show in Doris yet. Decide it, then add the row above.
        Assertions.assertEquals(EnumSet.allOf(DataTypeRoot.class), covered,
                "every fluss type root needs a case; missing: "
                        + complement(covered));
    }

    @Test
    public void charLongerThanTheDorisMaximumBecomesString() {
        // Doris CHAR stops at 255. Truncating would lose data and failing would make the table
        // unloadable, so the column widens to STRING - the same relief valve the paimon connector uses,
        // which is what keeps tbl and tbl$lake agreeing on a CHAR(1000) column.
        Assertions.assertEquals(ConnectorType.of("CHAR", 255, 0), map(DataTypes.CHAR(255)));
        Assertions.assertEquals(ConnectorType.of("STRING"), map(DataTypes.CHAR(256)));
    }

    @Test
    public void timestampPrecisionIsClampedToMicroseconds() {
        // Fluss keeps up to nanoseconds, Doris DATETIMEV2 up to microseconds. The clamp must be a clamp
        // and not an error: a nanosecond column still reads, it just reads with microsecond scale.
        Assertions.assertEquals(ConnectorType.of("DATETIMEV2", 0, 0), map(DataTypes.TIMESTAMP(0)));
        Assertions.assertEquals(ConnectorType.of("DATETIMEV2", 3, 0), map(DataTypes.TIMESTAMP(3)));
        Assertions.assertEquals(ConnectorType.of("DATETIMEV2", 6, 0), map(DataTypes.TIMESTAMP(9)));
        Assertions.assertEquals(ConnectorType.of("DATETIMEV2", 6, 0), map(DataTypes.TIMESTAMP_LTZ(9)));
        Assertions.assertEquals(ConnectorType.of("TIMESTAMPTZ", 6, 0),
                map(DataTypes.TIMESTAMP_LTZ(9), false, true));
    }

    @Test
    public void timeIsMarkedUnsupportedRatherThanReinterpreted() {
        // Doris has no storable TIME column. The two tempting substitutes both lie about the value -
        // STRING changes its type and the elapsed-millis INT other engines use changes its meaning - so
        // the column is marked unsupported and only a query that projects it fails.
        Assertions.assertEquals(ConnectorType.of("UNSUPPORTED"), map(DataTypes.TIME()));
        Assertions.assertEquals(ConnectorType.of("UNSUPPORTED"), map(DataTypes.TIME(3)));
    }

    @Test
    public void anUnsupportedLeafDoesNotSinkTheColumnsAroundIt() {
        // The marker degrades exactly one leaf. A wide table with one TIME field inside a struct still
        // loads, and its neighbouring fields stay readable - that is the whole reason this is a marker
        // and not an exception.
        ConnectorType struct = map(DataTypes.ROW(
                DataTypes.FIELD("started", DataTypes.TIME(3)),
                DataTypes.FIELD("id", DataTypes.BIGINT())));

        Assertions.assertEquals("STRUCT", struct.getTypeName());
        Assertions.assertEquals(ConnectorType.of("UNSUPPORTED"), struct.getChildren().get(0));
        Assertions.assertEquals(ConnectorType.of("BIGINT"), struct.getChildren().get(1));
    }

    @Test
    public void theBinaryFamilyFollowsTheVarbinarySwitch() {
        // Off by default (STRING), because that is what every other Doris catalog does with binary
        // columns and flipping the default would change what existing queries return.
        Assertions.assertEquals(ConnectorType.of("STRING"), map(DataTypes.BINARY(16)));
        Assertions.assertEquals(ConnectorType.of("STRING"), map(DataTypes.BYTES()));

        // On: fixed-length BINARY(n) keeps n as the VARBINARY bound; unbounded BYTES declares no length
        // so fe-core fills in the Doris VARBINARY maximum.
        Assertions.assertEquals(ConnectorType.of("VARBINARY", 16, 0), map(DataTypes.BINARY(16), true, false));
        Assertions.assertEquals(ConnectorType.of("VARBINARY"), map(DataTypes.BYTES(), true, false));
    }

    @Test
    public void timestampLtzFollowsTheTimestampTzSwitch() {
        Assertions.assertEquals(ConnectorType.of("DATETIMEV2", 3, 0), map(DataTypes.TIMESTAMP_LTZ(3)));
        Assertions.assertEquals(ConnectorType.of("TIMESTAMPTZ", 3, 0),
                map(DataTypes.TIMESTAMP_LTZ(3), false, true));

        // The two switches are independent: neither reads the other's property.
        Assertions.assertEquals(ConnectorType.of("DATETIMEV2", 3, 0),
                map(DataTypes.TIMESTAMP_LTZ(3), true, false));
        Assertions.assertEquals(ConnectorType.of("VARBINARY", 4, 0),
                map(DataTypes.BINARY(4), true, true));
    }

    @Test
    public void nestedTypesRecurseWithTheSameRulesAndSwitches() {
        // ARRAY<ROW<...>> and MAP<STRING, ARRAY<INT>>: the rules that apply to a top-level column apply
        // at any depth, switches included - a BINARY buried three levels down must not quietly ignore
        // enable.mapping.varbinary while the top-level one honours it.
        ConnectorType arrayOfStruct = map(DataTypes.ARRAY(DataTypes.ROW(
                DataTypes.FIELD("k", DataTypes.CHAR(300)),
                DataTypes.FIELD("v", DataTypes.BINARY(8)))), true, false);
        Assertions.assertEquals(
                ConnectorType.arrayOf(ConnectorType.structOf(
                        pair("k", "v"),
                        pair(ConnectorType.of("STRING"), ConnectorType.of("VARBINARY", 8, 0)))),
                arrayOfStruct);

        Assertions.assertEquals(
                ConnectorType.mapOf(ConnectorType.of("STRING"),
                        ConnectorType.arrayOf(ConnectorType.of("INT"))),
                map(DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT()))));

        // MAP is mapped, not marked unsupported: fluss reads it in the ARROW log format and in the
        // compacted KV format, and the lake sibling served by the paimon connector shows it as a MAP,
        // so refusing it here would give one table two different schemas.
        Assertions.assertEquals(
                ConnectorType.mapOf(ConnectorType.of("INT"),
                        ConnectorType.structOf(singleton("n"), singleton(ConnectorType.of("DOUBLE")))),
                map(DataTypes.MAP(DataTypes.INT(),
                        DataTypes.ROW(DataTypes.FIELD("n", DataTypes.DOUBLE())))));
    }

    @Test
    public void structFieldsKeepTheirDeclaredNullabilityAndComment() {
        // These two never affect the type's identity, only what DESCRIBE and SHOW CREATE TABLE print.
        // Dropping them would silently report every nested field as a nullable, undocumented column.
        ConnectorType struct = map(DataTypes.ROW(
                DataTypes.FIELD("required", DataTypes.INT().copy(false), "the id"),
                DataTypes.FIELD("optional", DataTypes.STRING())));

        Assertions.assertEquals(pair("required", "optional"), struct.getFieldNames());
        Assertions.assertFalse(struct.isChildNullable(0), "a NOT NULL fluss field must not read as nullable");
        Assertions.assertEquals("the id", struct.getChildComment(0));
        Assertions.assertTrue(struct.isChildNullable(1));
        Assertions.assertNull(struct.getChildComment(1), "a field with no description has no comment");
    }

    private static Set<DataTypeRoot> complement(Set<DataTypeRoot> covered) {
        Set<DataTypeRoot> missing = EnumSet.allOf(DataTypeRoot.class);
        missing.removeAll(covered);
        return missing;
    }

    private static <T> List<T> singleton(T value) {
        return Collections.singletonList(value);
    }

    private static <T> List<T> pair(T first, T second) {
        return Arrays.asList(first, second);
    }
}
