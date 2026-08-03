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

import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Which column types a fluss table may be partitioned by and still be readable.
 *
 * <p>The verdicts here were not reasoned out from fluss's naming rules and then trusted: every one of
 * them was run against a fluss cluster through a Doris catalog first, and the split below is where the
 * two sides actually part company. A type that renders into a partition name and back unchanged is
 * allowed; one that fluss rewrites on the way in is refused, because the rewriting loses which character
 * was there ({@code 1_5} was {@code 1.5}, {@code 01-02-03} was {@code 01:02:03}).
 *
 * <p>Completeness is the property worth more than any single verdict: every fluss type root is answered,
 * so a type a future release adds to fluss's partition-key whitelist cannot arrive here as "allowed by
 * omission" and reach fe-core's partition parser, where the failure names neither fluss nor the column.
 */
public class FlussPartitionColumnTypesTest {

    private static final FlussTypeMapping.Options VARBINARY =
            new FlussTypeMapping.Options(true, false);

    /** One type per fluss type root, and the verdict each is meant to get as a PARTITION column. */
    private static Map<DataType, Boolean> verdicts() {
        Map<DataType, Boolean> expected = new LinkedHashMap<>();
        // Stored in the partition name exactly as written, and read back the same way.
        expected.put(DataTypes.CHAR(2), true);
        expected.put(DataTypes.STRING(), true);
        expected.put(DataTypes.BOOLEAN(), true);
        expected.put(DataTypes.TINYINT(), true);
        expected.put(DataTypes.SMALLINT(), true);
        expected.put(DataTypes.INT(), true);
        expected.put(DataTypes.BIGINT(), true);
        expected.put(DataTypes.DATE(), true);
        // Named with the hex text of the bytes, which is a string; readable as long as the catalog is
        // mapping such a column to a string too. The other half of that pair is its own test below.
        expected.put(DataTypes.BINARY(2), true);
        expected.put(DataTypes.BYTES(), true);
        // Fluss allows all five as partition keys and rewrites the '.', ':' and ' ' in their values.
        expected.put(DataTypes.FLOAT(), false);
        expected.put(DataTypes.DOUBLE(), false);
        expected.put(DataTypes.TIME(), false);
        expected.put(DataTypes.TIMESTAMP(3), false);
        expected.put(DataTypes.TIMESTAMP_LTZ(3), false);
        // Fluss refuses these as partition keys itself; refused here as well so that its refusal is not
        // the only thing between them and fe-core's parser.
        expected.put(DataTypes.DECIMAL(20, 4), false);
        expected.put(DataTypes.ARRAY(DataTypes.INT()), false);
        expected.put(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()), false);
        expected.put(DataTypes.ROW(DataTypes.FIELD("f", DataTypes.INT())), false);
        return expected;
    }

    @Test
    public void everyFlussTypeRootHasADeliberateVerdict() {
        Set<DataTypeRoot> covered = EnumSet.noneOf(DataTypeRoot.class);
        for (Map.Entry<DataType, Boolean> entry : verdicts().entrySet()) {
            DataType type = entry.getKey();
            covered.add(type.getTypeRoot());
            boolean readable =
                    FlussPartitionColumnTypes.rejection(type, FlussTypeMapping.Options.DEFAULT) == null;
            Assertions.assertEquals(entry.getValue(), readable, "partition verdict for " + type);
        }
        Assertions.assertEquals(EnumSet.allOf(DataTypeRoot.class), covered,
                "a fluss type root has no verdict here; decide whether a partition of it can be read");
    }

    /**
     * The one verdict the catalog's own settings can flip. Fluss names a BINARY partition with the hex
     * text of its bytes: readable while the column is a Doris string, unreadable the moment
     * {@code enable.mapping.varbinary} turns it into a VARBINARY, which no hex text is a literal of.
     */
    @Test
    public void binaryPartitionsAreReadableOnlyWhileTheColumnIsText() {
        for (DataType type : Arrays.asList(DataTypes.BINARY(2), DataTypes.BYTES())) {
            Assertions.assertNull(
                    FlussPartitionColumnTypes.rejection(type, FlussTypeMapping.Options.DEFAULT),
                    type + " is readable while it maps to a string");
            String rejection = FlussPartitionColumnTypes.rejection(type, VARBINARY);
            Assertions.assertNotNull(rejection, type + " under varbinary mapping");
            // The property that caused it is named, because turning it off is the fix and nothing else
            // about the table changed.
            Assertions.assertTrue(
                    rejection.contains(FlussConnectorProperties.ENABLE_MAPPING_VARBINARY),
                    "the rejection should name the property that caused it: " + rejection);
        }
    }

    /**
     * The catalog's settings must not reach any other verdict. A property about binary columns deciding
     * whether a DATE partition can be read would be a rule nobody could predict from its name.
     */
    @Test
    public void noOtherVerdictDependsOnTheCatalogSettings() {
        for (DataType type : verdicts().keySet()) {
            if (type.getTypeRoot() == DataTypeRoot.BINARY || type.getTypeRoot() == DataTypeRoot.BYTES) {
                continue;
            }
            Assertions.assertEquals(
                    FlussPartitionColumnTypes.rejection(type, FlussTypeMapping.Options.DEFAULT) == null,
                    FlussPartitionColumnTypes.rejection(type, VARBINARY) == null,
                    "the verdict for " + type + " changed with the catalog's mapping options");
        }
    }

    /**
     * A refused type says why in words that name the loss, not just that something is unsupported: the
     * reader of this error has a table fluss created happily and a DESC that looks ordinary.
     */
    @Test
    public void refusalsExplainWhatWasLost() {
        List<DataType> rewritten = Arrays.asList(DataTypes.FLOAT(), DataTypes.DOUBLE(),
                DataTypes.TIME(), DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP_LTZ(3));
        for (DataType type : rewritten) {
            String rejection = FlussPartitionColumnTypes.rejection(type, FlussTypeMapping.Options.DEFAULT);
            Assertions.assertNotNull(rejection, type.toString());
            Assertions.assertTrue(rejection.contains("cannot be read back"),
                    "the rejection for " + type + " should say the value is lost, was: " + rejection);
        }
    }

    /**
     * A timestamp is refused at every precision. Doris can hold the value at six digits or fewer, so a
     * rule written around precision — the one the union-read key gate needs — would let those through
     * and back into the parser that cannot read the name they arrive under.
     */
    @Test
    public void timestampsAreRefusedAtEveryPrecision() {
        for (int precision = 0; precision <= 9; precision++) {
            Assertions.assertNotNull(
                    FlussPartitionColumnTypes.rejection(DataTypes.TIMESTAMP(precision),
                            FlussTypeMapping.Options.DEFAULT),
                    "TIMESTAMP(" + precision + ")");
            Assertions.assertNotNull(
                    FlussPartitionColumnTypes.rejection(DataTypes.TIMESTAMP_LTZ(precision),
                            FlussTypeMapping.Options.DEFAULT),
                    "TIMESTAMP_LTZ(" + precision + ")");
        }
    }

    /**
     * Readability is the weaker of the two questions asked about a partition column: whatever cannot be
     * read at all cannot be matched to a lake split either. Were that ever the other way round, a table
     * would be planned as a lake-plus-tail read on partition values nothing can parse.
     */
    @Test
    public void everythingReadableIsAlsoAskedTheStricterUnionQuestion() {
        for (DataType type : verdicts().keySet()) {
            if (FlussUnionKeyTypes.partitionColumnRejection(type) == null) {
                Assertions.assertNull(
                        FlussPartitionColumnTypes.rejection(type, FlussTypeMapping.Options.DEFAULT),
                        type + " may be matched across the halves but cannot be read at all");
            }
        }
    }
}
