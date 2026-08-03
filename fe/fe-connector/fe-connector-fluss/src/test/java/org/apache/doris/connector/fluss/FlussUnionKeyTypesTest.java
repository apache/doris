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
 * Which columns a primary-key table can be read by when its lake and its log tail are read as one.
 *
 * <p>The property worth more than any single row is completeness: every one of fluss's type roots has a
 * deliberate verdict here, so a type a future fluss release adds cannot slip in as "allowed by omission".
 * The rules themselves are one-directional in their risk — allowing a type that does not compare
 * identically on both sides returns wrong rows silently, refusing one that would have been fine only
 * costs the lake's speed — so the verdicts below are written to be defensible in that direction.
 */
public class FlussUnionKeyTypesTest {

    /** One type per fluss type root, and the verdict each is meant to get as a KEY column. */
    private static Map<DataType, Boolean> keyVerdicts() {
        Map<DataType, Boolean> expected = new LinkedHashMap<>();
        expected.put(DataTypes.BOOLEAN(), true);
        expected.put(DataTypes.TINYINT(), true);
        expected.put(DataTypes.SMALLINT(), true);
        expected.put(DataTypes.INT(), true);
        expected.put(DataTypes.BIGINT(), true);
        expected.put(DataTypes.DECIMAL(20, 4), true);
        expected.put(DataTypes.CHAR(10), true);
        expected.put(DataTypes.STRING(), true);
        expected.put(DataTypes.BINARY(16), true);
        expected.put(DataTypes.BYTES(), true);
        expected.put(DataTypes.DATE(), true);
        expected.put(DataTypes.TIMESTAMP(6), true);
        expected.put(DataTypes.TIMESTAMP_LTZ(6), true);
        // Refused, each for its own reason: two encodings of one number and a NaN that equals nothing;
        // a type Doris cannot represent at all; and the three fluss itself already refuses as a key,
        // covered here so the switch stays exhaustive rather than relying on that.
        expected.put(DataTypes.FLOAT(), false);
        expected.put(DataTypes.DOUBLE(), false);
        expected.put(DataTypes.TIME(), false);
        expected.put(DataTypes.ARRAY(DataTypes.INT()), false);
        expected.put(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()), false);
        expected.put(DataTypes.ROW(DataTypes.FIELD("f", DataTypes.INT())), false);
        return expected;
    }

    @Test
    public void everyFlussTypeRootHasADeliberateVerdict() {
        Set<DataTypeRoot> covered = EnumSet.noneOf(DataTypeRoot.class);
        for (Map.Entry<DataType, Boolean> entry : keyVerdicts().entrySet()) {
            DataType type = entry.getKey();
            covered.add(type.getTypeRoot());
            boolean allowed = FlussUnionKeyTypes.keyColumnRejection(type) == null;
            Assertions.assertEquals(entry.getValue(), allowed, "key verdict for " + type);
            // Every root is also asked the partition question, so neither switch can be the one that
            // silently falls through on a type fluss adds.
            Assertions.assertEquals(type.getTypeRoot() == DataTypeRoot.STRING,
                    FlussUnionKeyTypes.partitionColumnRejection(type) == null,
                    "partition verdict for " + type);
        }
        Assertions.assertEquals(EnumSet.allOf(DataTypeRoot.class), covered,
                "a fluss type root has no verdict here; decide whether it can be a union-read key");
    }

    /**
     * Doris stops at microseconds, so a finer fluss timestamp arrives rounded — and two keys that differ
     * only in the digits it rounded away become one. Over-matching drops a row that should have been
     * returned, which no row count reveals.
     */
    @Test
    public void timestampFinerThanDorisCanHoldIsRefused() {
        for (int precision = 0; precision <= 6; precision++) {
            Assertions.assertNull(FlussUnionKeyTypes.keyColumnRejection(DataTypes.TIMESTAMP(precision)),
                    "TIMESTAMP(" + precision + ")");
            Assertions.assertNull(FlussUnionKeyTypes.keyColumnRejection(DataTypes.TIMESTAMP_LTZ(precision)),
                    "TIMESTAMP_LTZ(" + precision + ")");
        }
        for (int precision = 7; precision <= 9; precision++) {
            Assertions.assertNotNull(FlussUnionKeyTypes.keyColumnRejection(DataTypes.TIMESTAMP(precision)),
                    "TIMESTAMP(" + precision + ")");
            Assertions.assertNotNull(
                    FlussUnionKeyTypes.keyColumnRejection(DataTypes.TIMESTAMP_LTZ(precision)),
                    "TIMESTAMP_LTZ(" + precision + ")");
        }
    }

    /**
     * Fluss accepts every one of these as a partition key ({@code PartitionUtils}), so refusing all but
     * STRING is this connector's own rule, not a restriction inherited from below: a partition value
     * arrives here already rendered, by two different systems, and only a string is guaranteed to come
     * out the same on both sides.
     */
    @Test
    public void everyPartitionKeyTypeFlussAllowsButStringIsRefused() {
        List<DataType> flussAllows = Arrays.asList(DataTypes.CHAR(8), DataTypes.BOOLEAN(),
                DataTypes.BINARY(4), DataTypes.BYTES(), DataTypes.TINYINT(), DataTypes.SMALLINT(),
                DataTypes.INT(), DataTypes.BIGINT(), DataTypes.DATE(), DataTypes.TIME());
        for (DataType type : flussAllows) {
            Assertions.assertNotNull(FlussUnionKeyTypes.partitionColumnRejection(type), type.toString());
        }
        Assertions.assertNull(FlussUnionKeyTypes.partitionColumnRejection(DataTypes.STRING()));
    }

    /** Nullability is not part of the question — a key column is NOT NULL in fluss either way. */
    @Test
    public void nullabilityDoesNotChangeTheVerdict() {
        Assertions.assertNull(FlussUnionKeyTypes.keyColumnRejection(DataTypes.INT().copy(false)));
        Assertions.assertNull(FlussUnionKeyTypes.partitionColumnRejection(DataTypes.STRING().copy(false)));
    }
}
