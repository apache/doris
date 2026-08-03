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

/**
 * Which column types a fluss table can be PARTITIONED by and still be readable here.
 *
 * <p>A fluss partition carries its value nowhere but in its own name, and fluss restricts what that name
 * may contain to ASCII letters, digits, {@code _} and {@code -} — so a value holding anything else is
 * rewritten on the way in. A FLOAT {@code 1.5} is named {@code 1_5}; a TIMESTAMP
 * {@code 2026-01-01 01:02:03.0} is named {@code 2026-01-01-01-02-03_0}. Nothing reads those back: the
 * substitution is many-to-one (a {@code _} was a {@code .} or a {@code :}), and Doris is handed the name,
 * not the value.
 *
 * <p>Which is why this is a refusal and not a conversion. Left alone, such a table reaches fe-core's
 * partition parser and dies there with {@code failed to convert partition [1_5] to list partition} — a
 * message that names neither the column nor fluss nor what to do about it, on a table whose {@code DESC}
 * looks perfectly ordinary. The verdict below is asked before the partitions are listed, so the answer
 * depends on the table's schema alone and is the same whether the table has partitions yet or not.
 *
 * <p>Not to be confused with {@link FlussUnionKeyTypes#partitionColumnRejection}, which asks a stricter
 * and later question: whether a LAKE split can be matched to a fluss partition by comparing two engines'
 * renderings of the same value. That one allows STRING only. This one asks whether Doris can read the
 * partition at all, and its answer is a precondition for the other's.
 */
final class FlussPartitionColumnTypes {

    /** The types whose value survives fluss's partition naming, for the error message. */
    static final String READABLE_TYPES = "CHAR, STRING, BOOLEAN, TINYINT, SMALLINT, INT, BIGINT and DATE";

    private FlussPartitionColumnTypes() {
    }

    /**
     * Why {@code type} cannot be a partition column of a readable table, or null when it can.
     *
     * <p>The switch is exhaustive over fluss's type roots on purpose: a type a future fluss release adds
     * to its own partition-key whitelist lands in the default branch and is refused with a message, rather
     * than being waved through into fe-core's parser by a rule written before it existed.
     * {@link FlussPartitionColumnTypesTest} fails the build when that happens.
     *
     * <p>BINARY and BYTES are the one verdict that depends on the catalog: fluss names their partitions
     * with the hex text of the bytes, which is a perfectly good STRING and not a VARBINARY at all.
     */
    static String rejection(DataType type, FlussTypeMapping.Options options) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case STRING:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DATE:
                return null;
            case BINARY:
            case BYTES:
                return options.isMapBinaryToVarbinary()
                        ? "fluss names such a partition with the hex text of the bytes, which this catalog"
                                + " cannot read back as the VARBINARY column that '"
                                + FlussConnectorProperties.ENABLE_MAPPING_VARBINARY + "=true' asks for;"
                                + " turning that property off reads the column, and the partition, as text"
                        : null;
            case FLOAT:
            case DOUBLE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return "fluss keeps a partition's value only in its name, where it rewrites every character"
                        + " a name may not contain — 1.5 is named 1_5 and 2026-01-01 01:02:03 is named"
                        + " 2026-01-01-01-02-03 — so the value that was written cannot be read back";
            default:
                // ARRAY, MAP and ROW among them: fluss refuses these as partition keys itself, and this
                // branch is what keeps that from being the only thing standing in the way.
                return "the fluss connector does not know how a partition of this type is named";
        }
    }
}
