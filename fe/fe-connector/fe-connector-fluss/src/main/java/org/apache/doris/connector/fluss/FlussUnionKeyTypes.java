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
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.TimestampType;

/**
 * Which column types a primary-key table can be read by, when its lake and its log tail are read as one.
 *
 * <p>Such a read identifies rows across two halves that were written by different systems and are read
 * by different code: the lake half arrives through paimon's readers, the log tail through fluss's. A
 * row of the lake is dropped when its KEY appears in the tail, so the two sides must agree on what
 * "the same key" means, exactly, for every key column. Where they might not, the table is not read that
 * way at all — the fluss-only read still returns every row of a primary-key table, so refusing to
 * combine the halves costs speed, never correctness.
 *
 * <p>The same question one level up decides which partition a lake split belongs to: the split's
 * partition VALUES are compared, as text, with the ones fluss reports. That comparison is only sound
 * for a type the two sides render identically, which is why partition columns are held to a stricter
 * rule than key columns.
 *
 * <p>Neither rule is a fluss restriction — fluss accepts a DOUBLE primary key and an INT partition key
 * quite happily ({@code TableDescriptorValidation}, whose only key-type ban is ARRAY/MAP/ROW). They are
 * this connector's, and they exist because a wrong answer here is silent: an over-matched key drops a
 * row that should have been returned, an under-matched one returns a superseded row twice.
 */
final class FlussUnionKeyTypes {

    /** Doris DATETIMEV2 stops at microseconds, so a finer fluss timestamp is rounded on the way in. */
    private static final int MAX_TIMESTAMP_PRECISION = 6;

    private FlussUnionKeyTypes() {
    }

    /**
     * Why {@code type} cannot be a key column of a union read, or null when it can.
     *
     * <p>The switch is exhaustive over fluss's type roots on purpose: a type a future fluss release adds
     * lands in the default branch and is refused, rather than being waved through by a rule written
     * before it existed. {@link FlussUnionKeyTypesTest} fails the build when that happens, so the
     * refusal is a stopgap, not the answer.
     */
    static String keyColumnRejection(DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
            case CHAR:
            case STRING:
            case BINARY:
            case BYTES:
            case DATE:
                return null;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return timestampRejection(((TimestampType) type).getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return timestampRejection(((LocalZonedTimestampType) type).getPrecision());
            case FLOAT:
            case DOUBLE:
                // Two encodings of the same number (0.0 and -0.0) compare equal while NaN compares equal
                // to nothing, so "the key is in the tail" has no stable answer. Doris's own key-based
                // table types refuse floating-point keys for the same reason.
                return "its equality is not exact for floating-point values";
            case TIME_WITHOUT_TIME_ZONE:
                // Doris has no type for it at all: the column already reads as UNSUPPORTED.
                return "Doris cannot represent a fluss TIME column";
            default:
                return "the fluss connector does not know how to compare values of this type";
        }
    }

    private static String timestampRejection(int precision) {
        return precision <= MAX_TIMESTAMP_PRECISION ? null
                : "Doris rounds it to microseconds, which can make two different keys look like one";
    }

    /**
     * Why {@code type} cannot be a partition column of a union read, or null when it can.
     *
     * <p>Only STRING. A partition value reaches this connector already rendered — fluss hands back the
     * text it stored, paimon renders its own typed partition value — and nothing guarantees the two
     * spellings of a number, a date or a padded CHAR agree. They cannot be normalized here either,
     * without this connector taking on how paimon renders every type it has.
     */
    static String partitionColumnRejection(DataType type) {
        return type.getTypeRoot() == DataTypeRoot.STRING ? null
                : "a lake split is matched to a fluss partition by its rendered partition value, which"
                        + " only STRING is guaranteed to render the same way on both sides";
    }
}
