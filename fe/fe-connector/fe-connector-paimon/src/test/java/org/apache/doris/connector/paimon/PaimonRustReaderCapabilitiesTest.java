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

package org.apache.doris.connector.paimon;

import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Exercises aggregate compatibility separately from the stricter primary-key merge memory gate. */
public class PaimonRustReaderCapabilitiesTest {
    @Test
    public void integerSumAndProductMustPreserveJavaOverflowSemantics() {
        for (DataType type : Arrays.asList(DataTypes.TINYINT(), DataTypes.SMALLINT(),
                DataTypes.INT(), DataTypes.BIGINT())) {
            assertAggregate("sum", type, false);
            assertAggregate("product", type, false);
        }
        assertAggregate("sum", DataTypes.DOUBLE(), true);
        assertAggregate("product", DataTypes.FLOAT(), true);
    }

    @Test
    public void compactDecimalIntermediateOverflowUsesJni() {
        assertAggregate("sum", DataTypes.DECIMAL(2, 0), false);
        assertAggregate("sum", DataTypes.DECIMAL(18, 2), false);
        assertAggregate("sum", DataTypes.DECIMAL(19, 2), true);
        assertAggregate("product", DataTypes.DECIMAL(20, 2), false);
    }

    @Test
    public void aggregateFunctionMustAcceptTheValueType() {
        assertAggregate("collect", DataTypes.ARRAY(DataTypes.INT()), false);
        assertAggregate("max", DataTypes.ARRAY(DataTypes.INT()), false);
        assertAggregate("max", DataTypes.INT(), true);
        assertAggregate("bool_and", DataTypes.BOOLEAN(), true);
        assertAggregate("bool_or", DataTypes.INT(), false);
        assertAggregate("last_non_null_value", DataTypes.ARRAY(DataTypes.INT()), true);
    }

    @Test
    public void boundedListaggCannotUseRustStringAccumulation() {
        assertAggregate("listagg", DataTypes.STRING(), true);
        assertAggregate("listagg", DataTypes.VARCHAR(20), false);
        assertAggregate("listagg", DataTypes.CHAR(20), false);
    }

    @Test
    public void fieldOverrideWinsBeforeDefaultTypeValidation() {
        for (String engine : Arrays.asList("aggregation", "partial-update")) {
            Map<String, String> options = options(engine);
            options.put("fields.default-aggregate-function", "sum");
            Assertions.assertFalse(compatible(DataTypes.INT(), options));
            options.put("fields.v.aggregate-function", "max");
            Assertions.assertTrue(compatible(DataTypes.INT(), options));
        }
    }

    @Test
    public void unsupportedUnusedNamesStillPreventRustOpeningTheTable() {
        for (String engine : Arrays.asList("aggregation", "partial-update")) {
            for (String key : Arrays.asList("fields.default-aggregate-function", "fields.id.aggregate-function")) {
                Map<String, String> options = options(engine);
                options.put("fields.v.aggregate-function", "max");
                options.put(key, "collect");
                Assertions.assertFalse(compatible(DataTypes.INT(), options));
                options.put(key, "sum");
                Assertions.assertTrue(compatible(DataTypes.INT(), options));
            }
        }
    }

    @Test
    public void partialUpdateSequenceFieldsDoNotInheritTheValueAggregate() {
        Map<String, String> options = options("partial-update");
        options.put("fields.default-aggregate-function", "sum");
        options.put("fields.v.aggregate-function", "max");
        options.put("fields.seq,seq2.sequence-group", "v");
        List<DataField> fields = Arrays.asList(new DataField(0, "id", DataTypes.INT().notNull()),
                new DataField(1, "v", DataTypes.INT()), new DataField(2, "seq", DataTypes.INT()),
                new DataField(3, "seq2", DataTypes.INT()));
        Assertions.assertTrue(PaimonRustReaderCapabilities.hasCompatibleAggregates(schema(fields, options)));
        options.put("fields.seq.aggregate-function", "collect");
        Assertions.assertFalse(PaimonRustReaderCapabilities.hasCompatibleAggregates(schema(fields, options)));
    }

    @Test
    public void deduplicateDoesNotInterpretAggregationOnlyOptions() {
        Map<String, String> options = options("deduplicate");
        options.put("fields.v.aggregate-function", "collect");
        Assertions.assertTrue(compatible(DataTypes.INT(), options));
    }

    private static void assertAggregate(String function, DataType type, boolean expected) {
        for (String engine : Arrays.asList("aggregation", "partial-update")) {
            Map<String, String> options = options(engine);
            options.put("fields.v.aggregate-function", function);
            Assertions.assertEquals(expected, compatible(type, options), engine + " " + function + " " + type);
        }
    }

    private static Map<String, String> options(String engine) {
        Map<String, String> options = new HashMap<>();
        options.put("merge-engine", engine);
        return options;
    }

    private static boolean compatible(DataType valueType, Map<String, String> options) {
        return PaimonRustReaderCapabilities.hasCompatibleAggregates(schema(Arrays.asList(
                new DataField(0, "id", DataTypes.INT().notNull()), new DataField(1, "v", valueType)), options));
    }

    private static TableSchema schema(List<DataField> fields, Map<String, String> options) {
        return new TableSchema(1, fields, 3, Collections.emptyList(), Collections.singletonList("id"), options, null);
    }
}
