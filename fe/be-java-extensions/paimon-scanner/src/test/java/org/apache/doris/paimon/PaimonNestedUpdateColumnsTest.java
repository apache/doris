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

package org.apache.doris.paimon;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class PaimonNestedUpdateColumnsTest {

    @Test
    public void resolvesNestedUpdateColumnAndItsNestedKey() {
        Map<String, String> options = new HashMap<>();
        options.put("merge-engine", "partial-update");
        options.put("fields.oligo.aggregate-function", "nested_update");
        options.put("fields.oligo.nested-key", "ext_id");
        // A plain aggregation column must not be pinned.
        options.put("fields.total.aggregate-function", "sum");

        PaimonNestedUpdateColumns columns = PaimonNestedUpdateColumns.resolve(options);

        Assertions.assertFalse(columns.isEmpty());
        Assertions.assertEquals(java.util.Arrays.asList("ext_id"),
                columns.requiredElementFields("oligo"));
        Assertions.assertTrue(columns.requiredElementFields("total").isEmpty());
        // Column lookup is case-insensitive: Doris lowercases top-level names while the option key
        // preserves the declared case.
        Assertions.assertEquals(java.util.Arrays.asList("ext_id"),
                columns.requiredElementFields("OLIGO"));
    }

    @Test
    public void nestedUpdateWithoutNestedKeyNeedsNoPinning() {
        // A nested_update column with no nested-key (whole-element dedup) has no field whose pruning
        // would corrupt the merge engine, so nothing must be pinned.
        Map<String, String> options = new HashMap<>();
        options.put("fields.payload.aggregate-function", "nested_update");

        PaimonNestedUpdateColumns columns = PaimonNestedUpdateColumns.resolve(options);

        Assertions.assertTrue(columns.isEmpty());
        Assertions.assertTrue(columns.requiredElementFields("payload").isEmpty());
    }

    @Test
    public void tableWithoutFieldsOptionsResolvesEmpty() {
        Assertions.assertTrue(PaimonNestedUpdateColumns.resolve(new HashMap<>()).isEmpty());
        Assertions.assertTrue(PaimonNestedUpdateColumns.resolve(null).isEmpty());
    }

    @Test
    public void commaSeparatedNestedKeyIsFullyResolved() {
        // Paimon allows a multi-field nested-key; every named field sits at a merge-engine-read position.
        Map<String, String> options = new HashMap<>();
        options.put("fields.orders.aggregate-function", "nested_update");
        options.put("fields.orders.nested-key", "oi_id,status");

        PaimonNestedUpdateColumns columns = PaimonNestedUpdateColumns.resolve(options);

        Assertions.assertEquals(java.util.Arrays.asList("oi_id", "status"),
                columns.requiredElementFields("orders"));
    }
}
