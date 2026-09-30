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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.utils.OffsetRow;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class PaimonVariantProjectionTest {

    @Test
    public void missingProjectedPathFromSpilledOffsetRowMaterializesAnEmptyObject() {
        PaimonVariantProjection projection = PaimonVariantProjection.create(
                Collections.singletonList(Collections.singletonList("missing")), "UTC");
        BinaryRow spilledRow = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(spilledRow);
        writer.writeInt(0, 42);
        writer.setNullAt(1);
        writer.complete();
        OffsetRow extracted = new OffsetRow(1, 1).replace(spilledRow);

        Variant actual = projection.materialize(GenericRow.of(extracted), 0);
        Variant expected = GenericVariant.fromJson("{}");

        Assertions.assertArrayEquals(expected.value(), actual.value());
        Assertions.assertArrayEquals(expected.metadata(), actual.metadata());
    }
}
