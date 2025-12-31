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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link InsertPartitionSpec}.
 */
public class InsertPartitionSpecTest {

    @Test
    public void testNone() {
        InsertPartitionSpec spec = InsertPartitionSpec.none();

        Assertions.assertFalse(spec.isStaticPartition());
        Assertions.assertFalse(spec.hasDynamicPartitionNames());
        Assertions.assertFalse(spec.isTemporary());
        Assertions.assertFalse(spec.isAutoDetect());
        Assertions.assertTrue(spec.getStaticPartitionValues().isEmpty());
        Assertions.assertTrue(spec.getPartitionNames().isEmpty());
    }

    @Test
    public void testAutoDetect() {
        // Non-temporary auto-detect
        InsertPartitionSpec spec = InsertPartitionSpec.autoDetect(false);

        Assertions.assertFalse(spec.isStaticPartition());
        Assertions.assertFalse(spec.hasDynamicPartitionNames());
        Assertions.assertFalse(spec.isTemporary());
        Assertions.assertTrue(spec.isAutoDetect());
        Assertions.assertTrue(spec.getStaticPartitionValues().isEmpty());
        Assertions.assertTrue(spec.getPartitionNames().isEmpty());
    }

    @Test
    public void testAutoDetectTemporary() {
        // Temporary auto-detect
        InsertPartitionSpec spec = InsertPartitionSpec.autoDetect(true);

        Assertions.assertFalse(spec.isStaticPartition());
        Assertions.assertFalse(spec.hasDynamicPartitionNames());
        Assertions.assertTrue(spec.isTemporary());
        Assertions.assertTrue(spec.isAutoDetect());
    }

    @Test
    public void testDynamicPartition() {
        List<String> partitionNames = ImmutableList.of("p1", "p2", "p3");
        InsertPartitionSpec spec = InsertPartitionSpec.dynamicPartition(partitionNames, false);

        Assertions.assertFalse(spec.isStaticPartition());
        Assertions.assertTrue(spec.hasDynamicPartitionNames());
        Assertions.assertFalse(spec.isTemporary());
        Assertions.assertFalse(spec.isAutoDetect());
        Assertions.assertEquals(3, spec.getPartitionNames().size());
        Assertions.assertEquals("p1", spec.getPartitionNames().get(0));
        Assertions.assertEquals("p2", spec.getPartitionNames().get(1));
        Assertions.assertEquals("p3", spec.getPartitionNames().get(2));
    }

    @Test
    public void testDynamicPartitionTemporary() {
        List<String> partitionNames = ImmutableList.of("p1");
        InsertPartitionSpec spec = InsertPartitionSpec.dynamicPartition(partitionNames, true);

        Assertions.assertFalse(spec.isStaticPartition());
        Assertions.assertTrue(spec.hasDynamicPartitionNames());
        Assertions.assertTrue(spec.isTemporary());
        Assertions.assertFalse(spec.isAutoDetect());
        Assertions.assertEquals(1, spec.getPartitionNames().size());
    }

    @Test
    public void testDynamicPartitionEmpty() {
        List<String> partitionNames = ImmutableList.of();
        InsertPartitionSpec spec = InsertPartitionSpec.dynamicPartition(partitionNames, false);

        Assertions.assertFalse(spec.isStaticPartition());
        Assertions.assertFalse(spec.hasDynamicPartitionNames());
        Assertions.assertFalse(spec.isTemporary());
        Assertions.assertFalse(spec.isAutoDetect());
        Assertions.assertTrue(spec.getPartitionNames().isEmpty());
    }

    @Test
    public void testDynamicPartitionNull() {
        InsertPartitionSpec spec = InsertPartitionSpec.dynamicPartition(null, false);

        Assertions.assertFalse(spec.isStaticPartition());
        Assertions.assertFalse(spec.hasDynamicPartitionNames());
        Assertions.assertTrue(spec.getPartitionNames().isEmpty());
    }

    @Test
    public void testStaticPartition() {
        Map<String, Expression> staticValues = ImmutableMap.of(
                "dt", new StringLiteral("2025-01-25"),
                "region", new StringLiteral("bj"));
        InsertPartitionSpec spec = InsertPartitionSpec.staticPartition(staticValues, false);

        Assertions.assertTrue(spec.isStaticPartition());
        Assertions.assertFalse(spec.hasDynamicPartitionNames());
        Assertions.assertFalse(spec.isTemporary());
        Assertions.assertFalse(spec.isAutoDetect());
        Assertions.assertEquals(2, spec.getStaticPartitionValues().size());
        Assertions.assertTrue(spec.getStaticPartitionValues().containsKey("dt"));
        Assertions.assertTrue(spec.getStaticPartitionValues().containsKey("region"));
    }

    @Test
    public void testStaticPartitionTemporary() {
        Map<String, Expression> staticValues = ImmutableMap.of(
                "year", new IntegerLiteral(2025));
        InsertPartitionSpec spec = InsertPartitionSpec.staticPartition(staticValues, true);

        Assertions.assertTrue(spec.isStaticPartition());
        Assertions.assertFalse(spec.hasDynamicPartitionNames());
        Assertions.assertTrue(spec.isTemporary());
        Assertions.assertFalse(spec.isAutoDetect());
        Assertions.assertEquals(1, spec.getStaticPartitionValues().size());
    }

    @Test
    public void testStaticPartitionEmpty() {
        Map<String, Expression> staticValues = ImmutableMap.of();
        InsertPartitionSpec spec = InsertPartitionSpec.staticPartition(staticValues, false);

        Assertions.assertFalse(spec.isStaticPartition());
        Assertions.assertFalse(spec.hasDynamicPartitionNames());
        Assertions.assertTrue(spec.getStaticPartitionValues().isEmpty());
    }

    @Test
    public void testStaticPartitionNull() {
        InsertPartitionSpec spec = InsertPartitionSpec.staticPartition(null, false);

        Assertions.assertFalse(spec.isStaticPartition());
        Assertions.assertTrue(spec.getStaticPartitionValues().isEmpty());
    }

    @Test
    public void testImmutability() {
        // Test that returned collections are immutable
        Map<String, Expression> staticValues = ImmutableMap.of(
                "dt", new StringLiteral("2025-01-25"));
        List<String> partitionNames = ImmutableList.of("p1");

        InsertPartitionSpec staticSpec = InsertPartitionSpec.staticPartition(staticValues, false);
        InsertPartitionSpec dynamicSpec = InsertPartitionSpec.dynamicPartition(partitionNames, false);

        // Verify collections are immutable (should throw UnsupportedOperationException)
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            staticSpec.getStaticPartitionValues().put("new_key", new StringLiteral("value"));
        });

        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            dynamicSpec.getPartitionNames().add("p2");
        });
    }

    @Test
    public void testMutualExclusivity() {
        // Static partition should not have dynamic partition names
        Map<String, Expression> staticValues = ImmutableMap.of(
                "dt", new StringLiteral("2025-01-25"));
        InsertPartitionSpec staticSpec = InsertPartitionSpec.staticPartition(staticValues, false);
        Assertions.assertTrue(staticSpec.isStaticPartition());
        Assertions.assertFalse(staticSpec.hasDynamicPartitionNames());
        Assertions.assertFalse(staticSpec.isAutoDetect());

        // Dynamic partition should not be static
        List<String> partitionNames = ImmutableList.of("p1", "p2");
        InsertPartitionSpec dynamicSpec = InsertPartitionSpec.dynamicPartition(partitionNames, false);
        Assertions.assertFalse(dynamicSpec.isStaticPartition());
        Assertions.assertTrue(dynamicSpec.hasDynamicPartitionNames());
        Assertions.assertFalse(dynamicSpec.isAutoDetect());

        // Auto-detect should not be static or have dynamic names
        InsertPartitionSpec autoSpec = InsertPartitionSpec.autoDetect(false);
        Assertions.assertFalse(autoSpec.isStaticPartition());
        Assertions.assertFalse(autoSpec.hasDynamicPartitionNames());
        Assertions.assertTrue(autoSpec.isAutoDetect());

        // None should have nothing
        InsertPartitionSpec noneSpec = InsertPartitionSpec.none();
        Assertions.assertFalse(noneSpec.isStaticPartition());
        Assertions.assertFalse(noneSpec.hasDynamicPartitionNames());
        Assertions.assertFalse(noneSpec.isAutoDetect());
    }
}
