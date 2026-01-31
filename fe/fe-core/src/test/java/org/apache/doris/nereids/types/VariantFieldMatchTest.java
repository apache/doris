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

package org.apache.doris.nereids.types;

import org.apache.doris.thrift.TPatternType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/**
 * Unit tests for VariantField pattern matching and VariantType field lookup.
 */
public class VariantFieldMatchTest {

    // ==================== VariantField.matches() tests ====================

    @Test
    public void testExactMatch() {
        VariantField field = new VariantField("number_latency", BigIntType.INSTANCE, "",
                TPatternType.MATCH_NAME.name());

        Assertions.assertTrue(field.matches("number_latency"));
        Assertions.assertFalse(field.matches("number_latency_ms"));
        Assertions.assertFalse(field.matches("other_field"));
    }

    @Test
    public void testGlobMatchSuffix() {
        // Pattern: number_* should match number_latency, number_count, etc.
        VariantField field = new VariantField("number_*", BigIntType.INSTANCE, "",
                TPatternType.MATCH_NAME_GLOB.name());

        Assertions.assertTrue(field.matches("number_latency"));
        Assertions.assertTrue(field.matches("number_count"));
        Assertions.assertTrue(field.matches("number_"));
        Assertions.assertFalse(field.matches("string_message"));
        Assertions.assertFalse(field.matches("numbering"));
    }

    @Test
    public void testGlobMatchPrefix() {
        // Pattern: *_latency should match number_latency, string_latency, etc.
        VariantField field = new VariantField("*_latency", BigIntType.INSTANCE, "",
                TPatternType.MATCH_NAME_GLOB.name());

        Assertions.assertTrue(field.matches("number_latency"));
        Assertions.assertTrue(field.matches("string_latency"));
        Assertions.assertTrue(field.matches("_latency"));
        Assertions.assertFalse(field.matches("latency_ms"));
    }

    @Test
    public void testGlobMatchMiddle() {
        // Pattern: num_*_ms should match num_latency_ms, num_count_ms, etc.
        VariantField field = new VariantField("num_*_ms", BigIntType.INSTANCE, "",
                TPatternType.MATCH_NAME_GLOB.name());

        Assertions.assertTrue(field.matches("num_latency_ms"));
        Assertions.assertTrue(field.matches("num_count_ms"));
        Assertions.assertTrue(field.matches("num__ms"));
        Assertions.assertFalse(field.matches("num_latency"));
        Assertions.assertFalse(field.matches("number_latency_ms"));
    }

    @Test
    public void testGlobMatchAll() {
        // Pattern: * should match everything
        VariantField field = new VariantField("*", StringType.INSTANCE, "",
                TPatternType.MATCH_NAME_GLOB.name());

        Assertions.assertTrue(field.matches("anything"));
        Assertions.assertTrue(field.matches(""));
        Assertions.assertTrue(field.matches("a.b.c"));
    }

    @Test
    public void testGlobMatchWithDot() {
        // Pattern: metrics.* should match metrics.score, metrics.count, etc.
        VariantField field = new VariantField("metrics.*", BigIntType.INSTANCE, "",
                TPatternType.MATCH_NAME_GLOB.name());

        Assertions.assertTrue(field.matches("metrics.score"));
        Assertions.assertTrue(field.matches("metrics.count"));
        Assertions.assertFalse(field.matches("metricsXscore"));
        Assertions.assertFalse(field.matches("metrics"));
    }

    @Test
    public void testDefaultPatternTypeIsGlob() {
        // Default constructor should use MATCH_NAME_GLOB
        VariantField field = new VariantField("number_*", BigIntType.INSTANCE, "");

        Assertions.assertTrue(field.matches("number_latency"));
        Assertions.assertEquals(TPatternType.MATCH_NAME_GLOB, field.getPatternType());
    }

    // ==================== VariantType.findMatchingField() tests ====================

    @Test
    public void testFindMatchingFieldSinglePattern() {
        VariantField field = new VariantField("number_*", BigIntType.INSTANCE, "");
        VariantType variantType = new VariantType(ImmutableList.of(field));

        Optional<VariantField> result = variantType.findMatchingField("number_latency");
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(BigIntType.INSTANCE, result.get().getDataType());
    }

    @Test
    public void testFindMatchingFieldMultiplePatterns() {
        VariantField numberField = new VariantField("number_*", BigIntType.INSTANCE, "");
        VariantField stringField = new VariantField("string_*", StringType.INSTANCE, "");
        VariantType variantType = new VariantType(ImmutableList.of(numberField, stringField));

        // Test number pattern
        Optional<VariantField> numberResult = variantType.findMatchingField("number_latency");
        Assertions.assertTrue(numberResult.isPresent());
        Assertions.assertEquals(BigIntType.INSTANCE, numberResult.get().getDataType());

        // Test string pattern
        Optional<VariantField> stringResult = variantType.findMatchingField("string_message");
        Assertions.assertTrue(stringResult.isPresent());
        Assertions.assertEquals(StringType.INSTANCE, stringResult.get().getDataType());
    }

    @Test
    public void testFindMatchingFieldNoMatch() {
        VariantField field = new VariantField("number_*", BigIntType.INSTANCE, "");
        VariantType variantType = new VariantType(ImmutableList.of(field));

        Optional<VariantField> result = variantType.findMatchingField("string_message");
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testFindMatchingFieldFirstMatchWins() {
        // When multiple patterns match, the first one should win
        VariantField field1 = new VariantField("num*", BigIntType.INSTANCE, "");
        VariantField field2 = new VariantField("number_*", DoubleType.INSTANCE, "");
        VariantType variantType = new VariantType(ImmutableList.of(field1, field2));

        Optional<VariantField> result = variantType.findMatchingField("number_latency");
        Assertions.assertTrue(result.isPresent());
        // First pattern "num*" should match, returning BigIntType
        Assertions.assertEquals(BigIntType.INSTANCE, result.get().getDataType());
    }

    @Test
    public void testFindMatchingFieldEmptyPredefinedFields() {
        VariantType variantType = new VariantType(0);

        Optional<VariantField> result = variantType.findMatchingField("any_field");
        Assertions.assertFalse(result.isPresent());
    }
}
