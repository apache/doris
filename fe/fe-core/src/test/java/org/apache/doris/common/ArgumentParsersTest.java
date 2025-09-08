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

package org.apache.doris.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ArgumentParsersTest {

    // String parsers tests

    @Test
    void testNonEmptyString() {
        ArgumentParser<String> parser = ArgumentParsers.nonEmptyString("testParam");

        // Test valid strings
        Assertions.assertEquals("hello", parser.parse("hello"));
        Assertions.assertEquals("world", parser.parse("  world  ")); // Should trim

        // Test invalid strings
        IllegalArgumentException exception1 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse(null));
        Assertions.assertTrue(exception1.getMessage().contains("testParam cannot be empty"));

        IllegalArgumentException exception2 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse(""));
        Assertions.assertTrue(exception2.getMessage().contains("testParam cannot be empty"));

        IllegalArgumentException exception3 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("   "));
        Assertions.assertTrue(exception3.getMessage().contains("testParam cannot be empty"));
    }

    @Test
    void testStringLength() {
        ArgumentParser<String> parser = ArgumentParsers.stringLength("name", 3, 10);

        // Test valid lengths
        Assertions.assertEquals("abc", parser.parse("abc")); // Min length
        Assertions.assertEquals("abcdefghij", parser.parse("abcdefghij")); // Max length
        Assertions.assertEquals("hello", parser.parse("  hello  ")); // Should trim

        // Test invalid lengths
        IllegalArgumentException exception1 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("ab")); // Too short
        Assertions.assertTrue(exception1.getMessage().contains("length must be between 3 and 10"));

        IllegalArgumentException exception2 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("abcdefghijk")); // Too long
        Assertions.assertTrue(exception2.getMessage().contains("length must be between 3 and 10"));

        // Test null
        IllegalArgumentException exception3 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse(null));
        Assertions.assertTrue(exception3.getMessage().contains("name cannot be null"));
    }

    @Test
    void testStringChoice() {
        ArgumentParser<String> parser = ArgumentParsers.stringChoice("mode", "READ", "WRITE", "APPEND");

        // Test valid choices
        Assertions.assertEquals("READ", parser.parse("READ"));
        Assertions.assertEquals("WRITE", parser.parse("WRITE"));
        Assertions.assertEquals("APPEND", parser.parse("  APPEND  ")); // Should trim

        // Test invalid choice
        IllegalArgumentException exception1 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("DELETE"));
        Assertions.assertTrue(exception1.getMessage().contains("mode must be one of READ, WRITE, APPEND"));

        // Test case sensitivity
        IllegalArgumentException exception2 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("read")); // Different case
        Assertions.assertTrue(exception2.getMessage().contains("mode must be one of READ, WRITE, APPEND"));

        // Test null
        IllegalArgumentException exception3 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse(null));
        Assertions.assertTrue(exception3.getMessage().contains("mode cannot be null"));
    }

    @Test
    void testStringChoiceIgnoreCase() {
        ArgumentParser<String> parser = ArgumentParsers.stringChoiceIgnoreCase("level", "DEBUG", "INFO", "WARN",
                "ERROR");

        // Test valid choices with different cases
        Assertions.assertEquals("DEBUG", parser.parse("debug"));
        Assertions.assertEquals("INFO", parser.parse("Info"));
        Assertions.assertEquals("WARN", parser.parse("WARN"));
        Assertions.assertEquals("ERROR", parser.parse("  error  ")); // Should trim

        // Test invalid choice
        IllegalArgumentException exception1 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("TRACE"));
        Assertions.assertTrue(
                exception1.getMessage().contains("level must be one of DEBUG, INFO, WARN, ERROR (case-insensitive)"));

        // Test null
        IllegalArgumentException exception2 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse(null));
        Assertions.assertTrue(exception2.getMessage().contains("level cannot be null"));
    }

    // Integer parsers tests

    @Test
    void testPositiveInt() {
        ArgumentParser<Integer> parser = ArgumentParsers.positiveInt("count");

        // Test valid positive integers
        Assertions.assertEquals(Integer.valueOf(1), parser.parse("1"));
        Assertions.assertEquals(Integer.valueOf(100), parser.parse("100"));
        Assertions.assertEquals(Integer.valueOf(Integer.MAX_VALUE), parser.parse(String.valueOf(Integer.MAX_VALUE)));
        Assertions.assertEquals(Integer.valueOf(42), parser.parse("  42  ")); // Should trim

        // Test invalid values
        IllegalArgumentException exception1 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("0"));
        Assertions.assertTrue(exception1.getMessage().contains("count must be positive"));

        IllegalArgumentException exception2 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("-5"));
        Assertions.assertTrue(exception2.getMessage().contains("count must be positive"));

        IllegalArgumentException exception3 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("not-a-number"));
        Assertions.assertTrue(exception3.getMessage().contains("Invalid count format"));
    }

    @Test
    void testIntRange() {
        ArgumentParser<Integer> parser = ArgumentParsers.intRange("port", 1, 65535);

        // Test valid range
        Assertions.assertEquals(Integer.valueOf(1), parser.parse("1"));
        Assertions.assertEquals(Integer.valueOf(8080), parser.parse("8080"));
        Assertions.assertEquals(Integer.valueOf(65535), parser.parse("65535"));
        Assertions.assertEquals(Integer.valueOf(443), parser.parse("  443  ")); // Should trim

        // Test out of range
        IllegalArgumentException exception1 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("0"));
        Assertions.assertTrue(exception1.getMessage().contains("port must be between 1 and 65535"));

        IllegalArgumentException exception2 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("65536"));
        Assertions.assertTrue(exception2.getMessage().contains("port must be between 1 and 65535"));

        // Test invalid format
        IllegalArgumentException exception3 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("abc"));
        Assertions.assertTrue(exception3.getMessage().contains("Invalid port format"));
    }

    // Long parsers tests

    @Test
    void testPositiveLong() {
        ArgumentParser<Long> parser = ArgumentParsers.positiveLong("size");

        // Test valid positive longs
        Assertions.assertEquals(Long.valueOf(1L), parser.parse("1"));
        Assertions.assertEquals(Long.valueOf(1000000L), parser.parse("1000000"));
        Assertions.assertEquals(Long.valueOf(Long.MAX_VALUE), parser.parse(String.valueOf(Long.MAX_VALUE)));
        Assertions.assertEquals(Long.valueOf(42L), parser.parse("  42  ")); // Should trim

        // Test invalid values
        IllegalArgumentException exception1 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("0"));
        Assertions.assertTrue(exception1.getMessage().contains("size must be positive"));

        IllegalArgumentException exception2 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("-1"));
        Assertions.assertTrue(exception2.getMessage().contains("size must be positive"));

        IllegalArgumentException exception3 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("invalid"));
        Assertions.assertTrue(exception3.getMessage().contains("Invalid size format"));
    }

    @Test
    void testNonNegativeLong() {
        ArgumentParser<Long> parser = ArgumentParsers.nonNegativeLong("offset");

        // Test valid non-negative longs
        Assertions.assertEquals(Long.valueOf(0L), parser.parse("0"));
        Assertions.assertEquals(Long.valueOf(100L), parser.parse("100"));
        Assertions.assertEquals(Long.valueOf(Long.MAX_VALUE), parser.parse(String.valueOf(Long.MAX_VALUE)));
        Assertions.assertEquals(Long.valueOf(42L), parser.parse("  42  ")); // Should trim

        // Test invalid values
        IllegalArgumentException exception1 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("-1"));
        Assertions.assertTrue(exception1.getMessage().contains("offset must be non-negative"));

        IllegalArgumentException exception2 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("not-a-number"));
        Assertions.assertTrue(exception2.getMessage().contains("Invalid offset format"));
    }

    @Test
    void testLongRange() {
        ArgumentParser<Long> parser = ArgumentParsers.longRange("timestamp", 0L, 9999999999L);

        // Test valid range
        Assertions.assertEquals(Long.valueOf(0L), parser.parse("0"));
        Assertions.assertEquals(Long.valueOf(1234567890L), parser.parse("1234567890"));
        Assertions.assertEquals(Long.valueOf(9999999999L), parser.parse("9999999999"));
        Assertions.assertEquals(Long.valueOf(123L), parser.parse("  123  ")); // Should trim

        // Test out of range
        IllegalArgumentException exception1 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("-1"));
        Assertions.assertTrue(exception1.getMessage().contains("timestamp must be between 0 and 9999999999"));

        IllegalArgumentException exception2 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("10000000000"));
        Assertions.assertTrue(exception2.getMessage().contains("timestamp must be between 0 and 9999999999"));

        // Test invalid format
        IllegalArgumentException exception3 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("invalid"));
        Assertions.assertTrue(exception3.getMessage().contains("Invalid timestamp format"));
    }

    // Double parsers tests

    @Test
    void testPositiveDouble() {
        ArgumentParser<Double> parser = ArgumentParsers.positiveDouble("rate");

        // Test valid positive doubles
        Assertions.assertEquals(Double.valueOf(1.0), parser.parse("1.0"));
        Assertions.assertEquals(Double.valueOf(3.14159), parser.parse("3.14159"));
        Assertions.assertEquals(Double.valueOf(100.5), parser.parse("100.5"));
        Assertions.assertEquals(Double.valueOf(1.0), parser.parse("1")); // Integer format
        Assertions.assertEquals(Double.valueOf(2.5), parser.parse("  2.5  ")); // Should trim

        // Test invalid values
        IllegalArgumentException exception1 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("0.0"));
        Assertions.assertTrue(exception1.getMessage().contains("rate must be positive"));

        IllegalArgumentException exception2 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("-1.5"));
        Assertions.assertTrue(exception2.getMessage().contains("rate must be positive"));

        IllegalArgumentException exception3 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("not-a-number"));
        Assertions.assertTrue(exception3.getMessage().contains("Invalid rate format"));
    }

    @Test
    void testDoubleRange() {
        ArgumentParser<Double> parser = ArgumentParsers.doubleRange("percentage", 0.0, 100.0);

        // Test valid range
        Assertions.assertEquals(Double.valueOf(0.0), parser.parse("0.0"));
        Assertions.assertEquals(Double.valueOf(50.5), parser.parse("50.5"));
        Assertions.assertEquals(Double.valueOf(100.0), parser.parse("100.0"));
        Assertions.assertEquals(Double.valueOf(25.0), parser.parse("25")); // Integer format
        Assertions.assertEquals(Double.valueOf(75.5), parser.parse("  75.5  ")); // Should trim

        // Test out of range
        IllegalArgumentException exception1 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("-0.1"));
        Assertions.assertTrue(exception1.getMessage().contains("percentage must be between 0.0 and 100.0"));

        IllegalArgumentException exception2 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("100.1"));
        Assertions.assertTrue(exception2.getMessage().contains("percentage must be between 0.0 and 100.0"));

        // Test invalid format
        IllegalArgumentException exception3 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("invalid"));
        Assertions.assertTrue(exception3.getMessage().contains("Invalid percentage format"));
    }

    // Boolean parser tests

    @Test
    void testBooleanValue() {
        ArgumentParser<Boolean> parser = ArgumentParsers.booleanValue("enabled");

        // Test valid boolean values
        Assertions.assertEquals(Boolean.TRUE, parser.parse("true"));
        Assertions.assertEquals(Boolean.TRUE, parser.parse("TRUE"));
        Assertions.assertEquals(Boolean.TRUE, parser.parse("True"));
        Assertions.assertEquals(Boolean.TRUE, parser.parse("  true  ")); // Should trim

        Assertions.assertEquals(Boolean.FALSE, parser.parse("false"));
        Assertions.assertEquals(Boolean.FALSE, parser.parse("FALSE"));
        Assertions.assertEquals(Boolean.FALSE, parser.parse("False"));
        Assertions.assertEquals(Boolean.FALSE, parser.parse("  false  ")); // Should trim

        // Test invalid values
        IllegalArgumentException exception1 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("yes"));
        Assertions.assertTrue(exception1.getMessage().contains("enabled must be 'true' or 'false'"));

        IllegalArgumentException exception2 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("1"));
        Assertions.assertTrue(exception2.getMessage().contains("enabled must be 'true' or 'false'"));

        IllegalArgumentException exception3 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse(""));
        Assertions.assertTrue(exception3.getMessage().contains("enabled must be 'true' or 'false'"));
    }

    // Edge cases and special scenarios

    @Test
    void testParserWithSpecialCharacters() {
        ArgumentParser<String> parser = ArgumentParsers.stringChoice("symbol", ">=", "<=", "!=", "==");

        Assertions.assertEquals(">=", parser.parse(">="));
        Assertions.assertEquals("<=", parser.parse("<="));
        Assertions.assertEquals("!=", parser.parse("!="));
        Assertions.assertEquals("==", parser.parse("=="));

        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse(">"));
        Assertions.assertTrue(exception.getMessage().contains("symbol must be one of >=, <=, !=, =="));
    }

    @Test
    void testIntegerBoundaryValues() {
        ArgumentParser<Integer> parser = ArgumentParsers.intRange("value", Integer.MIN_VALUE, Integer.MAX_VALUE);

        Assertions.assertEquals(Integer.valueOf(Integer.MIN_VALUE), parser.parse(String.valueOf(Integer.MIN_VALUE)));
        Assertions.assertEquals(Integer.valueOf(Integer.MAX_VALUE), parser.parse(String.valueOf(Integer.MAX_VALUE)));
    }

    @Test
    void testLongBoundaryValues() {
        ArgumentParser<Long> parser = ArgumentParsers.longRange("value", Long.MIN_VALUE, Long.MAX_VALUE);

        Assertions.assertEquals(Long.valueOf(Long.MIN_VALUE), parser.parse(String.valueOf(Long.MIN_VALUE)));
        Assertions.assertEquals(Long.valueOf(Long.MAX_VALUE), parser.parse(String.valueOf(Long.MAX_VALUE)));
    }

    @Test
    void testDoubleBoundaryValues() {
        ArgumentParser<Double> parser = ArgumentParsers.doubleRange("value", -Double.MAX_VALUE, Double.MAX_VALUE);

        Assertions.assertEquals(Double.valueOf(-Double.MAX_VALUE), parser.parse(String.valueOf(-Double.MAX_VALUE)));
        Assertions.assertEquals(Double.valueOf(Double.MAX_VALUE), parser.parse(String.valueOf(Double.MAX_VALUE)));
    }

    @Test
    void testDoubleSpecialValues() {
        ArgumentParser<Double> parser = ArgumentParsers.positiveDouble("value");

        // Test positive infinity
        Assertions.assertEquals(Double.POSITIVE_INFINITY, parser.parse("Infinity"));

        // Test NaN (should be treated as positive since Double.compare(Double.NaN, 0.0)
        // > 0 is false)
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("NaN"));
        Assertions.assertTrue(exception.getMessage().contains("value must be positive"));
    }

    @Test
    void testStringChoiceWithEmptyArray() {
        // Test parser with no allowed values
        ArgumentParser<String> parser = ArgumentParsers.stringChoice("empty");

        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("anything"));
        Assertions.assertTrue(exception.getMessage().contains("empty must be one of"));
    }

    @Test
    void testStringLengthWithZeroMinLength() {
        ArgumentParser<String> parser = ArgumentParsers.stringLength("text", 0, 5);

        Assertions.assertEquals("", parser.parse("")); // Empty string should be valid
        Assertions.assertEquals("hello", parser.parse("hello"));

        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> parser.parse("toolong"));
        Assertions.assertTrue(exception.getMessage().contains("length must be between 0 and 5"));
    }

    @Test
    void testNumericParsersWithLeadingZeros() {
        // Test integer parser with leading zeros
        ArgumentParser<Integer> intParser = ArgumentParsers.positiveInt("number");
        Assertions.assertEquals(Integer.valueOf(42), intParser.parse("0042"));

        // Test long parser with leading zeros
        ArgumentParser<Long> longParser = ArgumentParsers.positiveLong("number");
        Assertions.assertEquals(Long.valueOf(123L), longParser.parse("000123"));

        // Test double parser with leading zeros
        ArgumentParser<Double> doubleParser = ArgumentParsers.positiveDouble("number");
        Assertions.assertEquals(Double.valueOf(3.14), doubleParser.parse("003.14"));
    }
}
