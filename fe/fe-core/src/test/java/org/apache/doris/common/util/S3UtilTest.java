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

package org.apache.doris.common.util;

import org.junit.Assert;
import org.junit.Test;

public class S3UtilTest {

    @Test
    public void testExtendGlobNumberRange_simpleRange() {
        // Test simple range expansion {1..3}
        String input = "file_{1..3}.csv";
        String expected = "file_{1,2,3}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_reverseRange() {
        // Test reverse range {3..1}, should normalize to {1,2,3}
        String input = "file_{3..1}.csv";
        String expected = "file_{1,2,3}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_singleNumber() {
        // Test single number range {2..2}
        String input = "file_{2..2}.csv";
        String expected = "file_{2}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_mixedRangeAndValues() {
        // Test mixed range and single values {1..2,3,1..3}
        String input = "file_{1..2,3,1..3}.csv";
        String expected = "file_{1,2,3}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_multipleRanges() {
        // Test multiple ranges in one path {1..2}_{1..2}
        String input = "file_{1..2}_{1..2}.csv";
        String expected = "file_{1,2}_{1,2}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_largeRange() {
        // Test large range {0..9}
        String input = "file_{0..9}.csv";
        String expected = "file_{0,1,2,3,4,5,6,7,8,9}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_negativeNumbersFiltered() {
        // If start or end is negative, the entire range is skipped
        String input = "file_{-1..2}.csv";
        String expected = "file_{-1..2}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_allNegativeRange() {
        // Test all negative range {-3..-1}, should keep original
        String input = "file_{-3..-1}.csv";
        String expected = "file_{-3..-1}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_mixedWithNegative() {
        // The range -1..2 is skipped, only 1..3 is expanded
        String input = "file_{-1..2,1..3}.csv";
        String expected = "file_{1,2,3}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_invalidCharacters() {
        // Test invalid characters {Refrain,1..3}
        String input = "file_{Refrain,1..3}.csv";
        String expected = "file_{1,2,3}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_mixedInvalidAndValid() {
        // Range 3..1 is normalized to 1..3, resulting in {1,2,3}
        String input = "file_{3..1,2,1..2}.csv";
        String expected = "file_{1,2,3}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_noRange() {
        // Test no range pattern
        String input = "file_123.csv";
        String expected = "file_123.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_noNumericRange() {
        // Test no numeric range {a..z}
        String input = "file_{a..z}.csv";
        String expected = "file_{a..z}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_emptyBraces() {
        // Test empty braces {}
        String input = "file_{}.csv";
        String expected = "file_{}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_singleValue() {
        // Test single value in braces {5}
        String input = "file_{5}.csv";
        String expected = "file_{5}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_multipleValues() {
        // Test multiple single values {1,2,3}
        String input = "file_{1,2,3}.csv";
        String expected = "file_{1,2,3}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_duplicateRemoval() {
        // Test duplicate removal {1..3,2..4}
        String input = "file_{1..3,2..4}.csv";
        String expected = "file_{1,2,3,4}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_largeNumbers() {
        // Test large numbers {100..103}
        String input = "file_{100..103}.csv";
        String expected = "file_{100,101,102,103}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_zeroPadding() {
        // Test that zero-padding is not preserved (behavior test)
        // The function converts to integers, so "01" becomes "1"
        String input = "file_{01..03}.csv";
        String expected = "file_{1,2,3}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_complexPath() {
        // Test complex path with multiple patterns
        String input = "s3://bucket/data_{0..9}/file_{1..3}.csv";
        String expected = "s3://bucket/data_{0,1,2,3,4,5,6,7,8,9}/file_{1,2,3}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_noBraces() {
        // Test path without any braces
        String input = "s3://bucket/data.csv";
        String expected = "s3://bucket/data.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_specialCase() {
        // Test special case from PR description {2..4,6}
        String input = "data_{2..4,6}.csv";
        String expected = "data_{2,3,4,6}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testGetLongestPrefix_withGlobPattern() {
        // Test getLongestPrefix with glob patterns
        String input1 = "s3://bucket/path/to/file_{1..3}.csv";
        String expected1 = "s3://bucket/path/to/file_";
        String result1 = S3Util.getLongestPrefix(input1);
        Assert.assertEquals(expected1, result1);

        String input2 = "s3://bucket/path/*/file.csv";
        String expected2 = "s3://bucket/path/";
        String result2 = S3Util.getLongestPrefix(input2);
        Assert.assertEquals(expected2, result2);

        String input3 = "s3://bucket/path/file.csv";
        String expected3 = "s3://bucket/path/file.csv";
        String result3 = S3Util.getLongestPrefix(input3);
        Assert.assertEquals(expected3, result3);
    }

    @Test
    public void testExtendGlobs() {
        // Test extendGlobs method (which currently just calls extendGlobNumberRange)
        String input = "file_{1..3}.csv";
        String expected = "file_{1,2,3}.csv";
        String result = S3Util.extendGlobs(input);
        Assert.assertEquals(expected, result);
    }
}

