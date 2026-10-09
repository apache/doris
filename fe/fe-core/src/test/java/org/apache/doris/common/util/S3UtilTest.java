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

import org.apache.doris.common.Config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

public class S3UtilTest {
    private String originalS3ClientHttpScheme;

    @BeforeEach
    public void setUp() {
        originalS3ClientHttpScheme = Config.s3_client_http_scheme;
    }

    @AfterEach
    public void tearDown() {
        Config.s3_client_http_scheme = originalS3ClientHttpScheme;
    }

    @Test
    public void testBuildEndpointUrlDefaultsToHttps() {
        Config.s3_client_http_scheme = "https";
        Assertions.assertEquals("https://s3.us-east-1.amazonaws.com",
                S3Util.buildEndpointUrl("s3.us-east-1.amazonaws.com"));
    }

    @Test
    public void testBuildEndpointUrlUsesConfiguredHttpScheme() {
        Config.s3_client_http_scheme = "http";
        Assertions.assertEquals("http://127.0.0.1:9000",
                S3Util.buildEndpointUrl("127.0.0.1:9000"));
    }

    @Test
    public void testBuildEndpointUrlPreservesExplicitSchemes() {
        Config.s3_client_http_scheme = "https";
        Assertions.assertEquals("http://127.0.0.1:9000",
                S3Util.buildEndpointUrl("http://127.0.0.1:9000"));

        Config.s3_client_http_scheme = "http";
        Assertions.assertEquals("https://s3.us-east-1.amazonaws.com",
                S3Util.buildEndpointUrl("https://s3.us-east-1.amazonaws.com"));
    }

    @Test
    public void testBuildS3ClientAppliesDefaultSchemeAtClientCreation() {
        Config.s3_client_http_scheme = "https";
        try (S3Client client = S3Util.buildS3Client(
                "127.0.0.1:9000",
                "us-east-1",
                true,
                StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk")))) {
            Assertions.assertEquals(URI.create("https://127.0.0.1:9000"),
                    client.serviceClientConfiguration().endpointOverride().orElseThrow());
        }
    }

    @Test
    public void testExtendGlobNumberRange_simpleRange() {
        // Test simple range expansion {1..3}
        String input = "file_{1..3}.csv";
        String expected = "file_{1,2,3}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_reverseRange() {
        // Test reverse range {3..1}, should normalize to {1,2,3}
        String input = "file_{3..1}.csv";
        String expected = "file_{1,2,3}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_singleNumber() {
        // Test single number range {2..2}
        String input = "file_{2..2}.csv";
        String expected = "file_{2}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_mixedRangeAndValues() {
        // Test mixed range and single values {1..2,3,1..3}
        String input = "file_{1..2,3,1..3}.csv";
        String expected = "file_{1,2,3}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_multipleRanges() {
        // Test multiple ranges in one path {1..2}_{1..2}
        String input = "file_{1..2}_{1..2}.csv";
        String expected = "file_{1,2}_{1,2}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_largeRange() {
        // Test large range {0..9}
        String input = "file_{0..9}.csv";
        String expected = "file_{0,1,2,3,4,5,6,7,8,9}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_negativeNumbersFiltered() {
        // If start or end is negative, the entire range is skipped
        String input = "file_{-1..2}.csv";
        String expected = "file_{-1..2}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_allNegativeRange() {
        // Test all negative range {-3..-1}, should keep original
        String input = "file_{-3..-1}.csv";
        String expected = "file_{-3..-1}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_mixedWithNegative() {
        // The range -1..2 is skipped, only 1..3 is expanded
        String input = "file_{-1..2,1..3}.csv";
        String expected = "file_{1,2,3}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_invalidCharacters() {
        // Test invalid characters {Refrain,1..3}
        String input = "file_{Refrain,1..3}.csv";
        String expected = "file_{1,2,3}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_mixedInvalidAndValid() {
        // Range 3..1 is normalized to 1..3, resulting in {1,2,3}
        String input = "file_{3..1,2,1..2}.csv";
        String expected = "file_{1,2,3}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_noRange() {
        // Test no range pattern
        String input = "file_123.csv";
        String expected = "file_123.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_noNumericRange() {
        // Test no numeric range {a..z}
        String input = "file_{a..z}.csv";
        String expected = "file_{a..z}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_emptyBraces() {
        // Test empty braces {}
        String input = "file_{}.csv";
        String expected = "file_{}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_singleValue() {
        // Test single value in braces {5}
        String input = "file_{5}.csv";
        String expected = "file_{5}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_multipleValues() {
        // Test multiple single values {1,2,3}
        String input = "file_{1,2,3}.csv";
        String expected = "file_{1,2,3}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_duplicateRemoval() {
        // Test duplicate removal {1..3,2..4}
        String input = "file_{1..3,2..4}.csv";
        String expected = "file_{1,2,3,4}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_largeNumbers() {
        // Test large numbers {100..103}
        String input = "file_{100..103}.csv";
        String expected = "file_{100,101,102,103}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_zeroPadding() {
        // Test that zero-padding is not preserved (behavior test)
        // The function converts to integers, so "01" becomes "1"
        String input = "file_{01..03}.csv";
        String expected = "file_{1,2,3}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_complexPath() {
        // Test complex path with multiple patterns
        String input = "s3://bucket/data_{0..9}/file_{1..3}.csv";
        String expected = "s3://bucket/data_{0,1,2,3,4,5,6,7,8,9}/file_{1,2,3}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_noBraces() {
        // Test path without any braces
        String input = "s3://bucket/data.csv";
        String expected = "s3://bucket/data.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testExtendGlobNumberRange_specialCase() {
        // Test special case from PR description {2..4,6}
        String input = "data_{2..4,6}.csv";
        String expected = "data_{2,3,4,6}.csv";
        String result = S3Util.extendGlobNumberRange(input);
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testGetLongestPrefix_withGlobPattern() {
        // Test getLongestPrefix with glob patterns
        String input1 = "s3://bucket/path/to/file_{1..3}.csv";
        String expected1 = "s3://bucket/path/to/file_";
        String result1 = S3Util.getLongestPrefix(input1);
        Assertions.assertEquals(expected1, result1);

        String input2 = "s3://bucket/path/*/file.csv";
        String expected2 = "s3://bucket/path/";
        String result2 = S3Util.getLongestPrefix(input2);
        Assertions.assertEquals(expected2, result2);

        String input3 = "s3://bucket/path/file.csv";
        String expected3 = "s3://bucket/path/file.csv";
        String result3 = S3Util.getLongestPrefix(input3);
        Assertions.assertEquals(expected3, result3);
    }

    @Test
    public void testExtendGlobs() {
        // Test extendGlobs method (which currently just calls extendGlobNumberRange)
        String input = "file_{1..3}.csv";
        String expected = "file_{1,2,3}.csv";
        String result = S3Util.extendGlobs(input);
        Assertions.assertEquals(expected, result);
    }

    // Tests for isDeterministicPattern

    @Test
    public void testIsDeterministicPattern_simpleFile() {
        // Simple file path without any patterns
        Assertions.assertTrue(S3Util.isDeterministicPattern("path/to/file.csv"));
    }

    @Test
    public void testIsDeterministicPattern_withBraces() {
        // Path with brace pattern (deterministic - can be expanded)
        Assertions.assertTrue(S3Util.isDeterministicPattern("path/to/file{1,2,3}.csv"));
        Assertions.assertTrue(S3Util.isDeterministicPattern("path/to/file{1..3}.csv"));
    }

    @Test
    public void testIsDeterministicPattern_withAsterisk() {
        // Path with asterisk wildcard (not deterministic)
        Assertions.assertFalse(S3Util.isDeterministicPattern("path/to/*.csv"));
        Assertions.assertFalse(S3Util.isDeterministicPattern("path/*/file.csv"));
    }

    @Test
    public void testIsDeterministicPattern_withQuestionMark() {
        // Path with question mark wildcard (not deterministic)
        Assertions.assertFalse(S3Util.isDeterministicPattern("path/to/file?.csv"));
    }

    @Test
    public void testIsDeterministicPattern_withBrackets() {
        // Non-negated bracket patterns are deterministic (can be expanded)
        Assertions.assertTrue(S3Util.isDeterministicPattern("path/to/file[0-9].csv"));
        Assertions.assertTrue(S3Util.isDeterministicPattern("path/to/file[abc].csv"));
        Assertions.assertTrue(S3Util.isDeterministicPattern("path/to/file[a-zA-Z].csv"));
    }

    @Test
    public void testIsDeterministicPattern_withNegatedBrackets() {
        // Negated bracket patterns are NOT deterministic
        Assertions.assertFalse(S3Util.isDeterministicPattern("path/to/file[!abc].csv"));
        Assertions.assertFalse(S3Util.isDeterministicPattern("path/to/file[^0-9].csv"));
    }

    @Test
    public void testIsDeterministicPattern_withMalformedBrackets() {
        // Malformed brackets (no closing ]) are NOT deterministic
        Assertions.assertFalse(S3Util.isDeterministicPattern("path/to/file[abc.csv"));
        // Empty brackets [] are NOT deterministic
        Assertions.assertFalse(S3Util.isDeterministicPattern("path/to/file[].csv"));
    }

    @Test
    public void testIsDeterministicPattern_withEscape() {
        // Path with escape character (not deterministic - complex pattern)
        Assertions.assertFalse(S3Util.isDeterministicPattern("path/to/file\\*.csv"));
    }

    @Test
    public void testIsDeterministicPattern_mixed() {
        // Path with both braces and wildcards
        Assertions.assertFalse(S3Util.isDeterministicPattern("path/to/file{1,2}/*.csv"));
    }

    // Tests for expandBracePatterns

    @Test
    public void testExpandBracePatterns_noBraces() {
        // No braces - returns single path
        List<String> result = S3Util.expandBracePatterns("path/to/file.csv");
        Assertions.assertEquals(Arrays.asList("path/to/file.csv"), result);
    }

    @Test
    public void testExpandBracePatterns_simpleBrace() {
        // Simple brace expansion
        List<String> result = S3Util.expandBracePatterns("file{1,2,3}.csv");
        Assertions.assertEquals(Arrays.asList("file1.csv", "file2.csv", "file3.csv"), result);
    }

    @Test
    public void testExpandBracePatterns_multipleBraces() {
        // Multiple brace expansions
        List<String> result = S3Util.expandBracePatterns("dir{a,b}/file{1,2}.csv");
        Assertions.assertEquals(Arrays.asList(
                "dira/file1.csv", "dira/file2.csv",
                "dirb/file1.csv", "dirb/file2.csv"), result);
    }

    @Test
    public void testExpandBracePatterns_emptyBrace() {
        // Empty brace content
        List<String> result = S3Util.expandBracePatterns("file{}.csv");
        Assertions.assertEquals(Arrays.asList("file.csv"), result);
    }

    @Test
    public void testExpandBracePatterns_singleValue() {
        // Single value in brace
        List<String> result = S3Util.expandBracePatterns("file{1}.csv");
        Assertions.assertEquals(Arrays.asList("file1.csv"), result);
    }

    @Test
    public void testExpandBracePatterns_withPath() {
        // Full path with braces: 2 years × 2 months = 4 paths
        List<String> result = S3Util.expandBracePatterns("data/year{2023,2024}/month{01,02}/file.csv");
        Assertions.assertEquals(4, result.size());
        Assertions.assertTrue(result.contains("data/year2023/month01/file.csv"));
        Assertions.assertTrue(result.contains("data/year2023/month02/file.csv"));
        Assertions.assertTrue(result.contains("data/year2024/month01/file.csv"));
        Assertions.assertTrue(result.contains("data/year2024/month02/file.csv"));
    }

    @Test
    public void testExpandBracePatterns_extendedRange() {
        // Test with extended range (after extendGlobs processing)
        String expanded = S3Util.extendGlobs("file{1..3}.csv");
        List<String> result = S3Util.expandBracePatterns(expanded);
        Assertions.assertEquals(Arrays.asList("file1.csv", "file2.csv", "file3.csv"), result);
    }

    @Test
    public void testExpandBracePatterns_malformedBrace() {
        // Malformed brace pattern (no closing }) - treated as literal
        List<String> result = S3Util.expandBracePatterns("file{1,2.csv");
        Assertions.assertEquals(Arrays.asList("file{1,2.csv"), result);
    }

    @Test
    public void testExpandBracePatterns_malformedBraceWithDots() {
        // Malformed range-like pattern (no closing }) - treated as literal
        List<String> result = S3Util.expandBracePatterns("file{1..csv");
        Assertions.assertEquals(Arrays.asList("file{1..csv"), result);
    }

    // Tests for expandBracketPatterns

    @Test
    public void testExpandBracketPatterns_noBrackets() {
        // No brackets - returns unchanged
        Assertions.assertEquals("path/to/file.csv", S3Util.expandBracketPatterns("path/to/file.csv"));
    }

    @Test
    public void testExpandBracketPatterns_simpleCharList() {
        // [abc] => {a,b,c}
        Assertions.assertEquals("file{a,b,c}.csv", S3Util.expandBracketPatterns("file[abc].csv"));
    }

    @Test
    public void testExpandBracketPatterns_charRange() {
        // [0-3] => {0,1,2,3}
        Assertions.assertEquals("file{0,1,2,3}.csv", S3Util.expandBracketPatterns("file[0-3].csv"));
    }

    @Test
    public void testExpandBracketPatterns_mixedRangeAndChars() {
        // [a-cX] => {a,b,c,X}
        Assertions.assertEquals("file{a,b,c,X}.csv", S3Util.expandBracketPatterns("file[a-cX].csv"));
    }

    @Test
    public void testExpandBracketPatterns_multipleRanges() {
        // [a-c0-2] => {a,b,c,0,1,2}
        Assertions.assertEquals("file{a,b,c,0,1,2}.csv", S3Util.expandBracketPatterns("file[a-c0-2].csv"));
    }

    @Test
    public void testExpandBracketPatterns_fullPipeline() {
        // Full pipeline: bracket expansion -> extendGlobs -> brace expansion
        // file[abc].csv => file{a,b,c}.csv => [filea.csv, fileb.csv, filec.csv]
        String bracketExpanded = S3Util.expandBracketPatterns("file[abc].csv");
        String globExpanded = S3Util.extendGlobs(bracketExpanded);
        List<String> result = S3Util.expandBracePatterns(globExpanded);
        Assertions.assertEquals(Arrays.asList("filea.csv", "fileb.csv", "filec.csv"), result);
    }

    @Test
    public void testExpandBracketPatterns_withBracesAndBrackets() {
        // Mixed brackets and braces: dir[ab]/file{1,2}.csv
        // => dir{a,b}/file{1,2}.csv => [dira/file1.csv, dira/file2.csv, dirb/file1.csv, dirb/file2.csv]
        String bracketExpanded = S3Util.expandBracketPatterns("dir[ab]/file{1,2}.csv");
        Assertions.assertEquals("dir{a,b}/file{1,2}.csv", bracketExpanded);
        List<String> result = S3Util.expandBracePatterns(bracketExpanded);
        Assertions.assertEquals(Arrays.asList(
                "dira/file1.csv", "dira/file2.csv",
                "dirb/file1.csv", "dirb/file2.csv"), result);
    }

    @Test
    public void testExpandBracketPatterns_digitRange() {
        // [0-9] => {0,1,2,3,4,5,6,7,8,9}
        String expanded = S3Util.expandBracketPatterns("part[0-9].dat");
        List<String> result = S3Util.expandBracePatterns(expanded);
        Assertions.assertEquals(10, result.size());
        Assertions.assertTrue(result.contains("part0.dat"));
        Assertions.assertTrue(result.contains("part9.dat"));
    }

    @Test
    public void testExpandBracketPatterns_malformedBracket() {
        // Malformed bracket (no closing ]) - [ kept as literal
        Assertions.assertEquals("file[abc.csv", S3Util.expandBracketPatterns("file[abc.csv"));
    }
}
