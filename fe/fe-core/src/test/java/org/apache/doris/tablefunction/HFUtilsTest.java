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

package org.apache.doris.tablefunction;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.tablefunction.HFUtils.ParsedHFUrl;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class HFUtilsTest {

    @Test
    public void testValidHfUrlParsing() throws AnalysisException {
        // Test basic dataset URL
        String url1 = "hf://datasets/lhoestq/demo1/default/train/0000.parquet";
        ParsedHFUrl parsed1 = HFUtils.parseHfUrl(url1);

        Assert.assertEquals("datasets", parsed1.getRepoType());
        Assert.assertEquals("lhoestq/demo1", parsed1.getRepository());
        Assert.assertEquals("main", parsed1.getRevision());
        Assert.assertEquals("/default/train/0000.parquet", parsed1.getPath());
        Assert.assertEquals("https://huggingface.co", parsed1.getEndpoint());

        // Test URL with revision
        String url2 = "hf://datasets/username/dataset@v1.0/path/to/file.csv";
        ParsedHFUrl parsed2 = HFUtils.parseHfUrl(url2);

        Assert.assertEquals("datasets", parsed2.getRepoType());
        Assert.assertEquals("username/dataset", parsed2.getRepository());
        Assert.assertEquals("v1.0", parsed2.getRevision());
        Assert.assertEquals("/path/to/file.csv", parsed2.getPath());

        // Test spaces URL
        String url3 = "hf://spaces/gradio/calculator/app.py";
        ParsedHFUrl parsed3 = HFUtils.parseHfUrl(url3);

        Assert.assertEquals("spaces", parsed3.getRepoType());
        Assert.assertEquals("gradio/calculator", parsed3.getRepository());
        Assert.assertEquals("main", parsed3.getRevision());
        Assert.assertEquals("/app.py", parsed3.getPath());

        // Test URL with empty path
        String url4 = "hf://datasets/user/repo/";
        ParsedHFUrl parsed4 = HFUtils.parseHfUrl(url4);

        Assert.assertEquals("datasets", parsed4.getRepoType());
        Assert.assertEquals("user/repo", parsed4.getRepository());
        Assert.assertEquals("main", parsed4.getRevision());
        Assert.assertEquals("/", parsed4.getPath());

        // Test URL with HuggingFace web interface format (/blob/main/)
        String url5 = "hf://datasets/fka/awesome-chatgpt-prompts/blob/main/prompts.csv";
        ParsedHFUrl parsed5 = HFUtils.parseHfUrl(url5);

        Assert.assertEquals("datasets", parsed5.getRepoType());
        Assert.assertEquals("fka/awesome-chatgpt-prompts", parsed5.getRepository());
        Assert.assertEquals("main", parsed5.getRevision());
        Assert.assertEquals("/prompts.csv", parsed5.getPath());

        // Test URL with HuggingFace web interface format (/tree/v1.0/)
        String url6 = "hf://datasets/user/dataset/tree/v1.0/data/file.txt";
        ParsedHFUrl parsed6 = HFUtils.parseHfUrl(url6);

        Assert.assertEquals("datasets", parsed6.getRepoType());
        Assert.assertEquals("user/dataset", parsed6.getRepository());
        Assert.assertEquals("v1.0", parsed6.getRevision());
        Assert.assertEquals("/data/file.txt", parsed6.getPath());
    }

    @Test
    public void testHttpUrlConversion() throws AnalysisException {
        // Test basic conversion
        String hfUrl1 = "hf://datasets/lhoestq/demo1/default/train/0000.parquet";
        String httpUrl1 = HFUtils.convertHfUrlToHttpUrl(hfUrl1);
        String expected1 = "https://huggingface.co/datasets/lhoestq/demo1/resolve/main/default/train/0000.parquet";
        Assert.assertEquals(expected1, httpUrl1);

        // Test conversion with revision
        String hfUrl2 = "hf://datasets/username/dataset@v1.0/path/to/file.csv";
        String httpUrl2 = HFUtils.convertHfUrlToHttpUrl(hfUrl2);
        String expected2 = "https://huggingface.co/datasets/username/dataset/resolve/v1.0/path/to/file.csv";
        Assert.assertEquals(expected2, httpUrl2);

        // Test spaces conversion
        String hfUrl3 = "hf://spaces/gradio/calculator/app.py";
        String httpUrl3 = HFUtils.convertHfUrlToHttpUrl(hfUrl3);
        String expected3 = "https://huggingface.co/spaces/gradio/calculator/resolve/main/app.py";
        Assert.assertEquals(expected3, httpUrl3);

        // Test HuggingFace web interface format conversion
        String hfUrl4 = "hf://datasets/fka/awesome-chatgpt-prompts/blob/main/prompts.csv";
        String httpUrl4 = HFUtils.convertHfUrlToHttpUrl(hfUrl4);
        String expected4 = "https://huggingface.co/datasets/fka/awesome-chatgpt-prompts/resolve/main/prompts.csv";
        Assert.assertEquals(expected4, httpUrl4);
    }

    @Test
    public void testTreeApiUrlGeneration() throws AnalysisException {
        String hfUrl = "hf://datasets/lhoestq/demo1/default/train";
        ParsedHFUrl parsed = HFUtils.parseHfUrl(hfUrl);

        // Test without limit
        String treeUrl1 = HFUtils.buildTreeApiUrl(parsed, 0);
        String expected1 = "https://huggingface.co/api/datasets/lhoestq/demo1/tree/main/default/train";
        Assert.assertEquals(expected1, treeUrl1);

        // Test with limit
        String treeUrl2 = HFUtils.buildTreeApiUrl(parsed, 100);
        String expected2 = "https://huggingface.co/api/datasets/lhoestq/demo1/tree/main/default/train?limit=100";
        Assert.assertEquals(expected2, treeUrl2);
    }

    @Test
    public void testRepositoryInfo() throws AnalysisException {
        String hfUrl1 = "hf://datasets/lhoestq/demo1/default/train/0000.parquet";
        String repoInfo1 = HFUtils.getRepositoryInfo(hfUrl1);
        Assert.assertEquals("datasets/lhoestq/demo1@main", repoInfo1);

        String hfUrl2 = "hf://datasets/username/dataset@v1.0/path/to/file.csv";
        String repoInfo2 = HFUtils.getRepositoryInfo(hfUrl2);
        Assert.assertEquals("datasets/username/dataset@v1.0", repoInfo2);
    }

    @Test
    public void testValidHfUrlValidation() {
        // Valid URLs
        Assert.assertTrue(HFUtils.isValidHfUrl("hf://datasets/user/repo/file.txt"));
        Assert.assertTrue(HFUtils.isValidHfUrl("hf://spaces/user/space/app.py"));
        Assert.assertTrue(HFUtils.isValidHfUrl("hf://datasets/user/repo@v1.0/file.txt"));

        // Invalid URLs
        Assert.assertFalse(HFUtils.isValidHfUrl(null));
        Assert.assertFalse(HFUtils.isValidHfUrl(""));
        Assert.assertFalse(HFUtils.isValidHfUrl("http://example.com"));
        Assert.assertFalse(HFUtils.isValidHfUrl("hf://"));
        Assert.assertFalse(HFUtils.isValidHfUrl("hf://datasets"));
        Assert.assertFalse(HFUtils.isValidHfUrl("hf://datasets/"));
        Assert.assertFalse(HFUtils.isValidHfUrl("hf://datasets/user"));
        Assert.assertFalse(HFUtils.isValidHfUrl("hf://datasets/user/repo")); // Missing path
        Assert.assertFalse(HFUtils.isValidHfUrl("hf://invalid/user/repo/file.txt"));
    }

    @Test
    public void testInvalidUrlExceptions() {
        // Test null/empty URL
        try {
            HFUtils.parseHfUrl(null);
            Assert.fail("Should throw AnalysisException for null URL");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("cannot be null or empty"));
        }

        try {
            HFUtils.parseHfUrl("");
            Assert.fail("Should throw AnalysisException for empty URL");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("cannot be null or empty"));
        }

        // Test non-hf URL
        try {
            HFUtils.parseHfUrl("http://example.com");
            Assert.fail("Should throw AnalysisException for non-hf URL");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("must start with 'hf://'"));
        }

        // Test incomplete URL
        try {
            HFUtils.parseHfUrl("hf://datasets");
            Assert.fail("Should throw AnalysisException for incomplete URL");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to parse HuggingFace URL"));
        }

        // Test invalid repository type
        try {
            HFUtils.parseHfUrl("hf://models/user/model/file.txt");
            Assert.fail("Should throw AnalysisException for unsupported repo type");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("only supports 'datasets' and 'spaces'"));
        }

        // Test empty username
        try {
            HFUtils.parseHfUrl("hf://datasets//repo/file.txt");
            Assert.fail("Should throw AnalysisException for empty username");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to parse HuggingFace URL"));
        }

        // Test empty revision
        try {
            HFUtils.parseHfUrl("hf://datasets/user/repo@/file.txt");
            Assert.fail("Should throw AnalysisException for empty revision");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to parse HuggingFace URL"));
        }
    }

    @Test
    public void testConvertHfUrlToHttpUrlExceptions() {
        // Test null URL
        try {
            HFUtils.convertHfUrlToHttpUrl(null);
            Assert.fail("Should throw AnalysisException for null URL");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("cannot be null or empty"));
        }

        // Test empty URL
        try {
            HFUtils.convertHfUrlToHttpUrl("");
            Assert.fail("Should throw AnalysisException for empty URL");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("cannot be null or empty"));
        }

        // Test invalid URL
        try {
            HFUtils.convertHfUrlToHttpUrl("http://example.com");
            Assert.fail("Should throw AnalysisException for invalid URL");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("must start with 'hf://'"));
        }
    }

    @Test
    public void testEdgeCases() throws AnalysisException {
        // Test URL with special characters in path
        String hfUrl1 = "hf://datasets/user/repo/path with spaces/file-name_123.parquet";
        String httpUrl1 = HFUtils.convertHfUrlToHttpUrl(hfUrl1);
        String expected1 = "https://huggingface.co/datasets/user/repo/resolve/main/path with spaces/file-name_123.parquet";
        Assert.assertEquals(expected1, httpUrl1);

        // Test URL with multiple slashes in path
        String hfUrl2 = "hf://datasets/user/repo/path/to/deep/nested/file.txt";
        String httpUrl2 = HFUtils.convertHfUrlToHttpUrl(hfUrl2);
        String expected2 = "https://huggingface.co/datasets/user/repo/resolve/main/path/to/deep/nested/file.txt";
        Assert.assertEquals(expected2, httpUrl2);

        // Test URL with revision containing special characters
        String hfUrl3 = "hf://datasets/user/repo@feature-branch-v1.0/file.txt";
        String httpUrl3 = HFUtils.convertHfUrlToHttpUrl(hfUrl3);
        String expected3 = "https://huggingface.co/datasets/user/repo/resolve/feature-branch-v1.0/file.txt";
        Assert.assertEquals(expected3, httpUrl3);
    }

    @Test
    public void testGlobFunctionality() throws AnalysisException {
        // Test wildcard detection
        Assert.assertTrue(HFUtils.containsWildcards("/path/*.parquet"));
        Assert.assertTrue(HFUtils.containsWildcards("/path/**/train/*.csv"));
        Assert.assertTrue(HFUtils.containsWildcards("/path/file_[abc].txt"));
        Assert.assertTrue(HFUtils.containsWildcards("/path/file_{1,2,3}.txt"));
        Assert.assertFalse(HFUtils.containsWildcards("/path/file.txt"));
        Assert.assertFalse(HFUtils.containsWildcards(""));
        Assert.assertFalse(HFUtils.containsWildcards(null));

        // Test longest prefix extraction
        Assert.assertEquals("/path", HFUtils.getLongestPrefixWithoutWildcards("/path/*.parquet"));
        Assert.assertEquals("/path", HFUtils.getLongestPrefixWithoutWildcards("/path/**/train/*.csv"));
        Assert.assertEquals("/path", HFUtils.getLongestPrefixWithoutWildcards("/path/file_[abc].txt"));
        Assert.assertEquals("/path/to/deep", HFUtils.getLongestPrefixWithoutWildcards("/path/to/deep/*.txt"));
        Assert.assertEquals("/path/file.txt", HFUtils.getLongestPrefixWithoutWildcards("/path/file.txt"));
        Assert.assertEquals("", HFUtils.getLongestPrefixWithoutWildcards("*.txt"));

        // Test glob URL validation
        Assert.assertTrue(HFUtils.isValidGlobUrl("hf://datasets/user/repo/path/*.parquet"));
        Assert.assertTrue(HFUtils.isValidGlobUrl("hf://datasets/user/repo/path/**/train/*.csv"));
        Assert.assertFalse(HFUtils.isValidGlobUrl("hf://datasets/user/repo/path/file.txt"));
        Assert.assertFalse(HFUtils.isValidGlobUrl("http://example.com/*.txt"));
        Assert.assertFalse(HFUtils.isValidGlobUrl(null));
    }

    @Test
    public void testGlobPatternMatching() {
        // Test basic pattern matching
        Assert.assertTrue(HFUtils.matchGlobPattern("file.txt", "*.txt"));
        Assert.assertTrue(HFUtils.matchGlobPattern("file.parquet", "*.parquet"));
        Assert.assertTrue(HFUtils.matchGlobPattern("file_a.txt", "file_[abc].txt"));
        Assert.assertFalse(HFUtils.matchGlobPattern("file_d.txt", "file_[abc].txt"));
        Assert.assertFalse(HFUtils.matchGlobPattern("file.csv", "*.txt"));

        // Test edge cases
        Assert.assertFalse(HFUtils.matchGlobPattern(null, "*.txt"));
        Assert.assertFalse(HFUtils.matchGlobPattern("file.txt", null));
        Assert.assertFalse(HFUtils.matchGlobPattern("", "*.txt"));
    }

    @Test
    public void testPathSplitting() {
        List<String> components1 = HFUtils.splitPath("/path/to/file.txt");
        Assert.assertEquals(3, components1.size());
        Assert.assertEquals("path", components1.get(0));
        Assert.assertEquals("to", components1.get(1));
        Assert.assertEquals("file.txt", components1.get(2));

        List<String> components2 = HFUtils.splitPath("path/to/file.txt");
        Assert.assertEquals(3, components2.size());
        Assert.assertEquals("path", components2.get(0));

        List<String> components3 = HFUtils.splitPath("");
        Assert.assertEquals(0, components3.size());

        List<String> components4 = HFUtils.splitPath(null);
        Assert.assertEquals(0, components4.size());
    }

    @Test
    public void testAdvancedPatternMatching() {
        // Test ** recursive matching
        List<String> pathComponents1 = HFUtils.splitPath("path/to/deep/file.txt");
        List<String> patternComponents1 = HFUtils.splitPath("path/**/file.txt");
        Assert.assertTrue(HFUtils.matchPathComponents(pathComponents1, patternComponents1));

        List<String> pathComponents2 = HFUtils.splitPath("path/file.txt");
        List<String> patternComponents2 = HFUtils.splitPath("path/**/file.txt");
        Assert.assertTrue(HFUtils.matchPathComponents(pathComponents2, patternComponents2));

        List<String> pathComponents3 = HFUtils.splitPath("different/file.txt");
        List<String> patternComponents3 = HFUtils.splitPath("path/**/file.txt");
        Assert.assertFalse(HFUtils.matchPathComponents(pathComponents3, patternComponents3));

        // Test single * matching
        List<String> pathComponents4 = HFUtils.splitPath("path/train/file.txt");
        List<String> patternComponents4 = HFUtils.splitPath("path/*/file.txt");
        Assert.assertTrue(HFUtils.matchPathComponents(pathComponents4, patternComponents4));

        List<String> pathComponents5 = HFUtils.splitPath("path/to/deep/file.txt");
        List<String> patternComponents5 = HFUtils.splitPath("path/*/file.txt");
        Assert.assertFalse(HFUtils.matchPathComponents(pathComponents5, patternComponents5));
    }

    @Test
    public void testGlobExpansion() throws AnalysisException {
        // Test non-glob URL (should return single result)
        String nonGlobUrl = "hf://datasets/user/repo/path/file.txt";
        List<String> result1 = HFUtils.expandGlob(nonGlobUrl);
        Assert.assertEquals(1, result1.size());
        Assert.assertEquals("https://huggingface.co/datasets/user/repo/resolve/main/path/file.txt", result1.get(0));

        // Test glob URL validation
        String globUrl1 = "hf://datasets/user/repo/path/*.parquet";
        Assert.assertTrue(HFUtils.isValidGlobUrl(globUrl1));

        String globUrl2 = "hf://datasets/user/repo/path/*.csv";
        Assert.assertTrue(HFUtils.isValidGlobUrl(globUrl2));

        // Note: Real glob expansion tests would require actual HuggingFace API calls
        // The actual expansion will fail without real API access, but URL parsing works
    }

    @Test
    public void testGlobExpansionExceptions() throws AnalysisException {
        // Test null URL
        try {
            HFUtils.expandGlob(null);
            Assert.fail("Should throw AnalysisException for null URL");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("cannot be null or empty"));
        }

        // Test empty URL
        try {
            HFUtils.expandGlob("");
            Assert.fail("Should throw AnalysisException for empty URL");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("cannot be null or empty"));
        }

        // Test invalid URL
        try {
            HFUtils.expandGlob("http://example.com/*.txt");
            Assert.fail("Should throw AnalysisException for invalid URL");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("must start with 'hf://'"));
        }

        List<String> res = HFUtils.expandGlob("hf://datasets/fka/awesome-chatgpt-prompts/blob/main/prompts.csv");
        Assert.assertEquals(1, res.size());
        Assert.assertEquals("https://huggingface.co/datasets/fka/awesome-chatgpt-prompts/resolve/main/prompts.csv",
                res.get(0));

        ParsedHFUrl parsed = HFUtils.parseHfUrl("hf://datasets/fka/awesome-chatgpt-prompts/blob/main/prompts.csv");
        Assert.assertEquals("/prompts.csv", parsed.getPath());

        res = HFUtils.expandGlob("hf://datasets/fka/awesome-chatgpt-prompts/blob/main/*");
        Assert.assertEquals(3, res.size());
        Assert.assertTrue(res.contains(
                "https://huggingface.co/datasets/fka/awesome-chatgpt-prompts/resolve/main/prompts.csv"));
        Assert.assertTrue(res.contains(
                "https://huggingface.co/datasets/fka/awesome-chatgpt-prompts/resolve/main/.gitattributes"));
        Assert.assertTrue(res.contains(
                "https://huggingface.co/datasets/fka/awesome-chatgpt-prompts/resolve/main/README.md"));
    }
}

