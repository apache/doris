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

package org.apache.doris.datasource.property.fileformat;

import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.thrift.TFileCompressType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TextFileFormatPropertiesTest {

    private TextFileFormatProperties textFileFormatProperties;

    @Before
    public void setUp() {
        textFileFormatProperties = new TextFileFormatProperties();
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValid() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(TextFileFormatProperties.PROP_COLUMN_SEPARATOR, ",");
        properties.put(TextFileFormatProperties.PROP_LINE_DELIMITER, "\n");
        properties.put(TextFileFormatProperties.PROP_SKIP_LINES, "1");

        textFileFormatProperties.analyzeFileFormatProperties(properties, true);

        Assert.assertEquals(",", textFileFormatProperties.getColumnSeparator());
        Assert.assertEquals("\n", textFileFormatProperties.getLineDelimiter());
        Assert.assertEquals(1, textFileFormatProperties.getSkipLines());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesInvalidSeparator() {
        Map<String, String> properties = new HashMap<>();
        properties.put(TextFileFormatProperties.PROP_COLUMN_SEPARATOR, "");

        Assert.assertThrows(AnalysisException.class, () -> {
            textFileFormatProperties.analyzeFileFormatProperties(properties, true);
        });
    }

    @Test
    public void testAnalyzeFileFormatPropertiesInvalidLineDelimiter() {
        Map<String, String> properties = new HashMap<>();
        properties.put(TextFileFormatProperties.PROP_LINE_DELIMITER, "");

        Assert.assertThrows(AnalysisException.class, () -> {
            textFileFormatProperties.analyzeFileFormatProperties(properties, true);
        });
    }

    @Test
    public void testAnalyzeFileFormatPropertiesSkipLinesNegative() {
        Map<String, String> properties = new HashMap<>();
        properties.put(TextFileFormatProperties.PROP_SKIP_LINES, "-1");

        Assert.assertThrows(AnalysisException.class, () -> {
            textFileFormatProperties.analyzeFileFormatProperties(properties, true);
        });
    }

    @Test
    public void testAnalyzeFileFormatPropertiesSkipLinesLargeValue() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(TextFileFormatProperties.PROP_SKIP_LINES, "1000");

        textFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(1000, textFileFormatProperties.getSkipLines());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesInvalidCompressType() {
        Map<String, String> properties = new HashMap<>();
        properties.put(TextFileFormatProperties.PROP_COMPRESS_TYPE, "invalid");
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "Unknown compression type: invalid",
                () -> textFileFormatProperties.analyzeFileFormatProperties(properties, true));
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidCompressType() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(TextFileFormatProperties.PROP_COMPRESS_TYPE, "gz");

        textFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(TFileCompressType.GZ, textFileFormatProperties.getCompressionType());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesSkipLinesAsString() {
        Map<String, String> properties = new HashMap<>();
        properties.put(TextFileFormatProperties.PROP_SKIP_LINES, "abc");

        Assert.assertThrows(NumberFormatException.class, () -> {
            textFileFormatProperties.analyzeFileFormatProperties(properties, true);
        });
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidColumnSeparator() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(TextFileFormatProperties.PROP_COLUMN_SEPARATOR, ";");

        textFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(";", textFileFormatProperties.getColumnSeparator());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesLineDelimiterAsString() {
        Map<String, String> properties = new HashMap<>();
        properties.put(TextFileFormatProperties.PROP_LINE_DELIMITER, "abc");
        textFileFormatProperties.analyzeFileFormatProperties(properties, true);
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidLineDelimiter() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(TextFileFormatProperties.PROP_LINE_DELIMITER, "\r\n");

        textFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals("\r\n", textFileFormatProperties.getLineDelimiter());
    }
}
