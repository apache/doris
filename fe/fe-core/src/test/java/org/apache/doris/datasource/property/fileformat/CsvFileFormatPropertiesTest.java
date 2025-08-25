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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.thrift.TFileCompressType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class CsvFileFormatPropertiesTest {

    private CsvFileFormatProperties csvFileFormatProperties;

    @Before
    public void setUp() {
        csvFileFormatProperties = new CsvFileFormatProperties("csv");
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValid() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_COLUMN_SEPARATOR, ",");
        properties.put(CsvFileFormatProperties.PROP_LINE_DELIMITER, "\n");
        properties.put(CsvFileFormatProperties.PROP_SKIP_LINES, "1");

        csvFileFormatProperties.analyzeFileFormatProperties(properties, true);

        Assert.assertEquals(",", csvFileFormatProperties.getColumnSeparator());
        Assert.assertEquals("\n", csvFileFormatProperties.getLineDelimiter());
        Assert.assertEquals(1, csvFileFormatProperties.getSkipLines());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesInvalidSeparator() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_COLUMN_SEPARATOR, "");

        Assert.assertThrows(AnalysisException.class, () -> {
            csvFileFormatProperties.analyzeFileFormatProperties(properties, true);
        });
    }

    @Test
    public void testAnalyzeFileFormatPropertiesInvalidLineDelimiter() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_LINE_DELIMITER, "");

        Assert.assertThrows(AnalysisException.class, () -> {
            csvFileFormatProperties.analyzeFileFormatProperties(properties, true);
        });
    }

    @Test
    public void testAnalyzeFileFormatPropertiesInvalidEnclose() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_ENCLOSE, "invalid");

        Assert.assertThrows(AnalysisException.class, () -> {
            csvFileFormatProperties.analyzeFileFormatProperties(properties, true);
        });
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidEnclose() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_ENCLOSE, "\"");

        csvFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals((byte) '"', csvFileFormatProperties.getEnclose());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesSkipLinesNegative() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_SKIP_LINES, "-1");

        Assert.assertThrows(AnalysisException.class, () -> {
            csvFileFormatProperties.analyzeFileFormatProperties(properties, true);
        });
    }

    @Test
    public void testAnalyzeFileFormatPropertiesSkipLinesLargeValue() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_SKIP_LINES, "1000");

        csvFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(1000, csvFileFormatProperties.getSkipLines());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesTrimDoubleQuotesTrue() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_TRIM_DOUBLE_QUOTES, "true");

        csvFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(true, csvFileFormatProperties.isTrimDoubleQuotes());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesTrimDoubleQuotesFalse() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_TRIM_DOUBLE_QUOTES, "false");

        csvFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(false, csvFileFormatProperties.isTrimDoubleQuotes());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesInvalidCompressType() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_COMPRESS_TYPE, "invalid");
        csvFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(TFileCompressType.UNKNOWN, csvFileFormatProperties.getCompressionType());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidCompressType() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_COMPRESS_TYPE, "gz");

        csvFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(TFileCompressType.GZ, csvFileFormatProperties.getCompressionType());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesEmptyCsvSchema() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_CSV_SCHEMA, "");
        csvFileFormatProperties.analyzeFileFormatProperties(properties, true);
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidEncloseMultipleCharacters() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_ENCLOSE, "\"\"");

        Assert.assertThrows(AnalysisException.class, () -> {
            csvFileFormatProperties.analyzeFileFormatProperties(properties, true);
        });
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidEncloseEmpty() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_ENCLOSE, "");

        csvFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(0, csvFileFormatProperties.getEnclose());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesSkipLinesAsString() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_SKIP_LINES, "abc");

        Assert.assertThrows(NumberFormatException.class, () -> {
            csvFileFormatProperties.analyzeFileFormatProperties(properties, true);
        });
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidColumnSeparator() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_COLUMN_SEPARATOR, ";");

        csvFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(";", csvFileFormatProperties.getColumnSeparator());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesLineDelimiterAsString() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_LINE_DELIMITER, "abc");
        csvFileFormatProperties.analyzeFileFormatProperties(properties, true);
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidLineDelimiter() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_LINE_DELIMITER, "\r\n");

        csvFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals("\r\n", csvFileFormatProperties.getLineDelimiter());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidTrimDoubleQuotes() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_TRIM_DOUBLE_QUOTES, "true");

        csvFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(true, csvFileFormatProperties.isTrimDoubleQuotes());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesInvalidTrimDoubleQuotes() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_TRIM_DOUBLE_QUOTES, "invalid");

        csvFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(false, csvFileFormatProperties.isTrimDoubleQuotes());
    }
}
