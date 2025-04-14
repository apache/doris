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
import org.apache.doris.thrift.TFileFormatType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class CsvFileFormatConfiguratorTest {

    private CsvFileFormatConfigurator csvConfigurator;

    @Before
    public void setUp() {
        csvConfigurator = new CsvFileFormatConfigurator(TFileFormatType.FORMAT_CSV_PLAIN);
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValid() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_COLUMN_SEPARATOR, ",");
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_LINE_DELIMITER, "\n");
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_SKIP_LINES, "1");

        csvConfigurator.analyzeFileFormatProperties(properties);

        Assert.assertEquals(",", csvConfigurator.getColumnSeparator());
        Assert.assertEquals("\n", csvConfigurator.getLineDelimiter());
        Assert.assertEquals(1, csvConfigurator.getSkipLines());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesInvalidSeparator() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_COLUMN_SEPARATOR, "");

        Assert.assertThrows(AnalysisException.class, () -> {
            csvConfigurator.analyzeFileFormatProperties(properties);
        });
    }

    @Test
    public void testAnalyzeFileFormatPropertiesInvalidLineDelimiter() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_LINE_DELIMITER, "");

        Assert.assertThrows(AnalysisException.class, () -> {
            csvConfigurator.analyzeFileFormatProperties(properties);
        });
    }

    @Test
    public void testAnalyzeFileFormatPropertiesInvalidEnclose() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_ENCLOSE, "invalid");

        Assert.assertThrows(AnalysisException.class, () -> {
            csvConfigurator.analyzeFileFormatProperties(properties);
        });
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidEnclose() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_ENCLOSE, "\"");

        csvConfigurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals((byte) '"', csvConfigurator.getEnclose());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesSkipLinesNegative() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_SKIP_LINES, "-1");

        Assert.assertThrows(AnalysisException.class, () -> {
            csvConfigurator.analyzeFileFormatProperties(properties);
        });
    }

    @Test
    public void testAnalyzeFileFormatPropertiesSkipLinesLargeValue() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_SKIP_LINES, "1000");

        csvConfigurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals(1000, csvConfigurator.getSkipLines());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesTrimDoubleQuotesTrue() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_TRIM_DOUBLE_QUOTES, "true");

        csvConfigurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals(true, csvConfigurator.isTrimDoubleQuotes());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesTrimDoubleQuotesFalse() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_TRIM_DOUBLE_QUOTES, "false");

        csvConfigurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals(false, csvConfigurator.isTrimDoubleQuotes());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesInvalidCompressType() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_COMPRESS_TYPE, "invalid");

        Assert.assertThrows(AnalysisException.class, () -> {
            csvConfigurator.analyzeFileFormatProperties(properties);
        });
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidCompressType() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_COMPRESS_TYPE, "gzip");

        csvConfigurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals(TFileCompressType.GZ, csvConfigurator.getCompressionType());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesEmptyCsvSchema() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_CSV_SCHEMA, "");

        Assert.assertThrows(AnalysisException.class, () -> {
            csvConfigurator.analyzeFileFormatProperties(properties);
        });
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidCsvSchema() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_CSV_SCHEMA, "column1,column2,column3");

        csvConfigurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals(3, csvConfigurator.getCsvSchema().size());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidEncloseMultipleCharacters() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_ENCLOSE, "\"\"");

        Assert.assertThrows(AnalysisException.class, () -> {
            csvConfigurator.analyzeFileFormatProperties(properties);
        });
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidEncloseEmpty() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_ENCLOSE, "");

        Assert.assertThrows(AnalysisException.class, () -> {
            csvConfigurator.analyzeFileFormatProperties(properties);
        });
    }

    @Test
    public void testAnalyzeFileFormatPropertiesSkipLinesAsString() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_SKIP_LINES, "abc");

        Assert.assertThrows(AnalysisException.class, () -> {
            csvConfigurator.analyzeFileFormatProperties(properties);
        });
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidColumnSeparator() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_COLUMN_SEPARATOR, ";");

        csvConfigurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals(";", csvConfigurator.getColumnSeparator());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesLineDelimiterAsString() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_LINE_DELIMITER, "abc");

        Assert.assertThrows(AnalysisException.class, () -> {
            csvConfigurator.analyzeFileFormatProperties(properties);
        });
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidLineDelimiter() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_LINE_DELIMITER, "\r\n");

        csvConfigurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals("\r\n", csvConfigurator.getLineDelimiter());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidTrimDoubleQuotes() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_TRIM_DOUBLE_QUOTES, "true");

        csvConfigurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals(true, csvConfigurator.isTrimDoubleQuotes());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesInvalidTrimDoubleQuotes() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_TRIM_DOUBLE_QUOTES, "invalid");

        Assert.assertThrows(AnalysisException.class, () -> {
            csvConfigurator.analyzeFileFormatProperties(properties);
        });
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidCsvSchemaWithSpaces() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_CSV_SCHEMA, " column1 , column2 , column3 ");

        csvConfigurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals(3, csvConfigurator.getCsvSchema().size());
        Assert.assertEquals("column1", csvConfigurator.getCsvSchema().get(0).getName().trim());
        Assert.assertEquals("column2", csvConfigurator.getCsvSchema().get(1).getName().trim());
        Assert.assertEquals("column3", csvConfigurator.getCsvSchema().get(2).getName().trim());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidCsvSchemaWithSpecialCharacters() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatConfigurator.CsvFileFormatProperties.PROP_CSV_SCHEMA, "col1@#$,col2&*(),col3");

        csvConfigurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals(3, csvConfigurator.getCsvSchema().size());
        Assert.assertEquals("col1@#$", csvConfigurator.getCsvSchema().get(0).getName());
        Assert.assertEquals("col2&*()", csvConfigurator.getCsvSchema().get(1).getName());
        Assert.assertEquals("col3", csvConfigurator.getCsvSchema().get(2).getName());
    }
}
