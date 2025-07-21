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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class JsonFileFormatPropertiesTest {

    private JsonFileFormatProperties jsonFileFormatProperties;

    @Before
    public void setUp() {
        jsonFileFormatProperties = new JsonFileFormatProperties();
    }

    @Test
    public void testAnalyzeFileFormatPropertiesEmpty() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        jsonFileFormatProperties.analyzeFileFormatProperties(properties, true);

        Assert.assertEquals("", jsonFileFormatProperties.getJsonRoot());
        Assert.assertEquals("", jsonFileFormatProperties.getJsonPaths());
        Assert.assertEquals(false, jsonFileFormatProperties.isStripOuterArray());
        Assert.assertEquals(false, jsonFileFormatProperties.isReadJsonByLine());
        Assert.assertEquals(false, jsonFileFormatProperties.isNumAsString());
        Assert.assertEquals(false, jsonFileFormatProperties.isFuzzyParse());
        Assert.assertEquals(CsvFileFormatProperties.DEFAULT_LINE_DELIMITER,
                jsonFileFormatProperties.getLineDelimiter());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidJsonRoot() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatProperties.PROP_JSON_ROOT, "data.items");

        jsonFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals("data.items", jsonFileFormatProperties.getJsonRoot());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidJsonPaths() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatProperties.PROP_JSON_PATHS,
                "[\"$.name\", \"$.age\", \"$.city\"]");

        jsonFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals("[\"$.name\", \"$.age\", \"$.city\"]", jsonFileFormatProperties.getJsonPaths());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesStripOuterArrayTrue() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatProperties.PROP_STRIP_OUTER_ARRAY, "true");

        jsonFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(true, jsonFileFormatProperties.isStripOuterArray());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesStripOuterArrayFalse() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatProperties.PROP_STRIP_OUTER_ARRAY, "false");

        jsonFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(false, jsonFileFormatProperties.isStripOuterArray());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesReadJsonByLineTrue() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatProperties.PROP_READ_JSON_BY_LINE, "true");

        jsonFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(true, jsonFileFormatProperties.isReadJsonByLine());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesReadJsonByLineFalse() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatProperties.PROP_READ_JSON_BY_LINE, "false");

        jsonFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(false, jsonFileFormatProperties.isReadJsonByLine());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesNumAsStringTrue() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatProperties.PROP_NUM_AS_STRING, "true");

        jsonFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(true, jsonFileFormatProperties.isNumAsString());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesNumAsStringFalse() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatProperties.PROP_NUM_AS_STRING, "false");

        jsonFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(false, jsonFileFormatProperties.isNumAsString());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesFuzzyParseTrue() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatProperties.PROP_FUZZY_PARSE, "true");

        jsonFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(true, jsonFileFormatProperties.isFuzzyParse());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesFuzzyParseFalse() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatProperties.PROP_FUZZY_PARSE, "false");

        jsonFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(false, jsonFileFormatProperties.isFuzzyParse());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesInvalidBooleanValue() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatProperties.PROP_FUZZY_PARSE, "invalid");

        jsonFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(false, jsonFileFormatProperties.isFuzzyParse());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesAllProperties() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatProperties.PROP_JSON_ROOT, "data.records");
        properties.put(JsonFileFormatProperties.PROP_JSON_PATHS, "[\"$.id\", \"$.name\"]");
        properties.put(JsonFileFormatProperties.PROP_STRIP_OUTER_ARRAY, "true");
        properties.put(JsonFileFormatProperties.PROP_READ_JSON_BY_LINE, "true");
        properties.put(JsonFileFormatProperties.PROP_NUM_AS_STRING, "true");
        properties.put(JsonFileFormatProperties.PROP_FUZZY_PARSE, "true");

        jsonFileFormatProperties.analyzeFileFormatProperties(properties, true);

        Assert.assertEquals("data.records", jsonFileFormatProperties.getJsonRoot());
        Assert.assertEquals("[\"$.id\", \"$.name\"]", jsonFileFormatProperties.getJsonPaths());
        Assert.assertEquals(true, jsonFileFormatProperties.isStripOuterArray());
        Assert.assertEquals(true, jsonFileFormatProperties.isReadJsonByLine());
        Assert.assertEquals(true, jsonFileFormatProperties.isNumAsString());
        Assert.assertEquals(true, jsonFileFormatProperties.isFuzzyParse());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesSpecialCharactersInJsonRoot() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatProperties.PROP_JSON_ROOT, "data.special@#$%^&*()");

        jsonFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals("data.special@#$%^&*()", jsonFileFormatProperties.getJsonRoot());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesComplexJsonPaths() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatProperties.PROP_JSON_PATHS,
                "[\"$.deeply.nested[0].array[*].field\", \"$.complex.path[?(@.type=='value')]\"]");

        jsonFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals("[\"$.deeply.nested[0].array[*].field\", \"$.complex.path[?(@.type=='value')]\"]",
                jsonFileFormatProperties.getJsonPaths());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesEmptyJsonPaths() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatProperties.PROP_JSON_PATHS, "");

        jsonFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals("", jsonFileFormatProperties.getJsonPaths());
    }
}
