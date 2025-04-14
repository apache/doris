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
import org.apache.doris.thrift.TFileFormatType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class JsonFileFormatConfiguratorTest {

    private JsonFileFormatConfigurator configurator;

    @Before
    public void setUp() {
        configurator = new JsonFileFormatConfigurator(TFileFormatType.FORMAT_JSON);
    }

    @Test
    public void testAnalyzeFileFormatPropertiesEmpty() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        configurator.analyzeFileFormatProperties(properties);

        Assert.assertEquals("", configurator.getJsonRoot());
        Assert.assertEquals("", configurator.getJsonPaths());
        Assert.assertEquals(false, configurator.isStripOuterArray());
        Assert.assertEquals(false, configurator.isReadJsonByLine());
        Assert.assertEquals(false, configurator.isNumAsString());
        Assert.assertEquals(false, configurator.isFuzzyParse());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidJsonRoot() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatConfigurator.JsonFileFormatProperties.PROP_JSON_ROOT, "data.items");

        configurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals("data.items", configurator.getJsonRoot());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesValidJsonPaths() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatConfigurator.JsonFileFormatProperties.PROP_JSON_PATHS,
                "[\"$.name\", \"$.age\", \"$.city\"]");

        configurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals("[\"$.name\", \"$.age\", \"$.city\"]", configurator.getJsonPaths());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesStripOuterArrayTrue() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatConfigurator.JsonFileFormatProperties.PROP_STRIP_OUTER_ARRAY, "true");

        configurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals(true, configurator.isStripOuterArray());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesStripOuterArrayFalse() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatConfigurator.JsonFileFormatProperties.PROP_STRIP_OUTER_ARRAY, "false");

        configurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals(false, configurator.isStripOuterArray());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesReadJsonByLineTrue() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatConfigurator.JsonFileFormatProperties.PROP_READ_JSON_BY_LINE, "true");

        configurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals(true, configurator.isReadJsonByLine());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesReadJsonByLineFalse() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatConfigurator.JsonFileFormatProperties.PROP_READ_JSON_BY_LINE, "false");

        configurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals(false, configurator.isReadJsonByLine());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesNumAsStringTrue() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatConfigurator.JsonFileFormatProperties.PROP_NUM_AS_STRING, "true");

        configurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals(true, configurator.isNumAsString());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesNumAsStringFalse() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatConfigurator.JsonFileFormatProperties.PROP_NUM_AS_STRING, "false");

        configurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals(false, configurator.isNumAsString());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesFuzzyParseTrue() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatConfigurator.JsonFileFormatProperties.PROP_FUZZY_PARSE, "true");

        configurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals(true, configurator.isFuzzyParse());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesFuzzyParseFalse() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatConfigurator.JsonFileFormatProperties.PROP_FUZZY_PARSE, "false");

        configurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals(false, configurator.isFuzzyParse());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesInvalidBooleanValue() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatConfigurator.JsonFileFormatProperties.PROP_FUZZY_PARSE, "invalid");

        configurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals(false, configurator.isFuzzyParse());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesAllProperties() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatConfigurator.JsonFileFormatProperties.PROP_JSON_ROOT, "data.records");
        properties.put(JsonFileFormatConfigurator.JsonFileFormatProperties.PROP_JSON_PATHS, "[\"$.id\", \"$.name\"]");
        properties.put(JsonFileFormatConfigurator.JsonFileFormatProperties.PROP_STRIP_OUTER_ARRAY, "true");
        properties.put(JsonFileFormatConfigurator.JsonFileFormatProperties.PROP_READ_JSON_BY_LINE, "true");
        properties.put(JsonFileFormatConfigurator.JsonFileFormatProperties.PROP_NUM_AS_STRING, "true");
        properties.put(JsonFileFormatConfigurator.JsonFileFormatProperties.PROP_FUZZY_PARSE, "true");

        configurator.analyzeFileFormatProperties(properties);

        Assert.assertEquals("data.records", configurator.getJsonRoot());
        Assert.assertEquals("[\"$.id\", \"$.name\"]", configurator.getJsonPaths());
        Assert.assertEquals(true, configurator.isStripOuterArray());
        Assert.assertEquals(true, configurator.isReadJsonByLine());
        Assert.assertEquals(true, configurator.isNumAsString());
        Assert.assertEquals(true, configurator.isFuzzyParse());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesSpecialCharactersInJsonRoot() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatConfigurator.JsonFileFormatProperties.PROP_JSON_ROOT, "data.special@#$%^&*()");

        configurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals("data.special@#$%^&*()", configurator.getJsonRoot());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesComplexJsonPaths() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatConfigurator.JsonFileFormatProperties.PROP_JSON_PATHS,
                "[\"$.deeply.nested[0].array[*].field\", \"$.complex.path[?(@.type=='value')]\"]");

        configurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals("[\"$.deeply.nested[0].array[*].field\", \"$.complex.path[?(@.type=='value')]\"]",
                configurator.getJsonPaths());
    }

    @Test
    public void testAnalyzeFileFormatPropertiesEmptyJsonPaths() throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JsonFileFormatConfigurator.JsonFileFormatProperties.PROP_JSON_PATHS, "");

        configurator.analyzeFileFormatProperties(properties);
        Assert.assertEquals("", configurator.getJsonPaths());
    }
}
