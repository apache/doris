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

package org.apache.doris.common.jni.vec;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class JniSchemaParamsTest {

    private static String encodeToken(String value) {
        return "$" + Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void legacyParamsAreSplitOnTheirDelimitersWhenNoEncodedPairIsPresent() {
        Map<String, String> params = new HashMap<>();
        params.put("required_fields", "id,name");
        params.put("columns_types", "int#string");

        Assert.assertFalse(JniSchemaParams.usesEncodedSchema(params));
        Assert.assertArrayEquals(new String[] {"id", "name"}, JniSchemaParams.requiredFields(params));
        Assert.assertArrayEquals(new String[] {"int", "string"}, JniSchemaParams.requiredTypes(params));
    }

    @Test
    public void encodedPairRoundTripsDelimiterSafeNames() {
        // A quoted identifier can legitimately contain the delimiters ("," between fields, "#"
        // between types); the encoded pair is what lets such a name survive the trip intact.
        Map<String, String> params = new HashMap<>();
        params.put("required_fields", "placeholder");
        params.put("required_fields_base64", encodeToken("region,code") + "," + encodeToken("hash#name"));
        params.put("columns_types", "placeholder");
        params.put("columns_types_base64", encodeToken("string") + "," + encodeToken("string"));

        Assert.assertTrue(JniSchemaParams.usesEncodedSchema(params));
        Assert.assertArrayEquals(new String[] {"region,code", "hash#name"},
                JniSchemaParams.requiredFields(params));
        Assert.assertArrayEquals(new String[] {"string", "string"}, JniSchemaParams.requiredTypes(params));
    }

    @Test
    public void mismatchedEncodedPairPresenceFailsLoud() {
        Map<String, String> onlyFields = new HashMap<>();
        onlyFields.put("required_fields_base64", encodeToken("id"));

        try {
            JniSchemaParams.usesEncodedSchema(onlyFields);
            Assert.fail("required_fields_base64 without columns_types_base64 must not be accepted");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("required_fields_base64"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("columns_types_base64"));
        }

        Map<String, String> onlyTypes = new HashMap<>();
        onlyTypes.put("columns_types_base64", encodeToken("int"));

        try {
            JniSchemaParams.usesEncodedSchema(onlyTypes);
            Assert.fail("columns_types_base64 without required_fields_base64 must not be accepted");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("required_fields_base64"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("columns_types_base64"));
        }
    }

    @Test
    public void tokenWithoutTheVersionMarkerFailsLoud() {
        // Every token carries the marker so an empty encoded value still occupies a slot. Decoding a
        // marker-less token would mean BE and this parser disagree on the protocol version, and the
        // Base64 decoder would happily turn most of them into garbage names instead of failing.
        Map<String, String> params = new HashMap<>();
        params.put("required_fields_base64",
                Base64.getEncoder().encodeToString("id".getBytes(StandardCharsets.UTF_8)));
        params.put("columns_types_base64", encodeToken("int"));

        try {
            JniSchemaParams.requiredFields(params);
            Assert.fail("a token without its version marker must not be decoded");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("version marker"));
        }
    }

    @Test
    public void emptyEncodedListsProduceNoFields() {
        Map<String, String> params = new HashMap<>();
        params.put("required_fields_base64", "");
        params.put("columns_types_base64", "");

        Assert.assertTrue(JniSchemaParams.usesEncodedSchema(params));
        Assert.assertEquals(0, JniSchemaParams.requiredFields(params).length);
        Assert.assertEquals(0, JniSchemaParams.requiredTypes(params).length);
    }

    @Test
    public void singleEmptyFieldNameIsDistinctFromAnEmptyList() {
        // "$" alone decodes to one zero-length identifier -- a projection of one column whose name
        // happens to be empty -- which must not collapse into the empty-list case above.
        Map<String, String> params = new HashMap<>();
        params.put("required_fields_base64", "$");
        params.put("columns_types_base64", encodeToken("string"));

        Assert.assertArrayEquals(new String[] {""}, JniSchemaParams.requiredFields(params));
        Assert.assertArrayEquals(new String[] {"string"}, JniSchemaParams.requiredTypes(params));
    }
}
