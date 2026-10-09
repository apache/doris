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

package org.apache.doris.jni.toolkit.vec;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

        Assertions.assertFalse(JniSchemaParams.usesEncodedSchema(params));
        Assertions.assertArrayEquals(new String[] {"id", "name"}, JniSchemaParams.requiredFields(params));
        Assertions.assertArrayEquals(new String[] {"int", "string"}, JniSchemaParams.requiredTypes(params));
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

        Assertions.assertTrue(JniSchemaParams.usesEncodedSchema(params));
        Assertions.assertArrayEquals(new String[] {"region,code", "hash#name"},
                JniSchemaParams.requiredFields(params));
        Assertions.assertArrayEquals(new String[] {"string", "string"}, JniSchemaParams.requiredTypes(params));
    }

    @Test
    public void mismatchedEncodedPairPresenceFailsLoud() {
        Map<String, String> onlyFields = new HashMap<>();
        onlyFields.put("required_fields_base64", encodeToken("id"));

        try {
            JniSchemaParams.usesEncodedSchema(onlyFields);
            Assertions.fail("required_fields_base64 without columns_types_base64 must not be accepted");
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().contains("required_fields_base64"), e.getMessage());
            Assertions.assertTrue(e.getMessage().contains("columns_types_base64"), e.getMessage());
        }

        Map<String, String> onlyTypes = new HashMap<>();
        onlyTypes.put("columns_types_base64", encodeToken("int"));

        try {
            JniSchemaParams.usesEncodedSchema(onlyTypes);
            Assertions.fail("columns_types_base64 without required_fields_base64 must not be accepted");
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().contains("required_fields_base64"), e.getMessage());
            Assertions.assertTrue(e.getMessage().contains("columns_types_base64"), e.getMessage());
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
            Assertions.fail("a token without its version marker must not be decoded");
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().contains("version marker"), e.getMessage());
        }
    }

    @Test
    public void emptyEncodedListsProduceNoFields() {
        Map<String, String> params = new HashMap<>();
        params.put("required_fields_base64", "");
        params.put("columns_types_base64", "");

        Assertions.assertTrue(JniSchemaParams.usesEncodedSchema(params));
        Assertions.assertEquals(0, JniSchemaParams.requiredFields(params).length);
        Assertions.assertEquals(0, JniSchemaParams.requiredTypes(params).length);
    }

    @Test
    public void singleEmptyFieldNameIsDistinctFromAnEmptyList() {
        // "$" alone decodes to one zero-length identifier -- a projection of one column whose name
        // happens to be empty -- which must not collapse into the empty-list case above.
        Map<String, String> params = new HashMap<>();
        params.put("required_fields_base64", "$");
        params.put("columns_types_base64", encodeToken("string"));

        Assertions.assertArrayEquals(new String[] {""}, JniSchemaParams.requiredFields(params));
        Assertions.assertArrayEquals(new String[] {"string"}, JniSchemaParams.requiredTypes(params));
    }
}
