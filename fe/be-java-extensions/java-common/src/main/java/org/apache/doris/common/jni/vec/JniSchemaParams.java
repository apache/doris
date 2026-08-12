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

import com.google.common.base.Preconditions;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;

/**
 * Reads the {@code required_fields} / {@code columns_types} scanner parameters BE hands every JNI
 * scanner, in either encoding BE can produce: the legacy delimited grammar, or the paired
 * Base64-encoded parameters that preserve a STRUCT field's exact spelling (BE only publishes the pair
 * when the reader opts in via {@code publishes_encoded_schema()}). Shared so every scanner that reads
 * the encoded form -- today paimon and fluss -- decodes it identically.
 */
public final class JniSchemaParams {

    private JniSchemaParams() {
    }

    /**
     * Whether the scanner params carry the Base64-encoded schema pair rather than the legacy one.
     *
     * @throws IllegalArgumentException when only one half of the pair is present. The two describe one
     *         schema version; accepting a mixed pair would reintroduce the cardinality and nested-name
     *         ambiguity this protocol is meant to remove.
     */
    public static boolean usesEncodedSchema(Map<String, String> params) {
        boolean hasFields = params.containsKey("required_fields_base64");
        boolean hasTypes = params.containsKey("columns_types_base64");
        Preconditions.checkArgument(hasFields == hasTypes,
                "required_fields_base64 and columns_types_base64 must be provided together");
        return hasFields;
    }

    public static String[] requiredFields(Map<String, String> params) {
        String encodedFields = params.get("required_fields_base64");
        if (encodedFields == null) {
            return splitParam(params.get("required_fields"), ",");
        }
        if (encodedFields.isEmpty()) {
            return new String[0];
        }
        // Each identifier is encoded independently, so delimiters in quoted identifiers cannot
        // change field cardinality. The legacy parameter remains the rolling-upgrade fallback.
        return decodeSchemaValues(encodedFields);
    }

    public static String[] requiredTypes(Map<String, String> params) {
        String encodedTypes = params.get("columns_types_base64");
        return encodedTypes == null
                ? splitParam(params.get("columns_types"), "#")
                : decodeSchemaValues(encodedTypes);
    }

    private static String[] decodeSchemaValues(String encodedValues) {
        if (encodedValues.isEmpty()) {
            return new String[0];
        }
        return Arrays.stream(encodedValues.split(",", -1))
                .map(encoded -> {
                    // A marker on every token preserves list arity when the encoded value itself is empty.
                    Preconditions.checkArgument(encoded.startsWith("$"),
                            "Encoded JNI schema token is missing its version marker");
                    return new String(Base64.getDecoder().decode(encoded.substring(1)), StandardCharsets.UTF_8);
                })
                .toArray(String[]::new);
    }

    private static String[] splitParam(String value, String delimiter) {
        if (value == null || value.isEmpty()) {
            return new String[0];
        }
        return value.split(delimiter);
    }
}
