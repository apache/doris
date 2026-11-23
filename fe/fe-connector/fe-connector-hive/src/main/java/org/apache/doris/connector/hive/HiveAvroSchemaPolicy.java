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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

import java.util.List;
import java.util.Set;

final class HiveAvroSchemaPolicy {
    private static final String LITERAL = "avro.schema.literal";
    private static final String URL = "avro.schema.url";
    private static final Set<String> PRIMITIVES =
            Set.of("null", "boolean", "int", "long", "float", "double", "bytes", "string");

    private HiveAvroSchemaPolicy() {
    }

    static void validate(HiveTableHandle handle, HiveFileFormat format) {
        if (format != HiveFileFormat.AVRO) {
            return;
        }
        if (handle.requiresAvroStringCoercion()) {
            throw new DorisConnectorException("Native Hive Avro CHAR/VARCHAR coercion is unsupported");
        }
        String literal = property(handle, LITERAL);
        if (literal == null || "none".equals(literal)) {
            String url = property(handle, URL);
            if (url != null && !"none".equals(url)) {
                throw unsupported();
            }
            return;
        }
        // Hive synthesizes nullable fields with explicit null defaults for ordinary column DDL.
        // Custom reader defaults/aliases require Avro schema resolution; returning NULL instead
        // of a declared default (for example 7) would silently change the table's result.
        if (literal.length() > 1024 * 1024 || !boundedJson(literal)) {
            throw unsupported();
        }
        try {
            JsonElement schema = JsonParser.parseString(literal);
            if (!schema.isJsonObject() || !"record".equals(typeName(schema))
                    || !supported(schema, 0)) {
                throw unsupported();
            }
        } catch (JsonParseException | IllegalStateException e) {
            throw unsupported();
        }
    }

    static boolean requiresStringCoercion(List<ConnectorColumn> columns) {
        return columns != null && columns.stream().anyMatch(column -> requiresStringCoercion(column.getType()));
    }

    private static boolean requiresStringCoercion(ConnectorType type) {
        // DataTypeString later erases length and Hive's Unicode truncation/padding behavior.
        return "CHAR".equalsIgnoreCase(type.getTypeName()) || "VARCHAR".equalsIgnoreCase(type.getTypeName())
                || type.getChildren().stream().anyMatch(HiveAvroSchemaPolicy::requiresStringCoercion);
    }

    static String readerSchema(HiveTableHandle handle) {
        String literal = property(handle, LITERAL);
        return literal == null || "none".equals(literal) ? null : literal;
    }

    private static String property(HiveTableHandle handle, String key) {
        String value = handle.getTableParameters().get(key);
        return value != null ? value : handle.getSdParameters().get(key);
    }

    private static DorisConnectorException unsupported() {
        return new DorisConnectorException("Native Hive Avro reads support only generated-style nullable reader "
                + "schemas with null defaults; custom Avro reader defaults, aliases, named references and "
                + "schema URLs and Hive CHAR/VARCHAR logical types are unsupported");
    }

    private static String typeName(JsonElement schema) {
        if (schema == null) {
            return "";
        }
        JsonElement type = schema.isJsonObject() ? schema.getAsJsonObject().get("type") : schema;
        return type != null && type.isJsonPrimitive() && type.getAsJsonPrimitive().isString()
                ? type.getAsString() : "";
    }

    private static boolean nullable(JsonElement schema) {
        if ("null".equals(typeName(schema))) {
            return true;
        }
        if (schema != null && schema.isJsonArray()) {
            for (JsonElement branch : schema.getAsJsonArray()) {
                if ("null".equals(typeName(branch))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean aliases(JsonObject object) {
        JsonElement value = object.get("aliases");
        return value != null && (!value.isJsonArray() || value.getAsJsonArray().size() != 0);
    }

    private static boolean supported(JsonElement schema, int depth) {
        if (schema == null || depth > 64) {
            return false;
        }
        if (schema.isJsonArray()) {
            int nulls = 0;
            for (JsonElement branch : schema.getAsJsonArray()) {
                if (!supported(branch, depth + 1)) {
                    return false;
                }
                nulls += "null".equals(typeName(branch)) ? 1 : 0;
            }
            return nulls == 1 && schema.getAsJsonArray().size() <= 2;
        }
        String type = typeName(schema);
        if (!schema.isJsonObject()) {
            return PRIMITIVES.contains(type);
        }
        JsonObject object = schema.getAsJsonObject();
        JsonElement logical = object.get("logicalType");
        if (logical != null && logical.isJsonPrimitive()) {
            String name = logical.getAsString();
            // Hive recognizes these annotations without case, while the native SDK does not.
            if (name.chars().anyMatch(value -> value >= 128)
                    || "char".equalsIgnoreCase(name) || "varchar".equalsIgnoreCase(name)
                    || ("decimal".equalsIgnoreCase(name) && !"decimal".equals(name))) {
                return false;
            }
        }
        if (aliases(object)) {
            return false;
        }
        if ("record".equals(type)) {
            JsonElement fields = object.get("fields");
            if (fields == null || !fields.isJsonArray()) {
                return false;
            }
            for (JsonElement entry : fields.getAsJsonArray()) {
                if (!entry.isJsonObject()) {
                    return false;
                }
                JsonObject field = entry.getAsJsonObject();
                if (aliases(field) || !field.has("default") || !field.get("default").isJsonNull()
                        || !nullable(field.get("type")) || !supported(field.get("type"), depth + 1)) {
                    return false;
                }
            }
            return true;
        }
        if ("array".equals(type)) {
            return supported(object.get("items"), depth + 1);
        }
        if ("map".equals(type)) {
            return supported(object.get("values"), depth + 1);
        }
        return PRIMITIVES.contains(type);
    }

    private static boolean boundedJson(String text) {
        int depth = 0;
        boolean quoted = false;
        boolean escaped = false;
        for (int i = 0; i < text.length(); i++) {
            char value = text.charAt(i);
            if (quoted) {
                if (escaped) {
                    escaped = false;
                } else if (value == '\\') {
                    escaped = true;
                } else if (value == '"') {
                    quoted = false;
                }
            } else if (value == '"') {
                quoted = true;
            } else if ((value == '{' || value == '[') && ++depth > 128) {
                return false;
            } else if ((value == '}' || value == ']') && --depth < 0) {
                return false;
            }
        }
        return depth == 0 && !quoted;
    }
}
