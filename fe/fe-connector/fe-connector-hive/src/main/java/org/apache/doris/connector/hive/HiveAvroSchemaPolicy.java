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

import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Objects;
import java.util.Set;

final class HiveAvroSchemaPolicy {
    private static final String LITERAL = "avro.schema.literal";
    private static final String URL = "avro.schema.url";
    private static final int MAX_SCHEMA_LENGTH = 1024 * 1024;
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
            // Hive falls back to column metadata only for an absent URL; URL "none" is an error.
            if (url != null) {
                throw unsupported();
            }
            return;
        }
        // Hive synthesizes nullable fields with explicit null defaults for ordinary column DDL.
        // Custom reader defaults/aliases require Avro schema resolution; returning NULL instead
        // of a declared default (for example 7) would silently change the table's result.
        if (literal.length() > MAX_SCHEMA_LENGTH) {
            throw unsupported();
        }
        try {
            JsonNode schema = parseSchemaJson(literal);
            if (!schema.isObject() || !"record".equals(typeName(schema))
                    || !supported(schema, 0)) {
                throw unsupported();
            }
        } catch (IOException e) {
            throw unsupported();
        }
    }

    static void validatePartition(HiveTableHandle handle, HmsPartitionInfo partition, HiveFileFormat format) {
        if (format != HiveFileFormat.AVRO) {
            return;
        }
        String literal = partitionProperty(partition, LITERAL);
        String url = partitionProperty(partition, URL);
        // Hive opens partition files using partition-local reader properties before adapting
        // records to the table schema. A table literal cannot hide a partition URL. Even URL
        // "none" is a non-null URL setting in this path, rather than the literal's sentinel.
        if ((literal == null || "none".equals(literal)) && url != null) {
            throw unsupportedPartition();
        }
        literal = literal != null ? literal : property(handle, LITERAL);
        String tableLiteral = readerSchema(handle);
        literal = "none".equals(literal) ? null : literal;
        // BE receives one reader schema for the whole scan. A different partition schema could
        // supply a default (e.g. 7) where the table schema would silently materialize NULL.
        if (!sameReaderSchema(literal, tableLiteral)) {
            throw unsupportedPartition();
        }
        url = url != null ? url : property(handle, URL);
        // Hive ignores schema URLs when an effective literal is present.
        if (literal == null && url != null) {
            throw unsupportedPartition();
        }
    }

    private static boolean sameReaderSchema(String literal, String tableLiteral) {
        if (Objects.equals(literal, tableLiteral)) {
            return true;
        }
        if (literal == null || tableLiteral == null
                || literal.length() > MAX_SCHEMA_LENGTH || tableLiteral.length() > MAX_SCHEMA_LENGTH) {
            return false;
        }
        try {
            // Object key order and whitespace are irrelevant, but Avro canonical form would
            // discard defaults and logical annotations needed by the single-reader invariant.
            return parseSchemaJson(literal).equals(parseSchemaJson(tableLiteral));
        } catch (IOException e) {
            return false;
        }
    }

    private static final class SchemaJson {
        // Do not retain untrusted schema property names in the shared factory across scans.
        private static final JsonFactory FACTORY = JsonFactory.builder()
                .disable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES)
                .disable(JsonFactory.Feature.INTERN_FIELD_NAMES)
                .enable(JsonReadFeature.ALLOW_JAVA_COMMENTS)
                .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(128)
                        .maxStringLength(MAX_SCHEMA_LENGTH).maxNameLength(MAX_SCHEMA_LENGTH)
                        .maxNumberLength(MAX_SCHEMA_LENGTH).build())
                .build();
        private static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);
    }

    private static JsonNode parseSchemaJson(String literal) throws IOException {
        // Hive parses reader literals through Jackson's Reader API. Gson also admits BOMs and
        // mixed-case keywords that Hive rejects, so use the same JSON syntax for both checks.
        try (JsonParser parser = SchemaJson.FACTORY.createParser(new StringReader(literal))) {
            while (parser.nextToken() != null) {
                if (parser.currentToken() == JsonToken.VALUE_NUMBER_INT) {
                    int length = parser.getTextLength();
                    // Native RapidJSON cannot admit integers beyond finite double range. Bound
                    // digit count before readTree can materialize an expensive BigInteger, while
                    // allowing long finite fractions within the existing schema text budget.
                    if (length > 309 && (length > 310 || parser.getText().charAt(0) != '-')) {
                        throw new IOException("Avro schema integer exceeds native numeric range");
                    }
                }
            }
        }
        try (JsonParser parser = SchemaJson.FACTORY.createParser(new StringReader(literal))) {
            JsonNode schema = SchemaJson.MAPPER.readTree(parser);
            if (schema == null || schema.isMissingNode() || parser.nextToken() != null) {
                throw new IOException("Expected one Avro schema JSON value");
            }
            return schema;
        }
    }

    private static String partitionProperty(HmsPartitionInfo partition, String key) {
        // Hive overlays partition SerDe properties and then HMS partition parameters. A literal
        // stored in the latter can replace a SerDe schema with a default of 7.
        String value = partition.getParameters().get(key);
        if (value == null) {
            value = partition.getSdParameters().get(key);
        }
        return value;
    }

    private static DorisConnectorException unsupportedPartition() {
        return new DorisConnectorException("Native Hive Avro partition reader schema overrides are unsupported");
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

    private static String typeName(JsonNode schema) {
        if (schema == null) {
            return "";
        }
        JsonNode type = schema.isObject() ? schema.get("type") : schema;
        return type != null && type.isTextual()
                ? type.asText() : "";
    }

    private static boolean acceptsNullDefault(JsonNode schema) {
        if ("null".equals(typeName(schema))) {
            return true;
        }
        // Hive's Avro reader resolves a missing field's default against union branch zero.
        return schema != null && schema.isArray() && schema.size() != 0
                && "null".equals(typeName(schema.get(0)));
    }

    private static boolean aliases(JsonNode object) {
        JsonNode value = object.get("aliases");
        return value != null && (!value.isArray() || value.size() != 0);
    }

    private static boolean supported(JsonNode schema, int depth) {
        if (schema == null || depth > 64) {
            return false;
        }
        if (schema.isArray()) {
            int nulls = 0;
            for (JsonNode branch : schema) {
                if (!supported(branch, depth + 1)) {
                    return false;
                }
                nulls += "null".equals(typeName(branch)) ? 1 : 0;
            }
            return nulls == 1 && schema.size() <= 2;
        }
        String type = typeName(schema);
        if (!schema.isObject()) {
            return PRIMITIVES.contains(type);
        }
        JsonNode object = schema;
        JsonNode logical = object.get("logicalType");
        if (logical != null && logical.isValueNode() && !logical.isNull()) {
            String name = logical.asText();
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
            JsonNode fields = object.get("fields");
            if (fields == null || !fields.isArray()) {
                return false;
            }
            for (JsonNode entry : fields) {
                if (!entry.isObject()) {
                    return false;
                }
                JsonNode field = entry;
                if (aliases(field) || !field.has("default") || !field.get("default").isNull()
                        || !acceptsNullDefault(field.get("type")) || !supported(field.get("type"), depth + 1)) {
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
}
