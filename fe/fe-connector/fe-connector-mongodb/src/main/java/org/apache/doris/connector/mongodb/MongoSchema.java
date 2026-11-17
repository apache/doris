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

package org.apache.doris.connector.mongodb;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;

import org.bson.Document;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** A statement-pinned mapping. Validator constraints describe writes, never historical type guarantees. */
final class MongoSchema {
    static final int MAX_DEPTH = 64;
    static final int MAX_FIELDS = 4096;
    private static final Map<String, Integer> BSON_TYPES = Map.ofEntries(
            Map.entry("double", 1), Map.entry("string", 2), Map.entry("object", 3), Map.entry("array", 4),
            Map.entry("binData", 5), Map.entry("undefined", 6), Map.entry("objectId", 7), Map.entry("bool", 8),
            Map.entry("date", 9), Map.entry("null", 10), Map.entry("regex", 11), Map.entry("dbPointer", 12),
            Map.entry("javascript", 13), Map.entry("symbol", 14), Map.entry("javascriptWithScope", 15),
            Map.entry("int", 16), Map.entry("timestamp", 17), Map.entry("long", 18), Map.entry("decimal", 19),
            Map.entry("minKey", 255), Map.entry("maxKey", 127));

    static final class Binding {
        private final String name;
        private final List<String> path;
        private final ConnectorType type;
        private final List<Integer> bsonTypes;
        private final List<Binding> children;
        private final int subtype;
        private final String special;

        Binding(String name, List<String> path, ConnectorType type, List<Integer> bsonTypes,
                List<Binding> children, int subtype, String special) {
            this.name = name.toLowerCase(Locale.ROOT);
            this.path = List.copyOf(path);
            this.type = type;
            this.bsonTypes = List.copyOf(bsonTypes);
            this.children = List.copyOf(children);
            this.subtype = subtype;
            this.special = special;
        }

        String getName() {
            return name;
        }

        List<String> getPath() {
            return path;
        }

        Document toDocument() {
            return new Document("name", name).append("path", path).append("bson_types", bsonTypes)
                    .append("children", children.stream().map(Binding::toDocument).collect(Collectors.toList()))
                    .append("subtype", subtype).append("special", special)
                    .append("doris_type", type.getTypeName()).append("precision", type.getPrecision())
                    .append("scale", type.getScale());
        }

        ConnectorColumn column() {
            // Validator requirements do not establish a key, and views/time-series may have no unique _id.
            return new ConnectorColumn(name, type, "MongoDB BSON types=" + bsonTypes
                    + (subtype < 0 ? "" : ", subtype=" + subtype), true, null, false);
        }
    }

    private final List<Binding> bindings;
    private final String source;
    private final String version;
    private final String fallbackReason;

    private MongoSchema(List<Binding> bindings, String source, String identity, String fallbackReason) {
        this(bindings, source, identity, fallbackReason, false);
    }

    private MongoSchema(List<Binding> bindings, String source, String identity, String fallbackReason,
            boolean pinnedVersion) {
        this.bindings = List.copyOf(bindings);
        this.source = source;
        this.version = pinnedVersion ? identity : digest(identity + source + toBindingsJson());
        this.fallbackReason = fallbackReason;
    }

    String toPinnedJson() {
        return new Document("source", source).append("schema_version", version).append("reason", fallbackReason)
                .append("bindings", Document.parse(toBindingsJson())).toJson();
    }

    static MongoSchema fromPinnedJson(String json) {
        Document d = Document.parse(json);
        List<Binding> bindings = new ArrayList<>();
        for (Document value : d.get("bindings", Document.class).getList("columns", Document.class)) {
            bindings.add(pinnedBinding(value));
        }
        return new MongoSchema(bindings, d.getString("source"), d.getString("schema_version"),
                d.getString("reason"), true);
    }

    private static Binding pinnedBinding(Document value) {
        List<Binding> children = new ArrayList<>();
        for (Document child : value.getList("children", Document.class)) {
            children.add(pinnedBinding(child));
        }
        String typeName = value.getString("doris_type");
        ConnectorType type = new ConnectorType(typeName, value.getInteger("precision"), value.getInteger("scale"),
                children.stream().map(b -> b.type).collect(Collectors.toList()),
                "STRUCT".equals(typeName)
                        ? children.stream().map(b -> b.name).collect(Collectors.toList()) : List.of());
        return new Binding(value.getString("name"), value.getList("path", String.class), type,
                value.getList("bson_types", Integer.class), children, value.getInteger("subtype"),
                value.getString("special"));
    }

    static MongoSchema resolve(String namespace, Document metadata, MongoCatalogProperties properties) {
        String mode = properties.getSchemaMode();
        Document explicit = Document.parse(properties.getSchema()).get(namespace, Document.class);
        String identity = namespace + metadata.toJson() + properties.getSchema();
        if ("explicit".equals(mode) || ("auto".equals(mode) && explicit != null)) {
            if (explicit == null) {
                throw new IllegalArgumentException("No explicit MongoDB schema for the requested namespace");
            }
            List<Document> definitions = explicit.getList("columns", Document.class);
            if (definitions == null || definitions.isEmpty() || definitions.size() > MAX_FIELDS) {
                throw new IllegalArgumentException("MongoDB explicit schema requires bounded nonempty columns");
            }
            List<Binding> columns = new ArrayList<>();
            int[] count = {0};
            for (Document definition : definitions) {
                columns.add(explicitBinding(definition, 0, count));
            }
            unique(columns);
            return new MongoSchema(columns, "DORIS_EXPLICIT", identity, "");
        }
        if ("document".equals(mode)) {
            return document(identity, "");
        }
        Document options = metadata.get("options", new Document());
        Document validator = options.get("validator", new Document());
        Document jsonSchema = validator.get("$jsonSchema", Document.class);
        if (!"collection".equals(metadata.getString("type")) || jsonSchema == null || validator.size() != 1) {
            if ("validator".equals(mode)) {
                throw new IllegalArgumentException(
                        "MongoDB validator mode requires an ordinary collection JSON schema");
            }
            return document(identity, "No applicable JSON schema validator");
        }
        try {
            List<Binding> fields = objectFields(jsonSchema, 0, new int[] {0});
            if (fields.isEmpty()) {
                throw new IllegalArgumentException("Validator has no declared fields");
            }
            return new MongoSchema(fields, "MONGODB_VALIDATOR", identity, "");
        } catch (IllegalArgumentException e) {
            if ("validator".equals(mode)) {
                throw e;
            }
            // A partial mapping hides unsupported fields. Auto falls back for the entire collection.
            return document(identity, "Validator cannot be mapped completely");
        }
    }

    private static MongoSchema document(String identity, String reason) {
        return new MongoSchema(List.of(
                new Binding("_mongo_ejson", List.of(), ConnectorType.of("JSONB"), List.of(),
                        List.of(), -1, "ejson"),
                new Binding("_mongo_bson", List.of(), ConnectorType.of("VARBINARY"), List.of(),
                        List.of(), -1, "bson")), "DOCUMENT", identity, reason);
    }

    private static void requireRepresentableValidator(Document schema) {
        // For example, properties:{known:...} plus patternProperties:{"^extra$":...} must
        // not publish only known. Apply this check recursively before accepting any typed fields.
        Document patterns = schema.get("patternProperties", Document.class);
        Document dependencies = schema.get("dependencies", Document.class);
        if (schema.containsKey("anyOf") || schema.containsKey("oneOf") || schema.containsKey("allOf")
                || schema.containsKey("not") || (patterns != null && !patterns.isEmpty())
                || schema.get("additionalProperties") instanceof Document
                || (dependencies != null && dependencies.values().stream().anyMatch(v -> v instanceof Document))) {
            throw new IllegalArgumentException("MongoDB schema constructs require an explicit mapping");
        }
    }

    private static List<Binding> objectFields(Document schema, int depth, int[] count) {
        requireRepresentableValidator(schema);
        Document properties = schema.get("properties", Document.class);
        if (properties == null) {
            return List.of();
        }
        List<Binding> fields = new ArrayList<>();
        for (Map.Entry<String, Object> field : properties.entrySet()) {
            if (!(field.getValue() instanceof Document)) {
                throw new IllegalArgumentException("Invalid MongoDB field schema");
            }
            fields.add(validatorBinding(field.getKey(), (Document) field.getValue(), depth + 1, count));
        }
        unique(fields);
        return fields;
    }

    private static Binding validatorBinding(String name, Document definition, int depth, int[] count) {
        bounded(depth, count);
        requireRepresentableValidator(definition);
        Object declared = definition.get("bsonType");
        List<String> names = new ArrayList<>();
        if (declared instanceof String) {
            names.add((String) declared);
        } else if (declared instanceof List<?>) {
            for (Object value : (List<?>) declared) {
                if (!(value instanceof String)) {
                    throw new IllegalArgumentException("Invalid BSON type union");
                }
                names.add((String) value);
            }
        } else {
            throw new IllegalArgumentException("MongoDB field requires bsonType");
        }
        List<Integer> tags = tags(names);
        Set<String> nonNull = new HashSet<>(names);
        nonNull.remove("null");
        List<Binding> children = List.of();
        ConnectorType type;
        if (nonNull.equals(Set.of("int", "long"))) {
            type = ConnectorType.of("BIGINT");
        } else if (nonNull.size() > 1) {
            throw new IllegalArgumentException("Mixed BSON types require an explicit or document schema");
        } else {
            String nameOfType = nonNull.isEmpty() ? "null" : nonNull.iterator().next();
            switch (nameOfType) {
                case "int":
                    type = ConnectorType.of("INT");
                    break;
                case "long":
                    type = ConnectorType.of("BIGINT");
                    break;
                case "double":
                    type = ConnectorType.of("DOUBLE");
                    break;
                case "bool":
                    type = ConnectorType.of("BOOLEAN");
                    break;
                case "string":
                    type = ConnectorType.of("STRING");
                    break;
                case "objectId":
                    type = ConnectorType.of("VARBINARY", 12, -1);
                    break;
                case "date":
                    type = ConnectorType.of("DATETIMEV2", 3, 0);
                    break;
                case "object":
                    children = objectFields(definition, depth, count);
                    type = children.isEmpty() ? ConnectorType.of("JSONB") : structType(children);
                    break;
                case "array":
                    // MongoDB also permits tuple items (an array of schemas). Report that grammar
                    // as unsupported so auto takes the same whole-document fallback as other unions.
                    Object item = definition.get("items");
                    if (!(item instanceof Document)) {
                        throw new IllegalArgumentException("MongoDB array requires homogeneous items");
                    }
                    Binding element = validatorBinding("element", (Document) item, depth + 1, count);
                    children = List.of(element);
                    type = ConnectorType.arrayOf(element.type);
                    break;
                case "timestamp":
                    children = List.of(longBinding("seconds"), longBinding("increment"));
                    type = structType(children);
                    break;
                default:
                    // Decimal128 has no collection-wide scale; JSON preserves its exponent and special values.
                    type = ConnectorType.of("JSONB");
            }
        }
        return new Binding(name, List.of(name), type, tags, children, -1, "");
    }

    private static Binding explicitBinding(Document definition, int depth, int[] count) {
        bounded(depth, count);
        String name = definition.getString("name");
        List<String> path = definition.getList("path", String.class);
        if (name == null || name.isEmpty() || path == null || path.isEmpty()
                || path.size() > MAX_DEPTH || path.stream().anyMatch(p -> p == null || p.isEmpty())) {
            throw new IllegalArgumentException("MongoDB explicit column requires name and path segments");
        }
        ConnectorType type = parseDorisType(definition.getString("doris_type"));
        List<String> sources = definition.getList("bson_types", String.class);
        List<Integer> sourceTags = sources == null ? List.of() : tags(sources);
        int subtype = definition.getInteger("subtype", -1);
        if (subtype < -1 || subtype > 255) {
            throw new IllegalArgumentException("Invalid BSON binary subtype");
        }
        List<Binding> children = defaultChildren(type, depth, count);
        if (definition.containsKey("children")) {
            children = new ArrayList<>();
            for (Document child : definition.getList("children", Document.class)) {
                children.add(explicitBinding(child, depth + 1, count));
            }
            if (children.size() != type.getChildren().size()) {
                throw new IllegalArgumentException("Explicit child mapping must match the Doris type");
            }
            for (int i = 0; i < children.size(); i++) {
                if (!children.get(i).type.equals(type.getChildren().get(i))
                        || ("STRUCT".equals(type.getTypeName())
                            && !children.get(i).name.equals(type.getFieldNames().get(i)))) {
                    throw new IllegalArgumentException("Explicit child mapping type mismatch");
                }
            }
        }
        return new Binding(name, path, type, sourceTags, children, subtype, "");
    }

    private static List<Binding> defaultChildren(ConnectorType type, int depth, int[] count) {
        List<Binding> children = new ArrayList<>();
        for (int i = 0; i < type.getChildren().size(); i++) {
            bounded(depth + 1, count);
            String name = type.getFieldNames().isEmpty() ? "element" : type.getFieldNames().get(i);
            ConnectorType child = type.getChildren().get(i);
            children.add(new Binding(name, List.of(name), child, List.of(),
                    defaultChildren(child, depth + 1, count), -1, ""));
        }
        return children;
    }

    private static Binding longBinding(String name) {
        return new Binding(name, List.of(name), ConnectorType.of("BIGINT"), List.of(18), List.of(), -1, "");
    }

    private static ConnectorType structType(List<Binding> children) {
        return ConnectorType.structOf(children.stream().map(b -> b.name).collect(Collectors.toList()),
                children.stream().map(b -> b.type).collect(Collectors.toList()));
    }

    private static List<Integer> tags(List<String> names) {
        List<Integer> result = new ArrayList<>();
        for (String name : names) {
            Integer tag = BSON_TYPES.get(name);
            if (tag == null) {
                throw new IllegalArgumentException("Unsupported BSON schema type");
            }
            result.add(tag);
        }
        return result;
    }

    private static void bounded(int depth, int[] count) {
        if (depth > MAX_DEPTH || ++count[0] > MAX_FIELDS) {
            throw new IllegalArgumentException("MongoDB schema exceeds depth or field limits");
        }
    }

    private static void unique(List<Binding> bindings) {
        Set<String> names = new HashSet<>();
        for (Binding b : bindings) {
            if (!names.add(b.name)) {
                throw new IllegalArgumentException("MongoDB columns collide after case folding");
            }
        }
    }

    static ConnectorType parseDorisType(String text) {
        if (text == null || text.length() > 8192) {
            throw new IllegalArgumentException("Missing or oversized Doris type");
        }
        TypeParser parser = new TypeParser(text);
        ConnectorType type = parser.parse(0);
        parser.space();
        if (parser.position != text.length()) {
            throw new IllegalArgumentException("Trailing input in Doris type");
        }
        return type;
    }

    private static final class TypeParser {
        private final String text;
        private int position;

        TypeParser(String text) {
            this.text = text;
        }

        void space() {
            while (position < text.length() && Character.isWhitespace(text.charAt(position))) {
                position++;
            }
        }

        String word() {
            space();
            int start = position;
            while (position < text.length() && (Character.isLetterOrDigit(text.charAt(position))
                    || text.charAt(position) == '_')) {
                position++;
            }
            if (start == position) {
                throw new IllegalArgumentException("Invalid Doris type syntax");
            }
            return text.substring(start, position);
        }

        boolean take(char token) {
            space();
            if (position < text.length() && text.charAt(position) == token) {
                position++;
                return true;
            }
            return false;
        }

        void expect(char token) {
            if (!take(token)) {
                throw new IllegalArgumentException("Invalid Doris type delimiter");
            }
        }

        ConnectorType parse(int depth) {
            if (depth > MAX_DEPTH) {
                throw new IllegalArgumentException("Doris type exceeds nesting limit");
            }
            String name = word().toUpperCase(Locale.ROOT);
            if ("ARRAY".equals(name)) {
                expect('<');
                ConnectorType child = parse(depth + 1);
                expect('>');
                return ConnectorType.arrayOf(child);
            }
            if ("STRUCT".equals(name)) {
                expect('<');
                List<String> names = new ArrayList<>();
                List<ConnectorType> types = new ArrayList<>();
                do {
                    String field = word().toLowerCase(Locale.ROOT);
                    if (names.contains(field)) {
                        throw new IllegalArgumentException("Duplicate STRUCT field");
                    }
                    names.add(field);
                    expect(':');
                    types.add(parse(depth + 1));
                } while (take(','));
                expect('>');
                return ConnectorType.structOf(names, types);
            }
            int precision = -1;
            int scale = -1;
            if (take('(')) {
                precision = Integer.parseInt(word());
                if (take(',')) {
                    scale = Integer.parseInt(word());
                }
                expect(')');
            }
            if ("DECIMAL".equals(name) || "DECIMALV3".equals(name)) {
                if (precision < 1 || precision > 38 || scale < 0 || scale > precision) {
                    throw new IllegalArgumentException("MongoDB decimal requires DECIMAL(p,s), p <= 38");
                }
                return ConnectorType.of("DECIMALV3", precision, scale);
            }
            if ("DATETIME".equals(name) || "DATETIMEV2".equals(name)) {
                if (precision < 0 || precision > 6 || scale != -1) {
                    throw new IllegalArgumentException("MongoDB date requires DATETIME(scale), scale <= 6");
                }
                return ConnectorType.of("DATETIMEV2", precision, 0);
            }
            if ("VARBINARY".equals(name)) {
                if (precision == 0 || precision > 16777216 || scale != -1) {
                    throw new IllegalArgumentException("Invalid VARBINARY length");
                }
                return ConnectorType.of(name, precision, -1);
            }
            if (precision != -1 || scale != -1 || !Set.of("BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT",
                    "LARGEINT", "FLOAT", "DOUBLE", "STRING", "JSON", "JSONB").contains(name)) {
                throw new IllegalArgumentException("Unsupported explicit MongoDB Doris type");
            }
            return ConnectorType.of("JSON".equals(name) ? "JSONB" : name);
        }
    }

    List<Binding> getBindings() {
        return bindings;
    }

    List<ConnectorColumn> getColumns() {
        return Collections.unmodifiableList(bindings.stream().map(Binding::column).collect(Collectors.toList()));
    }

    String getSource() {
        return source;
    }

    String getVersion() {
        return version;
    }

    String getFallbackReason() {
        return fallbackReason;
    }

    String toBindingsJson() {
        return new Document("version", 1).append("columns",
                bindings.stream().map(Binding::toDocument).collect(Collectors.toList())).toJson();
    }

    static String digest(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }
}
