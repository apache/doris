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

package org.apache.doris.cdcclient.utils;

import org.apache.doris.cdcclient.common.DorisType;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for {@link SchemaChangeHelper#pgTypeNameToDorisType}. */
class SchemaChangeHelperTest {

    // ─── Integer types ────────────────────────────────────────────────────────

    @Test
    void integerTypes() {
        assertEquals(DorisType.SMALLINT, map("int2", -1, -1));
        assertEquals(DorisType.SMALLINT, map("smallserial", -1, -1));
        assertEquals(DorisType.INT,      map("int4", -1, -1));
        assertEquals(DorisType.INT,      map("serial", -1, -1));
        assertEquals(DorisType.BIGINT,   map("int8", -1, -1));
        assertEquals(DorisType.BIGINT,   map("bigserial", -1, -1));
    }

    @Test
    void floatTypes() {
        assertEquals(DorisType.FLOAT,  map("float4", -1, -1));
        assertEquals(DorisType.DOUBLE, map("float8", -1, -1));
    }

    // ─── Boolean / bit ───────────────────────────────────────────────────────

    @Test
    void boolType() {
        assertEquals(DorisType.BOOLEAN, map("bool", -1, -1));
    }

    @Test
    void bitType_singleBit_isBoolean() {
        assertEquals(DorisType.BOOLEAN, map("bit", 1, -1));
    }

    @Test
    void bitType_multiBit_isString() {
        assertEquals(DorisType.STRING, map("bit", 8, -1));
        assertEquals(DorisType.STRING, map("bit", 64, -1));
    }

    // ─── Numeric / decimal ───────────────────────────────────────────────────

    @Test
    void numericType_defaultPrecisionScale() {
        // length <= 0, scale < 0 → DECIMAL(38, 9)
        assertEquals("DECIMAL(38, 9)", map("numeric", 0, -1));
        assertEquals("DECIMAL(38, 9)", map("numeric", -1, -1));
    }

    @Test
    void numericType_explicitPrecisionScale() {
        assertEquals("DECIMAL(10, 2)", map("numeric", 10, 2));
        assertEquals("DECIMAL(5, 0)",  map("numeric", 5, 0));
    }

    @Test
    void numericType_precisionCappedAt38() {
        assertEquals("DECIMAL(38, 4)", map("numeric", 50, 4));
        assertEquals("DECIMAL(38, 9)", map("numeric", 100, -1));
    }

    // ─── Char types ──────────────────────────────────────────────────────────

    @Test
    void bpchar_shortLength_isChar() {
        // length=10 → 10*3=30 ≤ 255 → CHAR(30)
        assertEquals("CHAR(30)", map("bpchar", 10, -1));
        assertEquals("CHAR(3)",  map("bpchar", 1, -1));
    }

    @Test
    void bpchar_longLength_isVarchar() {
        // length=100 → 100*3=300 > 255 → VARCHAR(300)
        assertEquals("VARCHAR(300)", map("bpchar", 100, -1));
        assertEquals("VARCHAR(768)", map("bpchar", 256, -1));
    }

    @Test
    void varcharAndText_isString() {
        assertEquals(DorisType.STRING, map("varchar", 50, -1));
        assertEquals(DorisType.STRING, map("varchar", -1, -1));
        assertEquals(DorisType.STRING, map("text", -1, -1));
    }

    // ─── Date / time ─────────────────────────────────────────────────────────

    @Test
    void dateType() {
        assertEquals(DorisType.DATE, map("date", -1, -1));
    }

    @Test
    void timestampType_defaultScale_isDatetime6() {
        // scale < 0 or > 6 → default to 6
        assertEquals("DATETIME(6)", map("timestamp", -1, -1));
        assertEquals("DATETIME(6)", map("timestamptz", -1, -1));
        assertEquals("DATETIME(6)", map("timestamp", -1, 7));
    }

    @Test
    void timestampType_explicitScale() {
        assertEquals("DATETIME(3)", map("timestamp", -1, 3));
        assertEquals("DATETIME(0)", map("timestamptz", -1, 0));
        assertEquals("DATETIME(6)", map("timestamp", -1, 6));
    }

    @Test
    void timeTypes_isString() {
        assertEquals(DorisType.STRING, map("time", -1, -1));
        assertEquals(DorisType.STRING, map("timetz", -1, -1));
        assertEquals(DorisType.STRING, map("interval", -1, -1));
    }

    // ─── JSON ────────────────────────────────────────────────────────────────

    @Test
    void jsonTypes() {
        assertEquals(DorisType.JSON, map("json", -1, -1));
        assertEquals(DorisType.JSON, map("jsonb", -1, -1));
    }

    // ─── Geometric / network / misc types (all map to STRING) ────────────────

    @Test
    void networkAndMiscTypes_isString() {
        assertEquals(DorisType.STRING, map("inet",    -1, -1));
        assertEquals(DorisType.STRING, map("cidr",    -1, -1));
        assertEquals(DorisType.STRING, map("macaddr", -1, -1));
        assertEquals(DorisType.STRING, map("uuid",    -1, -1));
        assertEquals(DorisType.STRING, map("bytea",   -1, -1));
        assertEquals(DorisType.STRING, map("varbit",  -1, -1));
    }

    @Test
    void macaddr8XmlHstoreTypes_isString() {
        assertEquals(DorisType.STRING, map("macaddr8", -1, -1));
        assertEquals(DorisType.STRING, map("xml",      -1, -1));
        assertEquals(DorisType.STRING, map("hstore",   -1, -1));
    }

    @Test
    void geometricTypes_isString() {
        assertEquals(DorisType.STRING, map("point", -1, -1));
        assertEquals(DorisType.STRING, map("line", -1, -1));
        assertEquals(DorisType.STRING, map("lseg", -1, -1));
        assertEquals(DorisType.STRING, map("box", -1, -1));
        assertEquals(DorisType.STRING, map("path", -1, -1));
        assertEquals(DorisType.STRING, map("polygon", -1, -1));
        assertEquals(DorisType.STRING, map("circle", -1, -1));
    }

    // ─── Array types ─────────────────────────────────────────────────────────

    @Test
    void arrayTypes() {
        // covers the 10 types required by test_streaming_postgres_job_array_types
        assertEquals("ARRAY<SMALLINT>",     map("_int2",        -1, -1));
        assertEquals("ARRAY<INT>",          map("_int4",        -1, -1));
        assertEquals("ARRAY<BIGINT>",       map("_int8",        -1, -1));
        assertEquals("ARRAY<FLOAT>",        map("_float4",      -1, -1));
        assertEquals("ARRAY<DOUBLE>",       map("_float8",      -1, -1));
        assertEquals("ARRAY<BOOLEAN>",      map("_bool",        -1, -1));
        assertEquals("ARRAY<STRING>",       map("_varchar",     -1, -1));
        assertEquals("ARRAY<STRING>",       map("_text",        -1, -1));
        assertEquals("ARRAY<DATETIME(6)>",  map("_timestamp",   -1, -1));
        assertEquals("ARRAY<DATETIME(6)>",  map("_timestamptz", -1, -1));
        // additional types
        assertEquals("ARRAY<DATE>",         map("_date",        -1, -1));
        assertEquals("ARRAY<JSON>",         map("_json",        -1, -1));
        assertEquals("ARRAY<JSON>",         map("_jsonb",       -1, -1));
    }

    @Test
    void arrayType_numeric_defaultPrecisionScale() {
        assertEquals("ARRAY<DECIMAL(38, 9)>", map("_numeric", 0, -1));
    }

    @Test
    void arrayType_nested() {
        // Two-dimensional array: __int4 → ARRAY<ARRAY<INT>>
        assertEquals("ARRAY<ARRAY<INT>>", map("__int4", -1, -1));
    }

    // ─── Unknown type fallback ───────────────────────────────────────────────

    @Test
    void unknownType_defaultsToString() {
        assertEquals(DorisType.STRING, map("custom_type", -1, -1));
        assertEquals(DorisType.STRING, map("user_defined_enum", -1, -1));
    }

    // ─── Case-insensitive matching ────────────────────────────────────────────

    @Test
    void caseInsensitive() {
        assertEquals(DorisType.INT,     map("INT4", -1, -1));
        assertEquals(DorisType.BIGINT,  map("INT8", -1, -1));
        assertEquals(DorisType.BOOLEAN, map("BOOL", -1, -1));
        assertEquals(DorisType.FLOAT,   map("FLOAT4", -1, -1));
        assertEquals(DorisType.JSON,    map("JSON", -1, -1));
        assertEquals(DorisType.STRING,  map("TEXT", -1, -1));
    }

    // ─── helper ──────────────────────────────────────────────────────────────

    private static String map(String pgType, int length, int scale) {
        return SchemaChangeHelper.pgTypeNameToDorisType(pgType, length, scale);
    }
}
