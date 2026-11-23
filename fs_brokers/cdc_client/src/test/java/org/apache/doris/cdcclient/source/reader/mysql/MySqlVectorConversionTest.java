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

package org.apache.doris.cdcclient.source.reader.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.jdbc.JdbcValueConverters.BigIntUnsignedMode;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ValueConverter;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;

class MySqlVectorConversionTest {
    @Test
    void parsesVectorTypeAndUnquotedVectorIdentifiers() {
        MySqlAntlrDdlParser parser = new MySqlAntlrDdlParser();
        parser.setCurrentDatabase("db1");
        Tables tables = new Tables();
        parser.parse(
                "CREATE TABLE vector (id INT PRIMARY KEY, vector INT, v1 VECTOR(4), v2 VECTOR)",
                tables);
        Table table = tables.forTable(new TableId("db1", null, "vector"));
        assertEquals(Types.INTEGER, table.columnWithName("vector").jdbcType());
        assertEquals("VECTOR", table.columnWithName("v1").typeName());
        assertEquals(4, table.columnWithName("v1").length());
        assertEquals(2048, table.columnWithName("v2").length());
    }

    @Test
    void decodesLittleEndianFloat32AndPreservesNull() {
        ValueConverter converter = vectorConverter(true);
        assertEquals(
                Arrays.asList(1.25f, -2.5f, 0.0f, 4.0f),
                converter.convert(
                        new byte[] {
                            0,
                            0,
                            (byte) 0xa0,
                            0x3f,
                            0,
                            0,
                            0x20,
                            (byte) 0xc0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            (byte) 0x80,
                            0x40
                        }));
        assertNull(converter.convert(null));
    }

    @Test
    void convertsFloatArray() {
        assertEquals(
                Arrays.asList(1.25f, -2.5f, 0.0f, 4.0f),
                vectorConverter(true).convert(new float[] {1.25f, -2.5f, 0.0f, 4.0f}));
    }

    @Test
    void requiredVectorUsesListFallback() {
        assertEquals(Collections.emptyList(), vectorConverter(false).convert(null));
    }

    @Test
    void truncatedFloat32ReturnsEmptyList() {
        assertEquals(Collections.emptyList(), vectorConverter(true).convert(new byte[] {1, 2, 3}));
    }

    private ValueConverter vectorConverter(boolean optional) {
        MySqlValueConverters converters =
                new MySqlValueConverters(
                        DecimalMode.PRECISE, TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS,
                        BigIntUnsignedMode.PRECISE, BinaryHandlingMode.BYTES);
        Column column =
                Column.editor()
                        .name("v1")
                        .type("VECTOR")
                        .jdbcType(Types.OTHER)
                        .length(4)
                        .optional(optional)
                        .create();
        SchemaBuilder builder = converters.schemaBuilder(column);
        if (optional) {
            builder.optional();
        }
        Schema schema = builder.build();
        assertEquals(Schema.Type.ARRAY, schema.type());
        assertEquals(Schema.Type.FLOAT32, schema.valueSchema().type());
        return converters.converter(column, new Field("v1", 0, schema));
    }
}
