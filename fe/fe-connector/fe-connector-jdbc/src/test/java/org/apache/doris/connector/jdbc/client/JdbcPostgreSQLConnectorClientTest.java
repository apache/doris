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


package org.apache.doris.connector.jdbc.client;

import org.apache.doris.connector.jdbc.JdbcDbType;
import org.apache.doris.connector.spi.ConnectorType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class JdbcPostgreSQLConnectorClientTest {

    private static JdbcPostgreSQLConnectorClient client(Connection connection) {
        return new JdbcPostgreSQLConnectorClient("test_catalog", JdbcDbType.POSTGRESQL,
                "jdbc:postgresql://localhost:5432/test", false,
                Collections.emptyMap(), Collections.emptyMap(), false, false) {
            @Override
            public Connection getConnection() {
                return connection;
            }
        };
    }

    private static Map<String, Object> columnRow(String schema, String table, String column, String typeName,
            int dataType) {
        return FakeJdbc.row("TABLE_SCHEM", schema, "TABLE_NAME", table, "COLUMN_NAME", column,
                "DATA_TYPE", dataType, "TYPE_NAME", typeName, "COLUMN_SIZE", 10, "DECIMAL_DIGITS", 0,
                "NUM_PREC_RADIX", 10, "NULLABLE", 1, "REMARKS", "", "CHAR_OCTET_LENGTH", 0);
    }

    @Test
    public void columnsOfNeighbourTablesMatchedByLikeWildcardsAreDropped() {
        // getColumns() treats the names as LIKE patterns and the client does not escape '_' (some drivers
        // cannot take an escape there), so a lookup of t_1 also returns the columns of tx1 and of the same
        // table in schema sX. Only the rows of the requested schema.table may come back (#63402).
        List<Map<String, Object>> rows = Arrays.asList(
                columnRow("s_1", "t_1", "id", "int4", Types.INTEGER),
                columnRow("s_1", "tx1", "leaked_col", "text", Types.VARCHAR),
                columnRow("sx1", "t_1", "leaked_col2", "text", Types.VARCHAR),
                columnRow("s_1", "t_1", "name", "varchar", Types.VARCHAR));
        Connection conn = FakeJdbc.connection(FakeJdbc.resultSet(rows), null, sql -> null, new ArrayList<>());

        List<JdbcFieldInfo> fields = client(conn).getJdbcColumnsInfo("s_1", "t_1");

        List<String> names = new ArrayList<>();
        for (JdbcFieldInfo f : fields) {
            names.add(f.getColumnName());
        }
        Assertions.assertEquals(Arrays.asList("id", "name"), names);
    }

    private static JdbcFieldInfo field(String typeName, int dataType, int dims) {
        return new JdbcFieldInfo("col", Optional.of(typeName), dataType, Optional.of(10), Optional.of(0),
                Optional.of(dims));
    }

    @Test
    public void arrayElementTypes() {
        JdbcPostgreSQLConnectorClient client = client(null);

        ConnectorType intArray = client.jdbcTypeToConnectorType(field("_int4", Types.ARRAY, 1));
        Assertions.assertEquals("ARRAY", intArray.getTypeName());
        Assertions.assertEquals("INT", intArray.getChildren().get(0).getTypeName());

        ConnectorType nested = client.jdbcTypeToConnectorType(field("_int8", Types.ARRAY, 2));
        Assertions.assertEquals("ARRAY", nested.getTypeName());
        Assertions.assertEquals("ARRAY", nested.getChildren().get(0).getTypeName());
        Assertions.assertEquals("BIGINT", nested.getChildren().get(0).getChildren().get(0).getTypeName());

        // A Doris CHAR(n) counts bytes, a PostgreSQL char(n) counts characters; inside an array there is
        // nowhere to widen it, so the element is a STRING (as before the SPI migration), not CHAR(n).
        ConnectorType charArray = client.jdbcTypeToConnectorType(field("_bpchar", Types.ARRAY, 1));
        Assertions.assertEquals("STRING", charArray.getChildren().get(0).getTypeName());

        // Element types outside the mapped set degrade to STRING, matching the CDC client's mapping.
        ConnectorType macaddrArray = client.jdbcTypeToConnectorType(field("_macaddr", Types.ARRAY, 1));
        Assertions.assertEquals("STRING", macaddrArray.getChildren().get(0).getTypeName());
    }

    @Test
    public void scalarTypesAddedForStreaming() {
        JdbcPostgreSQLConnectorClient client = client(null);
        for (String pgType : new String[] {"macaddr8", "xml", "hstore"}) {
            Assertions.assertEquals("STRING",
                    client.jdbcTypeToConnectorType(field(pgType, Types.OTHER, 0)).getTypeName(), pgType);
        }
    }
}
