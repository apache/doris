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


package org.apache.doris.jdbc;

import org.apache.doris.jni.spi.utils.OffHeap;
import org.apache.doris.jni.spi.vec.ColumnType;
import org.apache.doris.jni.spi.vec.VectorColumn;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

class JdbcTimestampBindTest {
    @Test
    void oracleAndPrestoBindUtcTextForExplicitServerCasts() throws Exception {
        for (String dialect : new String[] {"ORACLE", "PRESTO"}) {
            JdbcTypeHandler handler = JdbcTypeHandlerFactory.create(dialect);
            PreparedStatement statement = Mockito.mock(PreparedStatement.class);
            handler.setTimestampTz(statement, 1, LocalDateTime.of(2020, 1, 2, 4, 1, 0, 111333000));
            Mockito.verify(statement).setString(1, "2020-01-02 04:01:00.111333"
                    + (dialect.equals("ORACLE") ? " +00:00" : " UTC"));
            handler.setTimestampTzNull(statement, 1);
            Mockito.verify(statement).setNull(1, Types.VARCHAR);
        }
    }

    @Test
    void testPostgresBindsExplicitOffset() throws Exception {
        verifyBind(PostgreSQLTypeHandler.class);
    }

    @Test
    void testOracleBindsExplicitOffset() throws Exception {
        verifyBind(OracleTypeHandler.class);
    }

    @Test
    void testClickHouseBindsInstantInsteadOfLocalFields() throws Exception {
        verifyBind(ClickHouseTypeHandler.class);
    }

    @Test
    void testTrinoAndPrestoBindTypedZonedLiteral() throws Exception {
        verifyBind(TrinoTypeHandler.class);
    }

    @Test
    void testTrinoAndPrestoNullBindUsesSupportedSqlType() throws Exception {
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        JdbcJniWriter executor = writer("TRINO", statement);
        Method insert = JdbcJniWriter.class.getDeclaredMethod("insertNullColumn", int.class, ColumnType.Type.class);
        insert.setAccessible(true);
        insert.invoke(executor, 1, ColumnType.Type.TIMESTAMPTZ);
        Mockito.verify(statement).setNull(1, Types.NULL);
    }

    private void verifyBind(Class<? extends JdbcTypeHandler> dialect) throws Exception {
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        JdbcJniWriter executor = writer(dialect.getSimpleName().replace("TypeHandler", "").toUpperCase(), statement);
        OffHeap.setTesting();
        VectorColumn column = VectorColumn.createWritableColumn(ColumnType.parseType("event_time", "timestamptz(6)"), 1);
        Method insert = JdbcJniWriter.class.getDeclaredMethod("insertColumn", int.class, int.class, VectorColumn.class);
        insert.setAccessible(true);
        TimeZone previous = TimeZone.getDefault();
        try {
            for (String zone : new String[] {"UTC", "Asia/Shanghai", "America/Los_Angeles"}) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                for (String text : new String[] {"2020-01-02T04:01:00.111333Z", "1969-12-31T23:59:59.999999Z",
                        "2023-11-05T08:30:00.123456Z", "2023-11-05T09:30:00.123456Z", "2020-01-02T00:00:00Z"}) {
                    LocalDateTime utc = LocalDateTime.ofInstant(Instant.parse(text), ZoneOffset.UTC);
                    column.reset();
                    column.appendTimeStampTz(utc);
                    insert.invoke(executor, 0, 0, column);
                    // A timestamp without an offset can be reinterpreted by the remote session or column zone.
                    if (dialect == TrinoTypeHandler.class) {
                        Mockito.verify(statement).setObject(1,
                                utc.format(DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS")) + " UTC",
                                Types.TIMESTAMP_WITH_TIMEZONE);
                    } else if (dialect == OracleTypeHandler.class) {
                        Mockito.verify(statement).setString(1,
                                utc.format(DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS")) + " +00:00");
                    } else if (dialect == ClickHouseTypeHandler.class) {
                        Mockito.verify(statement).setObject(1, utc.atOffset(ZoneOffset.UTC));
                    } else {
                        Mockito.verify(statement).setObject(1,
                                utc.atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE);
                    }
                    Mockito.clearInvocations(statement);
                }
                column.reset();
                column.appendTimeStampTz(new LocalDateTime[] {null}, true);
                insert.invoke(executor, 0, 0, column);
                Mockito.verify(statement).setNull(1,
                        dialect == TrinoTypeHandler.class ? Types.NULL
                                : dialect == OracleTypeHandler.class ? Types.VARCHAR : Types.TIMESTAMP_WITH_TIMEZONE);
                Mockito.clearInvocations(statement);
            }
        } finally {
            column.close();
            TimeZone.setDefault(previous);
        }
    }

    static JdbcJniWriter writer(String dialect, PreparedStatement statement) throws Exception {
        JdbcJniWriter writer = new JdbcJniWriter(1, java.util.Collections.singletonMap("table_type", dialect));
        java.lang.reflect.Field field = JdbcJniWriter.class.getDeclaredField("preparedStatement");
        field.setAccessible(true);
        field.set(writer, statement);
        return writer;
    }
}
