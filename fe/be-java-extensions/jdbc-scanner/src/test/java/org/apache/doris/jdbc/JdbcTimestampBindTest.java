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

import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.VectorColumn;

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
    void testPostgresBindsExplicitOffset() throws Exception {
        verifyBind(PostgreSQLJdbcExecutor.class);
    }

    @Test
    void testOracleBindsExplicitOffset() throws Exception {
        verifyBind(OracleJdbcExecutor.class);
    }

    @Test
    void testClickHouseBindsInstantInsteadOfLocalFields() throws Exception {
        verifyBind(ClickHouseJdbcExecutor.class);
    }

    @Test
    void testTrinoAndPrestoBindTypedZonedLiteral() throws Exception {
        verifyBind(TrinoJdbcExecutor.class);
    }

    @Test
    void testTrinoAndPrestoNullBindUsesSupportedSqlType() throws Exception {
        TrinoJdbcExecutor executor = Mockito.mock(TrinoJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        executor.preparedStatement = Mockito.mock(PreparedStatement.class);
        Method insert = BaseJdbcExecutor.class.getDeclaredMethod("insertNullColumn", int.class, ColumnType.Type.class);
        insert.setAccessible(true);
        insert.invoke(executor, 1, ColumnType.Type.TIMESTAMPTZ);
        Mockito.verify(executor.preparedStatement).setNull(1, Types.NULL);
    }

    private void verifyBind(Class<? extends BaseJdbcExecutor> dialect) throws Exception {
        BaseJdbcExecutor executor = Mockito.mock(dialect, Mockito.CALLS_REAL_METHODS);
        executor.preparedStatement = Mockito.mock(PreparedStatement.class);
        OffHeap.setTesting();
        VectorColumn column = VectorColumn.createWritableColumn(ColumnType.parseType("event_time", "timestamptz(6)"), 1);
        Method insert = BaseJdbcExecutor.class.getDeclaredMethod("insertColumn", int.class, int.class, VectorColumn.class);
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
                    if (dialect == TrinoJdbcExecutor.class) {
                        Mockito.verify(executor.preparedStatement).setObject(1,
                                utc.format(DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS")) + " UTC",
                                Types.TIMESTAMP_WITH_TIMEZONE);
                    } else if (dialect == ClickHouseJdbcExecutor.class) {
                        Mockito.verify(executor.preparedStatement).setObject(1, utc.atOffset(ZoneOffset.UTC));
                    } else {
                        Mockito.verify(executor.preparedStatement).setObject(1,
                                utc.atOffset(ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE);
                    }
                    Mockito.clearInvocations(executor.preparedStatement);
                }
                column.reset();
                column.appendTimeStampTz(new LocalDateTime[] {null}, true);
                insert.invoke(executor, 0, 0, column);
                Mockito.verify(executor.preparedStatement).setNull(1,
                        dialect == TrinoJdbcExecutor.class ? Types.NULL : Types.TIMESTAMP_WITH_TIMEZONE);
                Mockito.clearInvocations(executor.preparedStatement);
            }
        } finally {
            column.close();
            TimeZone.setDefault(previous);
        }
    }
}
