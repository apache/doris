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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.datasource.jdbc.client.JdbcClient;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.util.StreamingJobUtils;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PostgresResourceValidatorTest {

    @Test
    public void testAlterPublicationOwnershipAndLegacyName() throws Exception {
        for (String publication : List.of("custom_pub", DataSourceConfigKeys.defaultPublicationName("1"),
                DataSourceConfigKeys.LEGACY_PUBLICATION_NAME)) {
            boolean userProvided = !DataSourceConfigKeys.defaultPublicationName("1").equals(publication);
            Map<String, String> props = new HashMap<>();
            props.put(DataSourceConfigKeys.SCHEMA, "public");
            if (!DataSourceConfigKeys.LEGACY_PUBLICATION_NAME.equals(publication)) {
                props.put(DataSourceConfigKeys.PUBLICATION_NAME, publication);
            }
            JdbcClient client = Mockito.mock(JdbcClient.class);
            Connection connection = Mockito.mock(Connection.class);
            PreparedStatement existence = Mockito.mock(PreparedStatement.class);
            PreparedStatement membership = Mockito.mock(PreparedStatement.class);
            ResultSet exists = Mockito.mock(ResultSet.class);
            ResultSet tables = Mockito.mock(ResultSet.class);
            Statement statement = Mockito.mock(Statement.class);
            Mockito.when(client.getConnection()).thenReturn(connection);
            Mockito.when(connection.prepareStatement("SELECT 1 FROM pg_publication WHERE pubname = ?"))
                    .thenReturn(existence);
            Mockito.when(connection.prepareStatement(
                    "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = ?"))
                    .thenReturn(membership);
            Mockito.when(existence.executeQuery()).thenReturn(exists);
            Mockito.when(exists.next()).thenReturn(true);
            Mockito.when(membership.executeQuery()).thenReturn(tables);
            Mockito.when(tables.next()).thenReturn(true, true, false);
            Mockito.when(tables.getString(1)).thenReturn("public");
            Mockito.when(tables.getString(2)).thenReturn("retained", "removed");
            Mockito.when(connection.createStatement()).thenReturn(statement);
            List<String> desired = List.of("retained", "added");
            try (MockedStatic<StreamingJobUtils> utilities = Mockito.mockStatic(StreamingJobUtils.class)) {
                utilities.when(() -> StreamingJobUtils.getJdbcClient(DataSourceType.POSTGRES, props))
                        .thenReturn(client);
                if (userProvided) {
                    JobException error = Assertions.assertThrows(JobException.class,
                            () -> PostgresResourceValidator.ensurePublicationCoversTables(props, "1", desired));
                    Assertions.assertTrue(error.getMessage().contains("is missing required tables: [public.added]"));
                } else {
                    PostgresResourceValidator.ensurePublicationCoversTables(props, "1", desired);
                }

                // A covered publication needs no mutation, including retries after an ADD succeeded.
                Mockito.when(tables.next()).thenReturn(true, true, false);
                Mockito.when(tables.getString(2)).thenReturn("retained", "added");
                PostgresResourceValidator.ensurePublicationCoversTables(props, "1", desired);
                if (userProvided) {
                    Mockito.verify(connection, Mockito.never()).createStatement();
                    Mockito.verify(existence, Mockito.times(2)).setString(1, publication);
                    Mockito.verify(membership, Mockito.times(2)).setString(1, publication);
                    Mockito.when(exists.next()).thenReturn(false);
                    JobException error = Assertions.assertThrows(JobException.class,
                            () -> PostgresResourceValidator.ensurePublicationCoversTables(props, "1", desired));
                    Assertions.assertTrue(error.getMessage().contains("publication does not exist: " + publication));
                } else {
                    Mockito.verify(statement).execute("ALTER PUBLICATION \"" + publication
                            + "\" ADD TABLE \"public\".\"added\"");
                    Mockito.verify(statement).close();
                    Mockito.verifyNoMoreInteractions(statement);
                }
            }
        }
    }

    // A 22-char CJK database name is length()==22 but 66 bytes in UTF-8; PG truncates it to 63 bytes.
    // The byte-based check must reject it before connecting (validate fails on the very first line).
    @Test
    public void testRejectMultibyteOverLongDatabaseName() {
        String dbName = StringUtils.repeat("库", 22);
        Assertions.assertEquals(22, dbName.length());
        Map<String, String> props = new HashMap<>();
        props.put(DataSourceConfigKeys.DATABASE, dbName);
        JobException e = Assertions.assertThrows(JobException.class,
                () -> PostgresResourceValidator.validate(props, "1", Collections.emptyList()));
        Assertions.assertTrue(e.getMessage().contains("bytes"), e.getMessage());
    }
}
