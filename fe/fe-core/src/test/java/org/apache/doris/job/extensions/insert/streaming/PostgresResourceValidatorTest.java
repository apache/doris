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

import org.apache.doris.connector.spi.ConnectorQueryResult;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.util.StreamingJobUtils;
import org.apache.doris.job.util.StreamingSourceClient;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PostgresResourceValidatorTest {

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

    private static ConnectorQueryResult rows(List<List<Object>> rows) {
        return new ConnectorQueryResult(Collections.singletonList("c"), rows);
    }

    /**
     * A source whose publication {@code pubName} covers {@code coveredTables} and whose slot {@code slotName}
     * reports {@code slotActive} (null = no such slot); every other name is unknown.
     */
    private static StreamingSourceClient source(String pubName, List<String> coveredTables, String slotName,
            Boolean slotActive) throws Exception {
        StreamingSourceClient client = Mockito.mock(StreamingSourceClient.class);
        Mockito.when(client.executeQuery(Mockito.contains("FROM pg_publication WHERE"), Mockito.anyList()))
                .thenAnswer(inv -> rows(pubName.equals(param(inv.getArgument(1)))
                        ? Collections.singletonList(Collections.singletonList(1)) : Collections.emptyList()));
        Mockito.when(client.executeQuery(Mockito.contains("FROM pg_publication_tables"), Mockito.anyList()))
                .thenAnswer(inv -> {
                    List<List<Object>> covered = new ArrayList<>();
                    if (pubName.equals(param(inv.getArgument(1)))) {
                        for (String t : coveredTables) {
                            covered.add(Arrays.asList(t.split("\\.")[0], t.split("\\.")[1]));
                        }
                    }
                    return rows(covered);
                });
        Mockito.when(client.executeQuery(Mockito.contains("FROM pg_replication_slots"), Mockito.anyList()))
                .thenAnswer(inv -> rows(slotName.equals(param(inv.getArgument(1))) && slotActive != null
                        ? Collections.singletonList(Collections.singletonList(slotActive))
                        : Collections.emptyList()));
        return client;
    }

    private static Object param(List<Object> params) {
        return params.isEmpty() ? null : params.get(0);
    }

    private static Map<String, String> pgProps() {
        Map<String, String> props = new HashMap<>();
        props.put(DataSourceConfigKeys.DATABASE, "db");
        props.put(DataSourceConfigKeys.SCHEMA, "public");
        return props;
    }

    @Test
    public void defaultNamesPassWhenNothingConflicts() throws Exception {
        StreamingSourceClient client = source("none", Collections.emptyList(), "none", null);
        try (MockedStatic<StreamingJobUtils> utils = Mockito.mockStatic(StreamingJobUtils.class)) {
            serving(utils, client);
            PostgresResourceValidator.validate(pgProps(), "7", Collections.singletonList("t"));
        }
        // The names reach the source as bound parameters, never spliced into the SQL text.
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        Mockito.verify(client, Mockito.atLeastOnce()).executeQuery(sql.capture(), Mockito.anyList());
        for (String statement : sql.getAllValues()) {
            Assertions.assertTrue(statement.contains("= ?"), statement);
            Assertions.assertFalse(statement.contains("doris_"), statement);
        }
        Mockito.verify(client).close();
    }

    private static void serving(MockedStatic<StreamingJobUtils> utils, StreamingSourceClient client) {
        utils.when(() -> StreamingJobUtils.openSourceClient(Mockito.eq(DataSourceType.POSTGRES),
                Mockito.anyMap())).thenReturn(client);
    }

    @Test
    public void userProvidedPublicationMustExistAndCoverTheTables() throws Exception {
        Map<String, String> props = pgProps();
        props.put(DataSourceConfigKeys.PUBLICATION_NAME, "my_pub");
        StreamingSourceClient withoutPub = source("other_pub", Collections.emptyList(), "none", null);
        StreamingSourceClient partialPub = source("my_pub", Collections.singletonList("public.a"), "none", null);
        try (MockedStatic<StreamingJobUtils> utils = Mockito.mockStatic(StreamingJobUtils.class)) {
            serving(utils, withoutPub);
            JobException missing = Assertions.assertThrows(JobException.class,
                    () -> PostgresResourceValidator.validate(props, "7", Collections.singletonList("t")));
            Assertions.assertTrue(missing.getMessage().contains("publication does not exist: my_pub"),
                    missing.getMessage());

            serving(utils, partialPub);
            JobException uncovered = Assertions.assertThrows(JobException.class,
                    () -> PostgresResourceValidator.validate(props, "7", Arrays.asList("a", "t")));
            Assertions.assertTrue(uncovered.getMessage().contains("missing required tables: [public.t]"),
                    uncovered.getMessage());
        }
    }

    @Test
    public void activeDorisOwnedSlotIsAConflict() throws Exception {
        String defaultSlot = DataSourceConfigKeys.defaultSlotName("7");
        // The driver reports the slot's active flag as a Boolean, not a String.
        StreamingSourceClient activeSlot = source("none", Collections.emptyList(), defaultSlot, Boolean.TRUE);
        StreamingSourceClient idleSlot = source("none", Collections.emptyList(), defaultSlot, Boolean.FALSE);
        try (MockedStatic<StreamingJobUtils> utils = Mockito.mockStatic(StreamingJobUtils.class)) {
            serving(utils, activeSlot);
            JobException e = Assertions.assertThrows(JobException.class,
                    () -> PostgresResourceValidator.validate(pgProps(), "7", Collections.singletonList("t")));
            Assertions.assertTrue(e.getMessage().contains("is active, held by another consumer"), e.getMessage());

            // An inactive Doris-owned slot from an earlier run is simply reused.
            serving(utils, idleSlot);
            PostgresResourceValidator.validate(pgProps(), "7", Collections.singletonList("t"));
        }
    }

    @Test
    public void userProvidedSlotMustExist() throws Exception {
        Map<String, String> props = pgProps();
        props.put(DataSourceConfigKeys.SLOT_NAME, "my_slot");
        StreamingSourceClient otherSlot = source("none", Collections.emptyList(), "other", null);
        try (MockedStatic<StreamingJobUtils> utils = Mockito.mockStatic(StreamingJobUtils.class)) {
            serving(utils, otherSlot);
            JobException e = Assertions.assertThrows(JobException.class,
                    () -> PostgresResourceValidator.validate(props, "7", Collections.singletonList("t")));
            Assertions.assertTrue(e.getMessage().contains("replication slot does not exist: my_slot"),
                    e.getMessage());
        }
    }
}
