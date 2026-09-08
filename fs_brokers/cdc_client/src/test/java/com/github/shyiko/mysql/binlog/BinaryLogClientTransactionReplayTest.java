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

package com.github.shyiko.mysql.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class BinaryLogClientTransactionReplayTest {

    private static final String BINLOG_FILE = "mysql-bin.000001";
    private static final long TRANSACTION_START = 100L;

    @Test
    void rewindsIncompleteNonGtidTransaction() {
        BinaryLogClient client = clientAtTransactionStart();
        client.updateNonGtidTransactionStateBeforeEvent(
                queryEvent("BEGIN", TRANSACTION_START, 150L));

        client.setBinlogPosition(300L);
        client.rewindToTransactionStartIfNeeded();

        assertThat(client.getBinlogFilename()).isEqualTo(BINLOG_FILE);
        assertThat(client.getBinlogPosition()).isEqualTo(TRANSACTION_START);
    }

    @ParameterizedTest
    @EnumSource(value = EventType.class, names = {"ANONYMOUS_GTID", "MARIADB_GTID"})
    void tracksNonGtidTransactionStartAtGtidMarker(EventType transactionStart) {
        BinaryLogClient client = clientAtTransactionStart();
        client.updateNonGtidTransactionStateBeforeEvent(
                event(transactionStart, TRANSACTION_START, 150L));

        client.setBinlogPosition(300L);
        client.rewindToTransactionStartIfNeeded();

        assertThat(client.getBinlogPosition()).isEqualTo(TRANSACTION_START);
    }

    @ParameterizedTest
    @EnumSource(value = EventType.class, names = {"XID", "TRANSACTION_PAYLOAD"})
    void doesNotRewindCompletedNonGtidTransaction(EventType transactionEnd) {
        BinaryLogClient client = clientAtTransactionStart();
        client.updateNonGtidTransactionStateBeforeEvent(
                queryEvent("BEGIN", TRANSACTION_START, 150L));
        client.updateNonGtidTransactionStateAfterEvent(event(transactionEnd));

        client.setBinlogPosition(300L);
        client.rewindToTransactionStartIfNeeded();

        assertThat(client.getBinlogPosition()).isEqualTo(300L);
    }

    @ParameterizedTest
    @ValueSource(strings = {"COMMIT", "ROLLBACK"})
    void doesNotRewindCompletedQueryTransaction(String transactionEnd) {
        BinaryLogClient client = clientAtTransactionStart();
        client.updateNonGtidTransactionStateBeforeEvent(
                queryEvent("BEGIN", TRANSACTION_START, 150L));
        client.updateNonGtidTransactionStateAfterEvent(queryEvent(transactionEnd, 300L, 350L));

        client.setBinlogPosition(350L);
        client.rewindToTransactionStartIfNeeded();

        assertThat(client.getBinlogPosition()).isEqualTo(350L);
    }

    @Test
    void keepsGtidReconnectBehaviorUnchanged() {
        BinaryLogClient client = clientAtTransactionStart();
        client.setGtidSet("");
        client.updateNonGtidTransactionStateBeforeEvent(
                queryEvent("BEGIN", TRANSACTION_START, 150L));

        client.setBinlogPosition(300L);
        client.rewindToTransactionStartIfNeeded();

        assertThat(client.getBinlogPosition()).isEqualTo(300L);
    }

    private static BinaryLogClient clientAtTransactionStart() {
        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "password");
        client.setBinlogFilename(BINLOG_FILE);
        client.setBinlogPosition(TRANSACTION_START);
        return client;
    }

    private static Event queryEvent(String sql, long position, long nextPosition) {
        QueryEventData data = new QueryEventData();
        data.setSql(sql);
        EventHeaderV4 header = eventHeader(EventType.QUERY);
        header.setEventLength(nextPosition - position);
        header.setNextPosition(nextPosition);
        return new Event(header, data);
    }

    private static Event event(EventType eventType) {
        return new Event(eventHeader(eventType), null);
    }

    private static Event event(EventType eventType, long position, long nextPosition) {
        EventHeaderV4 header = eventHeader(eventType);
        header.setEventLength(nextPosition - position);
        header.setNextPosition(nextPosition);
        return new Event(header, null);
    }

    private static EventHeaderV4 eventHeader(EventType eventType) {
        EventHeaderV4 header = new EventHeaderV4();
        header.setEventType(eventType);
        return header;
    }
}
