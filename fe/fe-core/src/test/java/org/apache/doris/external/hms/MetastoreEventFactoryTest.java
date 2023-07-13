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

package org.apache.doris.external.hms;

import org.apache.doris.datasource.hive.event.AddPartitionEvent;
import org.apache.doris.datasource.hive.event.AlterDatabaseEvent;
import org.apache.doris.datasource.hive.event.AlterPartitionEvent;
import org.apache.doris.datasource.hive.event.AlterTableEvent;
import org.apache.doris.datasource.hive.event.CreateDatabaseEvent;
import org.apache.doris.datasource.hive.event.CreateTableEvent;
import org.apache.doris.datasource.hive.event.DropTableEvent;
import org.apache.doris.datasource.hive.event.InsertEvent;
import org.apache.doris.datasource.hive.event.MetastoreEvent;
import org.apache.doris.datasource.hive.event.MetastoreEventFactory;

import org.apache.hadoop.util.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;


public class MetastoreEventFactoryTest {
    private static final MetastoreEventFactory factory = new MetastoreEventFactory();

    @Test
    public void testCreateBatchEvents() {
        AlterPartitionEvent e1 = new AlterPartitionEvent(1L, "test_ctl", "test_db", "t1", "p1", "p1");
        AlterPartitionEvent e2 = new AlterPartitionEvent(2L, "test_ctl", "test_db", "t1", "p1", "p1");
        AddPartitionEvent e3 = new AddPartitionEvent(3L, "test_ctl", "test_db", "t1", Arrays.asList("p1"));
        AlterTableEvent e4 = new AlterTableEvent(4L, "test_ctl", "test_db", "t1", false, false);
        AlterTableEvent e5 = new AlterTableEvent(5L, "test_ctl", "test_db", "t1", true, false);
        AlterTableEvent e6 = new AlterTableEvent(6L, "test_ctl", "test_db", "t1", false, true);
        DropTableEvent e7 = new DropTableEvent(7L, "test_ctl", "test_db", "t1");
        InsertEvent e8 = new InsertEvent(8L, "test_ctl", "test_db", "t1");
        CreateDatabaseEvent e9 = new CreateDatabaseEvent(9L, "test_ctl", "test_db2");
        AlterPartitionEvent e10 = new AlterPartitionEvent(10L, "test_ctl", "test_db", "t2", "p1", "p1");
        AlterTableEvent e11 = new AlterTableEvent(11L, "test_ctl", "test_db", "t1", false, false);
        CreateTableEvent e12 = new CreateTableEvent(12L, "test_ctl", "test_db", "t1");
        AlterDatabaseEvent e13 = new AlterDatabaseEvent(13L, "test_ctl", "test_db", true);
        AlterDatabaseEvent e14 = new AlterDatabaseEvent(14L, "test_ctl", "test_db", false);

        List<MetastoreEvent> mergedEvents;
        List<MetastoreEvent> testEvents = Lists.newLinkedList();

        testEvents.add(e1);
        testEvents.add(e2);
        mergedEvents = factory.createBatchEvents("test_ctl", testEvents);
        Assertions.assertTrue(mergedEvents.size() == 1);
        Assertions.assertTrue(mergedEvents.get(0).getEventId() == 2L);

        testEvents.clear();
        testEvents.add(e1);
        testEvents.add(e2);
        testEvents.add(e3);
        testEvents.add(e9);
        testEvents.add(e10);
        testEvents.add(e4);
        mergedEvents = factory.createBatchEvents("test_ctl", testEvents);
        Assertions.assertTrue(mergedEvents.size() == 3);
        Assertions.assertTrue(mergedEvents.get(0).getEventId() == 9L);
        Assertions.assertTrue(mergedEvents.get(1).getEventId() == 10L);
        Assertions.assertTrue(mergedEvents.get(2).getEventId() == 4L);

        // because e5 is a rename event, it will not be merged
        testEvents.clear();
        testEvents.add(e1);
        testEvents.add(e2);
        testEvents.add(e10);
        testEvents.add(e5);
        testEvents.add(e4);
        mergedEvents = factory.createBatchEvents("test_ctl", testEvents);
        Assertions.assertTrue(mergedEvents.size() == 3);
        Assertions.assertTrue(mergedEvents.get(0).getEventId() == 10L);
        Assertions.assertTrue(mergedEvents.get(1).getEventId() == 5L);
        Assertions.assertTrue(mergedEvents.get(2).getEventId() == 4L);

        testEvents.clear();
        testEvents.add(e1);
        testEvents.add(e2);
        testEvents.add(e10);
        testEvents.add(e6);
        testEvents.add(e4);
        mergedEvents = factory.createBatchEvents("test_ctl", testEvents);
        Assertions.assertTrue(mergedEvents.size() == 3);
        Assertions.assertTrue(mergedEvents.get(0).getEventId() == 10L);
        Assertions.assertTrue(mergedEvents.get(1).getEventId() == 6L);
        Assertions.assertTrue(mergedEvents.get(2).getEventId() == 4L);

        testEvents.clear();
        testEvents.add(e1);
        testEvents.add(e2);
        testEvents.add(e10);
        testEvents.add(e4);
        testEvents.add(e11);
        mergedEvents = factory.createBatchEvents("test_ctl", testEvents);
        Assertions.assertTrue(mergedEvents.size() == 2);
        Assertions.assertTrue(mergedEvents.get(0).getEventId() == 10L);
        Assertions.assertTrue(mergedEvents.get(1).getEventId() == 11L);

        testEvents.clear();
        testEvents.add(e1);
        testEvents.add(e2);
        testEvents.add(e10);
        testEvents.add(e4);
        testEvents.add(e8);
        mergedEvents = factory.createBatchEvents("test_ctl", testEvents);
        Assertions.assertTrue(mergedEvents.size() == 2);
        Assertions.assertTrue(mergedEvents.get(0).getEventId() == 10L);
        Assertions.assertTrue(mergedEvents.get(1).getEventId() == 8L);

        // because e5 is a rename event, it will not be merged
        testEvents.clear();
        testEvents.add(e1);
        testEvents.add(e2);
        testEvents.add(e10);
        testEvents.add(e5);
        testEvents.add(e8);
        mergedEvents = factory.createBatchEvents("test_ctl", testEvents);
        Assertions.assertTrue(mergedEvents.size() == 3);
        Assertions.assertTrue(mergedEvents.get(0).getEventId() == 10L);
        Assertions.assertTrue(mergedEvents.get(1).getEventId() == 5L);
        Assertions.assertTrue(mergedEvents.get(2).getEventId() == 8L);

        testEvents.clear();
        testEvents.add(e1);
        testEvents.add(e2);
        testEvents.add(e10);
        testEvents.add(e12);
        testEvents.add(e4);
        testEvents.add(e7);
        mergedEvents = factory.createBatchEvents("test_ctl", testEvents);
        Assertions.assertTrue(mergedEvents.size() == 2);
        Assertions.assertTrue(mergedEvents.get(0).getEventId() == 10L);
        Assertions.assertTrue(mergedEvents.get(1).getEventId() == 7L);

        // because e5 is a rename event, it will not be merged
        testEvents.clear();
        testEvents.add(e1);
        testEvents.add(e2);
        testEvents.add(e10);
        testEvents.add(e5);
        testEvents.add(e7);
        mergedEvents = factory.createBatchEvents("test_ctl", testEvents);
        Assertions.assertTrue(mergedEvents.size() == 3);
        Assertions.assertTrue(mergedEvents.get(0).getEventId() == 10L);
        Assertions.assertTrue(mergedEvents.get(1).getEventId() == 5L);
        Assertions.assertTrue(mergedEvents.get(2).getEventId() == 7L);

        testEvents.clear();
        testEvents.add(e1);
        testEvents.add(e2);
        testEvents.add(e10);
        testEvents.add(e4);
        testEvents.add(e13);
        testEvents.add(e7);
        mergedEvents = factory.createBatchEvents("test_ctl", testEvents);
        Assertions.assertTrue(mergedEvents.size() == 4);
        Assertions.assertTrue(mergedEvents.get(0).getEventId() == 10L);
        Assertions.assertTrue(mergedEvents.get(1).getEventId() == 4L);
        Assertions.assertTrue(mergedEvents.get(2).getEventId() == 13L);
        Assertions.assertTrue(mergedEvents.get(3).getEventId() == 7L);

        testEvents.clear();
        testEvents.add(e1);
        testEvents.add(e2);
        testEvents.add(e10);
        testEvents.add(e4);
        testEvents.add(e14);
        testEvents.add(e7);
        mergedEvents = factory.createBatchEvents("test_ctl", testEvents);
        Assertions.assertTrue(mergedEvents.size() == 3);
        Assertions.assertTrue(mergedEvents.get(0).getEventId() == 10L);
        Assertions.assertTrue(mergedEvents.get(1).getEventId() == 14L);
        Assertions.assertTrue(mergedEvents.get(2).getEventId() == 7L);
    }
}
