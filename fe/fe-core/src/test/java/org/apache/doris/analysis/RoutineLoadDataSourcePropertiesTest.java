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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.routineload.KafkaProgress;

import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class RoutineLoadDataSourcePropertiesTest {

    @Test
    public void testCreateNormal() throws UserException {
        // normal
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY, "127.0.0.1:8080");
        properties.put(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY, "test");
        properties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "0, 1, 2");
        properties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "100, 101, 102");
        RoutineLoadDataSourceProperties dsProperties = new RoutineLoadDataSourceProperties("KAFKA", properties, false);
        dsProperties.setTimezone(TimeUtils.DEFAULT_TIME_ZONE);
        try {
            dsProperties.analyze();
            Assert.assertEquals("127.0.0.1:8080", dsProperties.getKafkaBrokerList());
            Assert.assertEquals("test", dsProperties.getKafkaTopic());
            List<Pair<Integer, Long>> partitinOffsets = dsProperties.getKafkaPartitionOffsets();
            Assert.assertEquals(3, partitinOffsets.size());
            Assert.assertEquals(Integer.valueOf(0), partitinOffsets.get(0).first);
            Assert.assertEquals(Integer.valueOf(1), partitinOffsets.get(1).first);
            Assert.assertEquals(Integer.valueOf(2), partitinOffsets.get(2).first);
            Assert.assertEquals(Long.valueOf(100), partitinOffsets.get(0).second);
            Assert.assertEquals(Long.valueOf(101), partitinOffsets.get(1).second);
            Assert.assertEquals(Long.valueOf(102), partitinOffsets.get(2).second);
            Assert.assertFalse(dsProperties.isOffsetsForTimes());
        } catch (AnalysisException e) {
            Assert.fail(e.getMessage());
        }

        // normal, with datetime
        properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY, "127.0.0.1:8080");
        properties.put(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY, "test");
        properties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "0, 1, 2");
        properties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "2021-10-10 11:00:00, 2021-10-10 11:00:00, 2021-10-10 12:00:00");
        dsProperties = new RoutineLoadDataSourceProperties("KAFKA", properties, false);
        dsProperties.setTimezone(TimeUtils.DEFAULT_TIME_ZONE);
        try {
            dsProperties.analyze();
            Assert.assertEquals("127.0.0.1:8080", dsProperties.getKafkaBrokerList());
            Assert.assertEquals("test", dsProperties.getKafkaTopic());
            List<Pair<Integer, Long>> partitinOffsets = dsProperties.getKafkaPartitionOffsets();
            Assert.assertEquals(3, partitinOffsets.size());
            Assert.assertEquals(Integer.valueOf(0), partitinOffsets.get(0).first);
            Assert.assertEquals(Integer.valueOf(1), partitinOffsets.get(1).first);
            Assert.assertEquals(Integer.valueOf(2), partitinOffsets.get(2).first);
            Assert.assertEquals(Long.valueOf(1633834800000L), partitinOffsets.get(0).second);
            Assert.assertEquals(Long.valueOf(1633834800000L), partitinOffsets.get(1).second);
            Assert.assertEquals(Long.valueOf(1633838400000L), partitinOffsets.get(2).second);
            Assert.assertTrue(dsProperties.isOffsetsForTimes());
        } catch (AnalysisException e) {
            Assert.fail(e.getMessage());
        }

        // normal, with default offset as datetime
        properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY, "127.0.0.1:8080");
        properties.put(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY, "test");
        properties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "0, 1, 2");
        properties.put("property." + CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS, "2020-01-10 00:00:00");
        dsProperties = new RoutineLoadDataSourceProperties("KAFKA", properties, false);
        dsProperties.setTimezone(TimeUtils.DEFAULT_TIME_ZONE);
        try {
            dsProperties.analyze();
            Assert.assertEquals("127.0.0.1:8080", dsProperties.getKafkaBrokerList());
            Assert.assertEquals("test", dsProperties.getKafkaTopic());
            List<Pair<Integer, Long>> partitinOffsets = dsProperties.getKafkaPartitionOffsets();
            Assert.assertEquals(3, partitinOffsets.size());
            Assert.assertEquals(Integer.valueOf(0), partitinOffsets.get(0).first);
            Assert.assertEquals(Integer.valueOf(1), partitinOffsets.get(1).first);
            Assert.assertEquals(Integer.valueOf(2), partitinOffsets.get(2).first);
            Assert.assertEquals(Long.valueOf(1578585600000L), partitinOffsets.get(0).second);
            Assert.assertEquals(Long.valueOf(1578585600000L), partitinOffsets.get(1).second);
            Assert.assertEquals(Long.valueOf(1578585600000L), partitinOffsets.get(2).second);
            Assert.assertEquals(2, dsProperties.getCustomKafkaProperties().size());
            Assert.assertEquals("1578585600000", dsProperties.getCustomKafkaProperties().get(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS));
            Assert.assertEquals("2020-01-10 00:00:00", dsProperties.getCustomKafkaProperties().get(CreateRoutineLoadStmt.KAFKA_ORIGIN_DEFAULT_OFFSETS));
            Assert.assertTrue(dsProperties.isOffsetsForTimes());
        } catch (AnalysisException e) {
            Assert.fail(e.getMessage());
        }

        // normal, only set default offset as datetime
        properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY, "127.0.0.1:8080");
        properties.put(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY, "test");
        properties.put("property." + CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS, "2020-01-10 00:00:00");
        dsProperties = new RoutineLoadDataSourceProperties("KAFKA", properties, false);
        dsProperties.setTimezone(TimeUtils.DEFAULT_TIME_ZONE);
        try {
            dsProperties.analyze();
            Assert.assertEquals("127.0.0.1:8080", dsProperties.getKafkaBrokerList());
            Assert.assertEquals("test", dsProperties.getKafkaTopic());
            List<Pair<Integer, Long>> partitinOffsets = dsProperties.getKafkaPartitionOffsets();
            Assert.assertEquals(0, partitinOffsets.size());
            Assert.assertEquals(2, dsProperties.getCustomKafkaProperties().size());
            Assert.assertEquals("1578585600000", dsProperties.getCustomKafkaProperties().get(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS));
            Assert.assertEquals("2020-01-10 00:00:00", dsProperties.getCustomKafkaProperties().get(CreateRoutineLoadStmt.KAFKA_ORIGIN_DEFAULT_OFFSETS));
        } catch (AnalysisException e) {
            Assert.fail(e.getMessage());
        }

        // normal, only set default offset as integer
        properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY, "127.0.0.1:8080");
        properties.put(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY, "test");
        properties.put("property." + CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS, KafkaProgress.OFFSET_END);
        dsProperties = new RoutineLoadDataSourceProperties("KAFKA", properties, false);
        dsProperties.setTimezone(TimeUtils.DEFAULT_TIME_ZONE);
        try {
            dsProperties.analyze();
            Assert.assertEquals("127.0.0.1:8080", dsProperties.getKafkaBrokerList());
            Assert.assertEquals("test", dsProperties.getKafkaTopic());
            List<Pair<Integer, Long>> partitinOffsets = dsProperties.getKafkaPartitionOffsets();
            Assert.assertEquals(0, partitinOffsets.size());
            Assert.assertEquals(1, dsProperties.getCustomKafkaProperties().size());
            Assert.assertEquals(KafkaProgress.OFFSET_END, dsProperties.getCustomKafkaProperties().get(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS));
        } catch (AnalysisException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testCreateAbnormal() {
        // can not set KAFKA_OFFSETS_PROPERTY and KAFKA_DEFAULT_OFFSETS together
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY, "127.0.0.1:8080");
        properties.put(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY, "test");
        properties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "0, 1, 2");
        properties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "1, 1, 1");
        properties.put("property." + CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS, "-1");
        RoutineLoadDataSourceProperties dsProperties = new RoutineLoadDataSourceProperties("KAFKA", properties, false);
        dsProperties.setTimezone(TimeUtils.DEFAULT_TIME_ZONE);
        try {
            dsProperties.analyze();
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Only one of kafka_offsets and kafka_default_offsets can be set."));
        }

        // can not set datetime formatted offset and integer offset together
        properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY, "127.0.0.1:8080");
        properties.put(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY, "test");
        properties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "0, 1, 2");
        properties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "1, 2020-10-10 12:11:11, 1");
        dsProperties = new RoutineLoadDataSourceProperties("KAFKA", properties, false);
        dsProperties.setTimezone(TimeUtils.DEFAULT_TIME_ZONE);
        try {
            dsProperties.analyze();
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("The offset of the partition cannot be specified by the timestamp " +
                    "and the offset at the same time"));
        }

        // no partitions but has offset
        properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY, "127.0.0.1:8080");
        properties.put(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY, "test");
        properties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "1, 1, 1");
        dsProperties = new RoutineLoadDataSourceProperties("KAFKA", properties, false);
        dsProperties.setTimezone(TimeUtils.DEFAULT_TIME_ZONE);
        try {
            dsProperties.analyze();
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Partitions number should be equals to offsets number"));
        }
    }

    @Test
    public void testAlterNormal() throws UserException {
        // normal
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "0, 1, 2");
        properties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "100, 101, 102");
        RoutineLoadDataSourceProperties dsProperties = new RoutineLoadDataSourceProperties("KAFKA", properties, true);
        dsProperties.setTimezone(TimeUtils.DEFAULT_TIME_ZONE);
        try {
            dsProperties.analyze();
            Assert.assertEquals("", dsProperties.getKafkaBrokerList());
            Assert.assertEquals("", dsProperties.getKafkaTopic());
            List<Pair<Integer, Long>> partitinOffsets = dsProperties.getKafkaPartitionOffsets();
            Assert.assertEquals(3, partitinOffsets.size());
            Assert.assertEquals(Integer.valueOf(0), partitinOffsets.get(0).first);
            Assert.assertEquals(Integer.valueOf(1), partitinOffsets.get(1).first);
            Assert.assertEquals(Integer.valueOf(2), partitinOffsets.get(2).first);
            Assert.assertEquals(Long.valueOf(100), partitinOffsets.get(0).second);
            Assert.assertEquals(Long.valueOf(101), partitinOffsets.get(1).second);
            Assert.assertEquals(Long.valueOf(102), partitinOffsets.get(2).second);
        } catch (AnalysisException e) {
            Assert.fail(e.getMessage());
        }

        // normal, with datetime
        properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "0, 1, 2");
        properties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "2021-10-10 11:00:00, 2021-10-10 11:00:00, 2021-10-10 12:00:00");
        dsProperties = new RoutineLoadDataSourceProperties("KAFKA", properties, true);
        dsProperties.setTimezone(TimeUtils.DEFAULT_TIME_ZONE);
        try {
            // can not set KAFKA_OFFSETS_PROPERTY and KAFKA_DEFAULT_OFFSETS togather
            dsProperties.analyze();
            List<Pair<Integer, Long>> partitinOffsets = dsProperties.getKafkaPartitionOffsets();
            Assert.assertEquals(3, partitinOffsets.size());
            Assert.assertEquals(Integer.valueOf(0), partitinOffsets.get(0).first);
            Assert.assertEquals(Integer.valueOf(1), partitinOffsets.get(1).first);
            Assert.assertEquals(Integer.valueOf(2), partitinOffsets.get(2).first);
            Assert.assertEquals(Long.valueOf(1633834800000L), partitinOffsets.get(0).second);
            Assert.assertEquals(Long.valueOf(1633834800000L), partitinOffsets.get(1).second);
            Assert.assertEquals(Long.valueOf(1633838400000L), partitinOffsets.get(2).second);
        } catch (AnalysisException e) {
            Assert.fail(e.getMessage());
        }

        // normal, with default offset as datetime
        properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "0, 1, 2");
        properties.put("property." + CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS, "2020-01-10 00:00:00");
        dsProperties = new RoutineLoadDataSourceProperties("KAFKA", properties, true);
        dsProperties.setTimezone(TimeUtils.DEFAULT_TIME_ZONE);
        try {
            // can not set KAFKA_OFFSETS_PROPERTY and KAFKA_DEFAULT_OFFSETS togather
            dsProperties.analyze();
            List<Pair<Integer, Long>> partitinOffsets = dsProperties.getKafkaPartitionOffsets();
            Assert.assertEquals(3, partitinOffsets.size());
            Assert.assertEquals(Integer.valueOf(0), partitinOffsets.get(0).first);
            Assert.assertEquals(Integer.valueOf(1), partitinOffsets.get(1).first);
            Assert.assertEquals(Integer.valueOf(2), partitinOffsets.get(2).first);
            Assert.assertEquals(Long.valueOf(1578585600000L), partitinOffsets.get(0).second);
            Assert.assertEquals(Long.valueOf(1578585600000L), partitinOffsets.get(1).second);
            Assert.assertEquals(Long.valueOf(1578585600000L), partitinOffsets.get(2).second);
            Assert.assertEquals(2, dsProperties.getCustomKafkaProperties().size());
            Assert.assertEquals("1578585600000", dsProperties.getCustomKafkaProperties().get(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS));
            Assert.assertEquals("2020-01-10 00:00:00", dsProperties.getCustomKafkaProperties().get(CreateRoutineLoadStmt.KAFKA_ORIGIN_DEFAULT_OFFSETS));
        } catch (AnalysisException e) {
            Assert.fail(e.getMessage());
        }

        // normal, only set default offset, with utc timezone
        properties = Maps.newHashMap();
        properties.put("property." + CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS, "2020-01-10 00:00:00");
        dsProperties = new RoutineLoadDataSourceProperties("KAFKA", properties, true);
        dsProperties.setTimezone(TimeUtils.UTC_TIME_ZONE);
        try {
            // can not set KAFKA_OFFSETS_PROPERTY and KAFKA_DEFAULT_OFFSETS togather
            dsProperties.analyze();
            List<Pair<Integer, Long>> partitinOffsets = dsProperties.getKafkaPartitionOffsets();
            Assert.assertEquals(0, partitinOffsets.size());
            Assert.assertEquals(2, dsProperties.getCustomKafkaProperties().size());
            Assert.assertEquals("1578614400000", dsProperties.getCustomKafkaProperties().get(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS));
            Assert.assertEquals("2020-01-10 00:00:00", dsProperties.getCustomKafkaProperties().get(CreateRoutineLoadStmt.KAFKA_ORIGIN_DEFAULT_OFFSETS));
        } catch (AnalysisException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testAlterAbnormal() {
        // now support set KAFKA_BROKER_LIST_PROPERTY
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY, "127.0.0.1:8080");
        properties.put("property." + CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS, "-1");
        RoutineLoadDataSourceProperties dsProperties = new RoutineLoadDataSourceProperties("KAFKA", properties, true);
        dsProperties.setTimezone(TimeUtils.DEFAULT_TIME_ZONE);
        try {
            dsProperties.analyze();
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("kafka_default_offsets can only be set to OFFSET_BEGINNING, OFFSET_END or date time"));
        }

        // can not set datetime formatted offset and integer offset together
        properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "0, 1, 2");
        properties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "1, 2020-10-10 12:11:11, 1");
        dsProperties = new RoutineLoadDataSourceProperties("KAFKA", properties, true);
        dsProperties.setTimezone(TimeUtils.DEFAULT_TIME_ZONE);
        try {
            // can not set KAFKA_OFFSETS_PROPERTY and KAFKA_DEFAULT_OFFSETS togather
            dsProperties.analyze();
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("The offset of the partition cannot be specified by the timestamp " +
                    "and the offset at the same time"));
        }

        // no partitions but has offset
        properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "1, 1, 1");
        dsProperties = new RoutineLoadDataSourceProperties("KAFKA", properties, true);
        dsProperties.setTimezone(TimeUtils.DEFAULT_TIME_ZONE);
        try {
            // can not set KAFKA_OFFSETS_PROPERTY and KAFKA_DEFAULT_OFFSETS togather
            dsProperties.analyze();
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Partitions number should be equals to offsets number"));
        }

        // only set partition
        properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "1, 1, 1");
        dsProperties = new RoutineLoadDataSourceProperties("KAFKA", properties, true);
        dsProperties.setTimezone(TimeUtils.DEFAULT_TIME_ZONE);
        try {
            dsProperties.analyze();
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Must set offset or default offset with partition property"));
        }
    }
}
