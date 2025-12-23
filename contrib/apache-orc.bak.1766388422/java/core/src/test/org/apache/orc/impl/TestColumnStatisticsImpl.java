/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.orc.DecimalColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.TimestampColumnStatistics;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestColumnStatisticsImpl {

  @Test
  public void testUpdateDate() {
    ColumnStatisticsImpl stat = ColumnStatisticsImpl.create(TypeDescription.createDate());
    DateWritable date = new DateWritable(16400);
    stat.increment();
    stat.updateDate(date);
    assertDateStatistics(stat, 1, 16400, 16400);

    date.set(16410);
    stat.increment();
    stat.updateDate(date);
    assertDateStatistics(stat, 2, 16400, 16410);

    date.set(16420);
    stat.increment();
    stat.updateDate(date);
    assertDateStatistics(stat, 3, 16400, 16420);
  }

  private void assertDateStatistics(ColumnStatisticsImpl stat, int count, int minimum, int maximum) {
    OrcProto.ColumnStatistics.Builder builder = stat.serialize();

    assertEquals(count, builder.getNumberOfValues());
    assertTrue(builder.hasDateStatistics());
    assertFalse(builder.hasStringStatistics());

    OrcProto.DateStatistics protoStat = builder.getDateStatistics();
    assertTrue(protoStat.hasMinimum());
    assertEquals(minimum, protoStat.getMinimum());
    assertTrue(protoStat.hasMaximum());
    assertEquals(maximum, protoStat.getMaximum());
  }

  @Test
  public void testOldTimestamps() throws IOException {
    TimeZone original = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
    Path exampleDir = new Path(System.getProperty("example.dir"));
    Path file = new Path(exampleDir, "TestOrcFile.testTimestamp.orc");
    Configuration conf = new Configuration();
    Reader reader = OrcFile.createReader(file, OrcFile.readerOptions(conf));
    TimestampColumnStatistics stats =
        (TimestampColumnStatistics) reader.getStatistics()[0];
    assertEquals("1995-01-01 00:00:00.688", stats.getMinimum().toString());
    // ORC-611: add TS stats nanosecond support for older files by using (max TS + 0.999 ms)
    assertEquals("2037-01-01 00:00:00.000999999", stats.getMaximum().toString());
    TimeZone.setDefault(original);
  }

  @Test
  public void testTimestamps() {
    TimeZone original = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    TypeDescription instant = TypeDescription.createTimestampInstant();
    ColumnStatisticsImpl stats = ColumnStatisticsImpl.create(instant);
    TimestampColumnStatistics dstats = (TimestampColumnStatistics) stats;
    assertNull(dstats.getMinimumUTC());
    assertNull(dstats.getMaximumUTC());
    stats.updateTimestamp(123, 456789);
    stats.updateTimestamp(1234, 567890);
    stats.increment(2);
    assertEquals("1970-01-01 00:00:00.123456789", dstats.getMinimum().toString());
    assertEquals("1970-01-01 00:00:01.23456789", dstats.getMaximum().toString());
    stats.updateTimestamp(123, 400000);
    stats.updateTimestamp(1234, 600000);
    assertEquals("1970-01-01 00:00:00.1234", dstats.getMinimum().toString());
    assertEquals("1970-01-01 00:00:01.2346", dstats.getMaximum().toString());
    stats.updateTimestamp(122, 300000);
    stats.updateTimestamp(1235, 400000);
    assertEquals("1970-01-01 00:00:00.1223", dstats.getMinimum().toString());
    assertEquals("1970-01-01 00:00:01.2354", dstats.getMaximum().toString());
    stats.merge(stats);
    assertEquals("1970-01-01 00:00:00.1223", dstats.getMinimum().toString());
    assertEquals("1970-01-01 00:00:01.2354", dstats.getMaximum().toString());
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(instant);
    stats2.updateTimestamp(100, 1);
    stats2.increment(1);
    TimestampColumnStatistics dstats2 = (TimestampColumnStatistics) stats2;
    assertEquals("1970-01-01 00:00:00.100000001", dstats2.getMinimum().toString());
    assertEquals("1970-01-01 00:00:00.100000001", dstats2.getMaximum().toString());
    stats.merge(stats2);
    assertEquals("1970-01-01 00:00:00.100000001", dstats.getMinimum().toString());
    assertEquals("1970-01-01 00:00:01.2354", dstats.getMaximum().toString());
    stats2.updateTimestamp(2000, 123456);
    assertEquals("1970-01-01 00:00:00.100000001", dstats2.getMinimum().toString());
    assertEquals("1970-01-01 00:00:02.000123456", dstats2.getMaximum().toString());
    stats.merge(stats2);
    assertEquals("1970-01-01 00:00:00.100000001", dstats.getMinimum().toString());
    assertEquals("1970-01-01 00:00:02.000123456", dstats.getMaximum().toString());
    TimeZone.setDefault(original);
  }

  @Test
  public void testDecimal64Overflow() {
    TypeDescription schema = TypeDescription.fromString("decimal(18,6)");
    OrcProto.ColumnStatistics.Builder pb =
        OrcProto.ColumnStatistics.newBuilder();
    OrcProto.DecimalStatistics.Builder decimalBuilder =
        OrcProto.DecimalStatistics.newBuilder();
    decimalBuilder.setMaximum("1000.0");
    decimalBuilder.setMinimum("1.010");
    decimalBuilder.setSum("123456789.123456");
    pb.setDecimalStatistics(decimalBuilder);
    pb.setHasNull(false);
    pb.setNumberOfValues(3);

    // the base case doesn't overflow
    DecimalColumnStatistics stats1 = (DecimalColumnStatistics)
        ColumnStatisticsImpl.deserialize(schema, pb.build());
    ColumnStatisticsImpl updateStats1 = (ColumnStatisticsImpl) stats1;
    assertEquals("1.01", stats1.getMinimum().toString());
    assertEquals("1000", stats1.getMaximum().toString());
    assertEquals("123456789.123456", stats1.getSum().toString());
    assertEquals(3, stats1.getNumberOfValues());

    // Now set the sum to something that overflows Decimal64.
    decimalBuilder.setSum("1234567890123.45");
    pb.setDecimalStatistics(decimalBuilder);
    DecimalColumnStatistics stats2 = (DecimalColumnStatistics)
        ColumnStatisticsImpl.deserialize(schema, pb.build());
    assertNull(stats2.getSum());

    // merge them together
    updateStats1.merge((ColumnStatisticsImpl) stats2);
    assertNull(stats1.getSum());

    updateStats1.reset();
    assertEquals("0", stats1.getSum().toString());
    updateStats1.increment();
    updateStats1.updateDecimal64(10000, 6);
    assertEquals("0.01", stats1.getSum().toString());
    updateStats1.updateDecimal64(1, 4);
    assertEquals("0.0101", stats1.getSum().toString());
    updateStats1.updateDecimal64(TypeDescription.MAX_DECIMAL64, 6);
    assertNull(stats1.getSum());
    updateStats1.reset();
    updateStats1.updateDecimal64(TypeDescription.MAX_DECIMAL64, 6);
    assertEquals("999999999999.999999", stats1.getSum().toString());
    updateStats1.updateDecimal64(1, 6);
    assertNull(stats1.getSum());

    updateStats1.reset();
    ColumnStatisticsImpl updateStats2 = (ColumnStatisticsImpl) stats2;
    updateStats2.reset();
    updateStats1.increment();
    updateStats2.increment();
    updateStats1.updateDecimal64(TypeDescription.MAX_DECIMAL64, 6);
    updateStats2.updateDecimal64(TypeDescription.MAX_DECIMAL64, 6);
    assertEquals("999999999999.999999", stats1.getSum().toString());
    assertEquals("999999999999.999999", stats2.getSum().toString());
    updateStats1.merge(updateStats2);
    assertNull(stats1.getSum());
  }

  @Test
  public void testCollectionColumnStats() {
    /* test List */
    final ColumnStatisticsImpl statList = ColumnStatisticsImpl.create(TypeDescription.createList(TypeDescription.createInt()));

    statList.increment();
    statList.updateCollectionLength(10);

    statList.increment();
    statList.updateCollectionLength(20);

    statList.increment();
    statList.updateCollectionLength(30);

    statList.increment();
    statList.updateCollectionLength(40);

    final OrcProto.ColumnStatistics.Builder builder = statList.serialize();
    final OrcProto.CollectionStatistics collectionStatistics = builder.getCollectionStatistics();

    assertEquals(10, collectionStatistics.getMinChildren());
    assertEquals(40, collectionStatistics.getMaxChildren());
    assertEquals(100, collectionStatistics.getTotalChildren());

  }
}
