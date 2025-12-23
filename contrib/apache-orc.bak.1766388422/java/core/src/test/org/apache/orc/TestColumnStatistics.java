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

package org.apache.orc;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.impl.ColumnStatisticsImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test ColumnStatisticsImpl for ORC.
 */
public class TestColumnStatistics {

  @Test
  public void testLongSumOverflow() {
    TypeDescription schema = TypeDescription.createInt();
    ColumnStatisticsImpl stats = ColumnStatisticsImpl.create(schema);
    stats.updateInteger(1, 1);
    assertTrue(((IntegerColumnStatistics) stats).isSumDefined());
    stats.updateInteger(Long.MAX_VALUE, 3);
    assertFalse(((IntegerColumnStatistics) stats).isSumDefined());
  }

  @Test
  public void testLongMerge() throws Exception {
    TypeDescription schema = TypeDescription.createInt();

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
    stats1.updateInteger(10, 2);
    stats2.updateInteger(1, 1);
    stats2.updateInteger(1000, 1);
    stats1.merge(stats2);
    IntegerColumnStatistics typed = (IntegerColumnStatistics) stats1;
    assertEquals(1, typed.getMinimum());
    assertEquals(1000, typed.getMaximum());
    stats1.reset();
    stats1.updateInteger(-10, 1);
    stats1.updateInteger(10000, 1);
    stats1.merge(stats2);
    assertEquals(-10, typed.getMinimum());
    assertEquals(10000, typed.getMaximum());
  }

  @Test
  public void testDoubleMerge() throws Exception {
    TypeDescription schema = TypeDescription.createDouble();

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
    stats1.updateDouble(10.0);
    stats1.updateDouble(100.0);
    stats2.updateDouble(1.0);
    stats2.updateDouble(1000.0);
    stats1.merge(stats2);
    DoubleColumnStatistics typed = (DoubleColumnStatistics) stats1;
    assertEquals(1.0, typed.getMinimum(), 0.001);
    assertEquals(1000.0, typed.getMaximum(), 0.001);
    stats1.reset();
    stats1.updateDouble(-10);
    stats1.updateDouble(10000);
    stats1.merge(stats2);
    assertEquals(-10, typed.getMinimum(), 0.001);
    assertEquals(10000, typed.getMaximum(), 0.001);
  }


  @Test
  public void testStringMerge() throws Exception {
    TypeDescription schema = TypeDescription.createString();

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
    stats1.increment(3);
    stats1.updateString(new Text("bob"));
    stats1.updateString(new Text("david"));
    stats1.updateString(new Text("charles"));
    stats2.increment(2);
    stats2.updateString(new Text("anne"));
    byte[] erin = new byte[]{0, 1, 2, 3, 4, 5, 101, 114, 105, 110};
    stats2.increment();
    stats2.updateString(erin, 6, 4, 5);
    assertEquals(24, ((StringColumnStatistics)stats2).getSum());
    stats1.merge(stats2);
    StringColumnStatistics typed = (StringColumnStatistics) stats1;
    assertEquals("anne", typed.getMinimum());
    assertEquals("erin", typed.getMaximum());
    assertEquals(39, typed.getSum());
    stats1.reset();
    stats1.increment(2);
    stats1.updateString(new Text("aaa"));
    stats1.updateString(new Text("zzz"));
    stats1.merge(stats2);
    assertEquals("aaa", typed.getMinimum());
    assertEquals("zzz", typed.getMaximum());
  }

  @Test
  public void testUpperAndLowerBounds() throws Exception {
    final TypeDescription schema = TypeDescription.createString();

    final String test = RandomStringUtils.random(1024+10);
    final String fragment = "foo"+test;
    final String fragmentLowerBound = "bar"+test;


    final ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    final ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);

    /* test a scenario for the first max string */
    stats1.increment();
    stats1.updateString(new Text(test));

    final StringColumnStatistics typed = (StringColumnStatistics) stats1;
    final StringColumnStatistics typed2 = (StringColumnStatistics) stats2;

    assertTrue(1024 >= typed.getUpperBound().getBytes(StandardCharsets.UTF_8).length,
        "Upperbound cannot be more than 1024 bytes");
    assertTrue(1024 >= typed.getLowerBound().getBytes(StandardCharsets.UTF_8).length,
        "Lowerbound cannot be more than 1024 bytes");

    assertNull(typed.getMinimum());
    assertNull(typed.getMaximum());

    stats1.reset();

    /* test a scenario for the first max bytes */
    stats1.increment();
    stats1.updateString(test.getBytes(StandardCharsets.UTF_8), 0,
        test.getBytes(StandardCharsets.UTF_8).length, 0);

    assertTrue(1024 >= typed.getLowerBound().getBytes(StandardCharsets.UTF_8).length,
        "Lowerbound cannot be more than 1024 bytes");
    assertTrue(1024 >= typed.getUpperBound().getBytes(StandardCharsets.UTF_8).length,
            "Upperbound cannot be more than 1024 bytes");

    assertNull(typed.getMinimum());
    assertNull(typed.getMaximum());

    stats1.reset();
    /* test upper bound - merging  */
    stats1.increment(3);
    stats1.updateString(new Text("bob"));
    stats1.updateString(new Text("david"));
    stats1.updateString(new Text("charles"));

    stats2.increment(2);
    stats2.updateString(new Text("anne"));
    stats2.updateString(new Text(fragment));

    assertEquals("anne", typed2.getMinimum());
    assertNull(typed2.getMaximum());

    stats1.merge(stats2);

    assertEquals("anne", typed.getMinimum());
    assertNull(typed.getMaximum());


    /* test lower bound - merging  */
    stats1.reset();
    stats2.reset();

    stats1.increment(2);
    stats1.updateString(new Text("david"));
    stats1.updateString(new Text("charles"));
    stats2.increment(2);
    stats2.updateString(new Text("jane"));
    stats2.updateString(new Text(fragmentLowerBound));

    stats1.merge(stats2);

    assertNull(typed.getMinimum());
    assertEquals("jane", typed.getMaximum());
  }

  /**
   * Test the string truncation with 1 byte characters. The last character
   * of the truncated string is 0x7f so that it will expand into a 2 byte
   * utf-8 character.
   */
  @Test
  public void testBoundsAscii() {
    StringBuilder buffer = new StringBuilder();
    for(int i=0; i < 256; ++i) {
      buffer.append("Owe\u007fn");
    }
    ColumnStatisticsImpl stats = ColumnStatisticsImpl.create(
            TypeDescription.createString());
    stats.increment();
    stats.updateString(new Text(buffer.toString()));
    StringColumnStatistics stringStats = (StringColumnStatistics) stats;

    // make sure that the min/max are null
    assertNull(stringStats.getMinimum());
    assertNull(stringStats.getMaximum());
    assertEquals(5 * 256, stringStats.getSum());

    // and that the lower and upper bound are correct
    assertEquals(buffer.substring(0, 1024), stringStats.getLowerBound());
    assertEquals("Owe\u0080", stringStats.getUpperBound().substring(1020));
    assertEquals("count: 1 hasNull: false lower: " + stringStats.getLowerBound()
            + " upper: " + stringStats.getUpperBound() + " sum: 1280",
        stringStats.toString());

    // make sure that when we replace the min & max the flags get cleared.
    stats.increment();
    stats.updateString(new Text("xxx"));
    assertEquals("xxx", stringStats.getMaximum());
    assertEquals("xxx", stringStats.getUpperBound());
    stats.increment();
    stats.updateString(new Text("A"));
    assertEquals("A", stringStats.getMinimum());
    assertEquals("A", stringStats.getLowerBound());
    assertEquals("count: 3 hasNull: false min: A max: xxx sum: 1284",
        stats.toString());
  }

  /**
   * Test truncation with 2 byte utf-8 characters.
   */
  @Test
  public void testBoundsTwoByte() {
    StringBuilder buffer = new StringBuilder();
    final String PATTERN = "\u0080\u07ff\u0432\u0246\u0123";
    for(int i=0; i < 256; ++i) {
      buffer.append(PATTERN);
    }
    ColumnStatisticsImpl stats = ColumnStatisticsImpl.create(
        TypeDescription.createString());
    stats.increment();
    stats.updateString(new Text(buffer.toString()));
    StringColumnStatistics stringStats = (StringColumnStatistics) stats;

    // make sure that the min/max are null
    assertNull(stringStats.getMinimum());
    assertNull(stringStats.getMaximum());
    assertEquals(2 * 5 * 256, stringStats.getSum());

    // and that the lower and upper bound are correct
    // 512 two byte characters fit in 1024 bytes
    assertEquals(buffer.substring(0, 512), stringStats.getLowerBound());
    assertEquals(buffer.substring(0, 511),
        stringStats.getUpperBound().substring(0, 511));
    assertEquals("\u0800", stringStats.getUpperBound().substring(511));
  }

  /**
   * Test truncation with 3 byte utf-8 characters.
   */
  @Test
  public void testBoundsThreeByte() {
    StringBuilder buffer = new StringBuilder();
    final String PATTERN = "\uffff\u0800\u4321\u1234\u3137";
    for(int i=0; i < 256; ++i) {
      buffer.append(PATTERN);
    }
    ColumnStatisticsImpl stats = ColumnStatisticsImpl.create(
        TypeDescription.createString());
    stats.increment();
    stats.updateString(new Text(buffer.toString()));
    StringColumnStatistics stringStats = (StringColumnStatistics) stats;

    // make sure that the min/max are null
    assertNull(stringStats.getMinimum());
    assertNull(stringStats.getMaximum());
    assertEquals(3 * 5 * 256, stringStats.getSum());

    // and that the lower and upper bound are correct
    // 341 three byte characters fit in 1024 bytes
    assertEquals(buffer.substring(0, 341), stringStats.getLowerBound());
    assertEquals(buffer.substring(0, 340),
        stringStats.getUpperBound().substring(0,340));
    assertEquals("\ud800\udc00", stringStats.getUpperBound().substring(340));
  }

  /**
   * Test truncation with 4 byte utf-8 characters.
   */
  @Test
  public void testBoundsFourByte() {
    StringBuilder buffer = new StringBuilder();
    final String PATTERN = "\ud800\udc00\ud801\udc01\ud802\udc02\ud803\udc03\ud804\udc04";
    for(int i=0; i < 256; ++i) {
      buffer.append(PATTERN);
    }
    ColumnStatisticsImpl stats = ColumnStatisticsImpl.create(
        TypeDescription.createString());
    stats.increment();
    stats.updateString(new Text(buffer.toString()));
    StringColumnStatistics stringStats = (StringColumnStatistics) stats;

    // make sure that the min/max are null
    assertNull(stringStats.getMinimum());
    assertNull(stringStats.getMaximum());
    assertEquals(4 * 5 * 256, stringStats.getSum());

    // and that the lower and upper bound are correct
    // 256 four byte characters fit in 1024 bytes
    assertEquals(buffer.substring(0, 512), stringStats.getLowerBound());
    assertEquals(buffer.substring(0, 510),
        stringStats.getUpperBound().substring(0, 510));
    assertEquals("\\uD800\\uDC01",
        StringEscapeUtils.escapeJava(stringStats.getUpperBound().substring(510)));
  }

  @Test
  public void testUpperBoundCodepointIncrement() {
    /* test with characters that use more than one byte */
    final String fragment =  "載記応存環敢辞月発併際岩。外現抱疑曲旧持九柏先済索。"
        + "富扁件戒程少交文相修宮由改価苦。位季供幾日本求知集機所江取号均下犯変第勝。"
        + "管今文図石職常暮海営感覧果賞挙。難加判郵年太願会周面市害成産。"
        + "内分載函取片領披見復来車必教。元力理関未法会伊団万球幕点帳幅為都話間。"
        + "親禁感栗合開注読月島月紀間卒派伏闘。幕経阿刊間都紹知禁追半業。"
        + "根案協話射格治位相機遇券外野何。話第勝平当降負京複掲書変痛。"
        + "博年群辺軽妻止和真権暑着要質在破応。"
        + "नीचे मुक्त बिन्दुओ समस्याओ आंतरकार्यक्षमता सुना प्रति सभीकुछ यायेका दिनांक वातावरण ";

    final String input = fragment
            + "मुश्किले केन्द्रिय "
            + "लगती नवंबर प्रमान गयेगया समस्याओ विश्व लिये समजते आपके एकत्रित विकेन्द्रित स्वतंत्र "
            + "व्याख्यान भेदनक्षमता शीघ्र होभर मुखय करता। दर्शाता वातावरण विस्तरणक्षमता दोषसके प्राप्त समाजो "
            + "।क तकनीकी दर्शाता कार्यकर्ता बाधा औषधिक समस्याओ समस्याए गोपनीयता प्राण पसंद "
            + "भीयह नवंबर दोषसके अनुवादक सोफ़तवेर समस्याए क्षमता। कार्य होभर\n";

    final String lowerBound = fragment +
        "मुश्किले केन्द्रिय लगती नवंबर प्रमान गयेगया समस्याओ विश्व लिये ";

    final String upperbound = fragment +
        "मुश्किले केन्द्रिय लगती नवंबर प्रमान गयेगया समस्याओ विश्व लिये!";

    final TypeDescription schema = TypeDescription.createString();
    final ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    byte[] utf8 = input.getBytes(StandardCharsets.UTF_8);
    stats1.updateString(utf8, 0, utf8.length, 1);
    stats1.increment();
    final StringColumnStatistics typed = (StringColumnStatistics) stats1;

    assertEquals(354, typed.getUpperBound().length());
    assertEquals(354, typed.getLowerBound().length());
    assertEquals(1764L, typed.getSum());

    assertEquals(upperbound, typed.getUpperBound());
    assertEquals(lowerBound, typed.getLowerBound());
    OrcProto.ColumnStatistics serial = stats1.serialize().build();
    ColumnStatisticsImpl stats2 =
        ColumnStatisticsImpl.deserialize(schema, serial);
    StringColumnStatistics typed2 = (StringColumnStatistics) stats2;
    assertNull(typed2.getMinimum());
    assertNull(typed2.getMaximum());
    assertEquals(lowerBound, typed2.getLowerBound());
    assertEquals(upperbound, typed2.getUpperBound());
    assertEquals(1764L, typed2.getSum());
  }


  @Test
  public void testDateMerge() throws Exception {
    TypeDescription schema = TypeDescription.createDate();

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
    stats1.updateDate(new DateWritable(1000));
    stats1.updateDate(new DateWritable(100));
    stats1.increment(2);
    stats2.updateDate(new DateWritable(10));
    stats2.updateDate(new DateWritable(2000));
    stats2.increment(2);
    stats1.merge(stats2);
    DateColumnStatistics typed = (DateColumnStatistics) stats1;
    assertEquals(new DateWritable(10).get(), typed.getMinimum());
    assertEquals(new DateWritable(2000).get(), typed.getMaximum());
    stats1.reset();
    stats1.updateDate(new DateWritable(-10));
    stats1.updateDate(new DateWritable(10000));
    stats1.increment(2);
    stats1.merge(stats2);
    assertEquals(new DateWritable(-10).get(), typed.getMinimum());
    assertEquals(new DateWritable(10000).get(), typed.getMaximum());
  }

  @Test
  public void testLocalDateMerge() throws Exception {
    TypeDescription schema = TypeDescription.createDate();

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
    stats1.updateDate(1000);
    stats1.updateDate(100);
    stats1.increment(2);
    stats2.updateDate(10);
    stats2.updateDate(2000);
    stats2.increment(2);
    stats1.merge(stats2);
    DateColumnStatistics typed = (DateColumnStatistics) stats1;
    assertEquals(10, typed.getMinimumDayOfEpoch());
    assertEquals(2000, typed.getMaximumDayOfEpoch());
    stats1.reset();
    stats1.updateDate(-10);
    stats1.updateDate(10000);
    stats1.increment(2);
    stats1.merge(stats2);
    assertEquals(-10, typed.getMinimumLocalDate().toEpochDay());
    assertEquals(10000, typed.getMaximumLocalDate().toEpochDay());
  }

  @Test
  public void testTimestampMergeUTC() throws Exception {
    TypeDescription schema = TypeDescription.createTimestamp();

    TimeZone original = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
    stats1.updateTimestamp(new Timestamp(10));
    stats1.updateTimestamp(new Timestamp(100));
    stats2.updateTimestamp(new Timestamp(1));
    stats2.updateTimestamp(new Timestamp(1000));
    stats1.increment(2);
    stats2.increment(2);
    stats1.merge(stats2);
    TimestampColumnStatistics typed = (TimestampColumnStatistics) stats1;
    assertEquals(1, typed.getMinimum().getTime());
    assertEquals(1000, typed.getMaximum().getTime());
    stats1.reset();
    stats1.updateTimestamp(new Timestamp(-10));
    stats1.updateTimestamp(new Timestamp(10000));
    stats1.increment(2);
    stats1.merge(stats2);
    assertEquals(-10, typed.getMinimum().getTime());
    assertEquals(10000, typed.getMaximum().getTime());
    TimeZone.setDefault(original);
  }

  private static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

  private static Timestamp parseTime(SimpleDateFormat format, String value) {
    try {
      return new Timestamp(format.parse(value).getTime());
    } catch (ParseException e) {
      throw new IllegalArgumentException("bad time parse for " + value, e);
    }
  }

  @Test
  public void testTimestampMergeLA() throws Exception {
    TypeDescription schema = TypeDescription.createTimestamp();

    TimeZone original = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
    SimpleDateFormat format = new SimpleDateFormat(TIME_FORMAT);
    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
    stats1.updateTimestamp(parseTime(format, "2000-04-02 03:30:00"));
    stats1.updateTimestamp(parseTime(format, "2000-04-02 01:30:00"));
    stats1.increment(2);
    stats2.updateTimestamp(parseTime(format, "2000-10-29 01:30:00"));
    stats2.updateTimestamp(parseTime(format, "2000-10-29 03:30:00"));
    stats2.increment(2);
    TimestampColumnStatistics typed = (TimestampColumnStatistics) stats1;
    assertEquals("2000-04-02 01:30:00.0", typed.getMinimum().toString());
    assertEquals("2000-04-02 03:30:00.0", typed.getMaximum().toString());
    stats1.merge(stats2);
    assertEquals("2000-04-02 01:30:00.0", typed.getMinimum().toString());
    assertEquals("2000-10-29 03:30:00.0", typed.getMaximum().toString());
    stats1.reset();
    stats1.updateTimestamp(parseTime(format, "1999-04-04 00:00:00"));
    stats1.updateTimestamp(parseTime(format, "2009-03-08 12:00:00"));
    stats1.increment(2);
    stats1.merge(stats2);
    assertEquals("1999-04-04 00:00:00.0", typed.getMinimum().toString());
    assertEquals("2009-03-08 12:00:00.0", typed.getMaximum().toString());

    // serialize and read back in with phoenix timezone
    OrcProto.ColumnStatistics serial = stats2.serialize().build();
    TimeZone.setDefault(TimeZone.getTimeZone("America/Phoenix"));
    ColumnStatisticsImpl stats3 = ColumnStatisticsImpl.deserialize(schema, serial);
    assertEquals("2000-10-29 01:30:00.0",
        ((TimestampColumnStatistics) stats3).getMinimum().toString());
    assertEquals("2000-10-29 03:30:00.0",
        ((TimestampColumnStatistics) stats3).getMaximum().toString());
    TimeZone.setDefault(original);
  }

  @Test
  public void testTimestampNanoPrecision() {
    TypeDescription schema = TypeDescription.createTimestamp();

    TimeZone original = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    stats1.updateTimestamp(Timestamp.valueOf( "2000-04-02 03:31:00.000"));
    stats1.increment(1);
    TimestampColumnStatistics typed = (TimestampColumnStatistics) stats1;

    // Base case no nanos
    assertEquals("2000-04-02 03:31:00.0", typed.getMinimum().toString());
    assertEquals("2000-04-02 03:31:00.0", typed.getMaximum().toString());

    // Add nano precision to min
    stats1.updateTimestamp(Timestamp.valueOf( "2000-04-01 03:30:00.0005"));
    stats1.increment(1);
    assertEquals("2000-04-01 03:30:00.0005", typed.getMinimum().toString());
    assertEquals("2000-04-02 03:31:00.0", typed.getMaximum().toString());

    // Change max with precision
    stats1.updateTimestamp(Timestamp.valueOf( "2000-04-04 03:30:00.0008"));
    stats1.increment(1);
    assertEquals("2000-04-01 03:30:00.0005", typed.getMinimum().toString());
    assertEquals("2000-04-04 03:30:00.0008", typed.getMaximum().toString());

    // Equal min with nano diff
    stats1.updateTimestamp(Timestamp.valueOf( "2000-04-04 03:30:00.0009"));
    stats1.increment(1);
    assertEquals("2000-04-01 03:30:00.0005", typed.getMinimum().toString());
    assertEquals("2000-04-04 03:30:00.0009", typed.getMaximum().toString());

    // Test serialisation/deserialisation
    OrcProto.ColumnStatistics serial = stats1.serialize().build();
    ColumnStatisticsImpl stats2 =
        ColumnStatisticsImpl.deserialize(schema, serial);
    TimestampColumnStatistics typed2 = (TimestampColumnStatistics) stats2;
    assertEquals("2000-04-01 03:30:00.0005", typed2.getMinimum().toString());
    assertEquals("2000-04-04 03:30:00.0009", typed2.getMaximum().toString());
    assertEquals(4L, typed2.getNumberOfValues());
    TimeZone.setDefault(original);
  }

  @Test
  public void testTimestampNanoPrecisionMerge() {
    TypeDescription schema = TypeDescription.createTimestamp();

    TimeZone original = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
    stats1.updateTimestamp(Timestamp.valueOf("2000-04-02 03:30:00.0001"));
    stats1.updateTimestamp(Timestamp.valueOf( "2000-04-02 01:30:00.0009"));
    stats1.increment(2);

    stats2.updateTimestamp(Timestamp.valueOf( "2000-04-02 01:30:00.00088"));
    stats2.updateTimestamp(Timestamp.valueOf( "2000-04-02 03:30:00.00001"));
    stats2.increment(2);

    TimestampColumnStatistics typed = (TimestampColumnStatistics) stats1;
    assertEquals("2000-04-02 01:30:00.0009", typed.getMinimum().toString());
    assertEquals("2000-04-02 03:30:00.0001", typed.getMaximum().toString());

    TimestampColumnStatistics typed2 = (TimestampColumnStatistics) stats2;
    assertEquals("2000-04-02 03:30:00.00001", typed2.getMaximum().toString());
    assertEquals("2000-04-02 01:30:00.00088", typed2.getMinimum().toString());

    // make sure merge goes down to ns precision
    stats1.merge(stats2);
    assertEquals("2000-04-02 01:30:00.00088", typed.getMinimum().toString());
    assertEquals("2000-04-02 03:30:00.0001", typed.getMaximum().toString());

    stats1.reset();
    assertNull(typed.getMinimum());
    assertNull(typed.getMaximum());

    stats1.updateTimestamp(Timestamp.valueOf( "1999-04-04 00:00:00.000231"));
    stats1.updateTimestamp(Timestamp.valueOf( "2009-03-08 12:00:00.000654"));
    stats1.increment(2);

    stats1.merge(stats2);
    assertEquals("1999-04-04 00:00:00.000231", typed.getMinimum().toString());
    assertEquals("2009-03-08 12:00:00.000654", typed.getMaximum().toString());
    TimeZone.setDefault(original);
  }

  @Test
  public void testNegativeTimestampNanoPrecision() {
    TypeDescription schema = TypeDescription.createTimestamp();

    TimeZone original = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
    stats1.updateTimestamp(Timestamp.valueOf("1960-04-02 03:30:00.0001"));
    stats1.updateTimestamp(Timestamp.valueOf( "1969-12-31 16:00:00.0009"));
    stats1.increment(2);

    stats2.updateTimestamp(Timestamp.valueOf( "1962-04-02 01:30:00.00088"));
    stats2.updateTimestamp(Timestamp.valueOf( "1969-12-31 16:00:00.00001"));
    stats2.increment(2);

    stats1.merge(stats2);

    TimestampColumnStatistics typed = (TimestampColumnStatistics) stats1;
    assertEquals("1960-04-02 03:30:00.0001", typed.getMinimum().toString());
    assertEquals("1969-12-31 16:00:00.0009", typed.getMaximum().toString());

    stats1.reset();
    assertNull(typed.getMinimum());
    assertNull(typed.getMaximum());

    stats1.updateTimestamp(Timestamp.valueOf("1969-12-31 15:00:00.0005"));
    stats1.increment(1);

    assertEquals("1969-12-31 15:00:00.0005", typed.getMinimum().toString());
    assertEquals("1969-12-31 15:00:00.0005", typed.getMaximum().toString());

    stats1.updateTimestamp(Timestamp.valueOf("1969-12-31 15:00:00.00055"));
    stats1.increment(1);

    assertEquals("1969-12-31 15:00:00.0005", typed.getMinimum().toString());
    assertEquals("1969-12-31 15:00:00.00055", typed.getMaximum().toString());
    TimeZone.setDefault(original);
  }

  @Test
  public void testDecimalMerge() throws Exception {
    TypeDescription schema = TypeDescription.createDecimal()
        .withPrecision(38).withScale(16);

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
    stats1.updateDecimal(new HiveDecimalWritable(10));
    stats1.updateDecimal(new HiveDecimalWritable(100));
    stats2.updateDecimal(new HiveDecimalWritable(1));
    stats2.updateDecimal(new HiveDecimalWritable(1000));
    stats1.merge(stats2);
    DecimalColumnStatistics typed = (DecimalColumnStatistics) stats1;
    assertEquals(1, typed.getMinimum().longValue());
    assertEquals(1000, typed.getMaximum().longValue());
    stats1.reset();
    stats1.updateDecimal(new HiveDecimalWritable(-10));
    stats1.updateDecimal(new HiveDecimalWritable(10000));
    stats1.merge(stats2);
    assertEquals(-10, typed.getMinimum().longValue());
    assertEquals(10000, typed.getMaximum().longValue());
  }

  @Test
  public void testDecimalMinMaxStatistics() throws Exception {
    TypeDescription schema = TypeDescription.fromString("decimal(7,2)");

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema));
    VectorizedRowBatch batch = schema.createRowBatch();
    DecimalColumnVector decimalColumnVector = (DecimalColumnVector) batch.cols[0];
    batch.size = 2;

    decimalColumnVector.set(0, new HiveDecimalWritable("-99999.99"));
    decimalColumnVector.set(1, new HiveDecimalWritable("-88888.88"));
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    DecimalColumnStatistics statistics = (DecimalColumnStatistics) reader.getStatistics()[0];
    assertEquals(new BigDecimal("-99999.99"), statistics.getMinimum().bigDecimalValue(),
        "Incorrect maximum value");
    assertEquals(new BigDecimal("-88888.88"), statistics.getMaximum().bigDecimalValue(),
        "Incorrect minimum value");
  }


  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  Configuration conf;
  FileSystem fs;
  Path testFilePath;

  @BeforeEach
  public void openFileSystem(TestInfo testInfo) throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    fs.setWorkingDirectory(workDir);
    testFilePath = new Path(
        "TestOrcFile." + testInfo.getTestMethod().get().getName() + ".orc");
    fs.delete(testFilePath, false);
  }
}
