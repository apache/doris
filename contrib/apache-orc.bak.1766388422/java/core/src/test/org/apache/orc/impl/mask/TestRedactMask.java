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
package org.apache.orc.impl.mask;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestRedactMask {

  @Test
  public void testSimpleReplaceLongDigits() throws Exception {
    RedactMaskFactory mask = new RedactMaskFactory("Xx7");
    assertEquals(7, mask.maskLong(0));
    assertEquals(7, mask.maskLong(9));
    assertEquals(-7, mask.maskLong(-9));
    assertEquals(-7, mask.maskLong(-1));
    assertEquals(77, mask.maskLong(10));
    assertEquals(-77, mask.maskLong(-10));
    assertEquals(7_777_777_777_777_777_777L,
        mask.maskLong(Long.MAX_VALUE));
    assertEquals(-7_777_777_777_777_777_777L,
        mask.maskLong(Long.MIN_VALUE + 1));
    assertEquals(-7_777_777_777_777_777_777L,
        mask.maskLong(Long.MIN_VALUE));
  }

  @Test
  public void testPow10ReplaceLongDigits() throws Exception {
    for(int digit=0; digit < 10; ++digit) {
      RedactMaskFactory mask = new RedactMaskFactory("Xx" + digit);
      long expected = digit;
      long input = 1;
      for(int i=0; i < 19; ++i) {
        // 9_999_999_999_999_999_999 is bigger than 2**63, so it overflows.
        // The routine uses one less digit for that case.
        if (i == 18 && digit == 9) {
          expected = 999_999_999_999_999_999L;
        }
        assertEquals(expected, mask.maskLong(input),
            "digit " + digit + " value " + input);
        assertEquals(expected, mask.maskLong(5 * input),
            "digit " + digit + " value " + (5 * input));
        assertEquals(expected, mask.maskLong(9 * input),
            "digit " + digit + " value " + (9 * input));
        expected = expected * 10 + digit;
        input *= 10;
      }
    }
  }

  @Test
  public void testSimpleReplaceDoubleDigits() throws Exception {
    RedactMaskFactory mask = new RedactMaskFactory("Xx7");
    assertEquals(7.77777, mask.maskDouble(0.0), 0.000001);
    assertEquals(7.77777, mask.maskDouble(9.9), 0.000001);
    assertEquals(-7.77777, mask.maskDouble(-9.9), 0.000001);
    assertEquals(-7.77777, mask.maskDouble(-1.0), 0.000001);
    assertEquals(77.7777, mask.maskDouble(10.0), 0.000001);
    assertEquals(-77.7777, mask.maskDouble(-10.0), 0.000001);
    assertEquals(7_777_770_000_000_000_000.0,
        mask.maskDouble(Long.MAX_VALUE), 0.000001);
    assertEquals(-7_777_770_000_000_000_000.0,
        mask.maskDouble(Long.MIN_VALUE), 0.000001);
    assertEquals(7.77777e-308,
        mask.maskDouble(Double.MIN_NORMAL), 1e-310);
    assertEquals(7.77777e307,
        mask.maskDouble(Double.MAX_VALUE), 1e299);

    // change to mask of 1
    mask = new RedactMaskFactory("Xx1");
    assertEquals(-1.11111e-308,
        mask.maskDouble(-Double.MIN_NORMAL), 1e-310);

    // change to mask of 9
    mask = new RedactMaskFactory();
    assertEquals(-9.99999e307,
        mask.maskDouble(-Double.MAX_VALUE), 1e299);
  }

  @Test
  public void testSimpleMaskTimestamp() throws Exception {
    RedactMaskFactory mask = new RedactMaskFactory();
    Timestamp ts = Timestamp.valueOf("2011-10-02 18:48:05.123456");
    assertEquals("2011-01-01 00:00:00.0",
        new Timestamp(mask.maskTime(ts.getTime())).toString());
    ts = Timestamp.valueOf("2012-02-28 01:23:45");
    assertEquals("2012-01-01 00:00:00.0",
        new Timestamp(mask.maskTime(ts.getTime())).toString());
    ts = Timestamp.valueOf("2017-05-18 01:23:45");
    assertEquals("2017-01-01 00:00:00.0",
        new Timestamp(mask.maskTime(ts.getTime())).toString());
    mask = new RedactMaskFactory("", "2000 _ _ 15 0 _");
    assertEquals("2000-05-18 15:00:45.0",
        new Timestamp(mask.maskTime(ts.getTime())).toString());
    mask = new RedactMaskFactory("", "2000 _ _ 15 0 _");
    assertEquals("2000-05-18 15:00:45.0",
        new Timestamp(mask.maskTime(ts.getTime())).toString());
    mask = new RedactMaskFactory("", "2007 _ _ _ _ _");
    assertEquals("2007-05-18 01:23:45.0",
        new Timestamp(mask.maskTime(ts.getTime())).toString());
    mask = new RedactMaskFactory("", "_ 7 _ _ _ _");
    assertEquals("2017-07-18 01:23:45.0",
        new Timestamp(mask.maskTime(ts.getTime())).toString());
    mask = new RedactMaskFactory("", "_ _ 7 _ _ _");
    assertEquals("2017-05-07 01:23:45.0",
        new Timestamp(mask.maskTime(ts.getTime())).toString());
    mask = new RedactMaskFactory("", "_ _ _ 7 _ _");
    assertEquals("2017-05-18 07:23:45.0",
        new Timestamp(mask.maskTime(ts.getTime())).toString());
    mask = new RedactMaskFactory("", "_ _ _ _ 7 _");
    assertEquals("2017-05-18 01:07:45.0",
        new Timestamp(mask.maskTime(ts.getTime())).toString());
    mask = new RedactMaskFactory("", "_ _ _ _ _ 7");
    assertEquals("2017-05-18 01:23:07.0",
        new Timestamp(mask.maskTime(ts.getTime())).toString());
  }

  @Test
  public void testSimpleMaskDate() throws Exception {
    RedactMaskFactory mask = new RedactMaskFactory();
    DateWritable date = new DateWritable(Date.valueOf("1965-03-12"));
    assertEquals("1965-01-01",
        new DateWritable(mask.maskDate(date.getDays())).toString());
    mask = new RedactMaskFactory("", "2000 _ _");
    assertEquals("2000-03-12",
        new DateWritable(mask.maskDate(date.getDays())).toString());
    mask = new RedactMaskFactory(new String[]{"", "_ 7 _"});
    assertEquals("1965-07-12",
        new DateWritable(mask.maskDate(date.getDays())).toString());
    mask = new RedactMaskFactory("", "_ _ 7");
    assertEquals("1965-03-07",
        new DateWritable(mask.maskDate(date.getDays())).toString());
    date = new DateWritable(Date.valueOf("2017-09-20"));
    assertEquals("2017-09-07",
        new DateWritable(mask.maskDate(date.getDays())).toString());
  }

  @Test
  public void testSimpleMaskDecimal() throws Exception {
    RedactMaskFactory mask = new RedactMaskFactory("Xx7");
    assertEquals(new HiveDecimalWritable("777.777"),
        mask.maskDecimal(new HiveDecimalWritable("123.456")));
    // test removal of leading and  trailing zeros.
    assertEquals(new HiveDecimalWritable("777777777777777777.7777"),
        mask.maskDecimal(new HiveDecimalWritable("0123456789123456789.01230")));
  }

  @Test
  public void testReplacements() throws Exception {
    RedactMaskFactory mask = new RedactMaskFactory("1234567890");
    assertEquals("1".codePointAt(0), mask.getReplacement("X".codePointAt(0)));
    assertEquals("2".codePointAt(0), mask.getReplacement("x".codePointAt(0)));
    assertEquals("3".codePointAt(0), mask.getReplacement("0".codePointAt(0)));
    assertEquals("4".codePointAt(0), mask.getReplacement("$".codePointAt(0)));
    assertEquals("5".codePointAt(0), mask.getReplacement(".".codePointAt(0)));
    assertEquals("6".codePointAt(0), mask.getReplacement(" ".codePointAt(0)));
    assertEquals("7".codePointAt(0), mask.getReplacement("ה".codePointAt(0)));
    assertEquals("8".codePointAt(0), mask.getReplacement("ी".codePointAt(0)));
    assertEquals("9".codePointAt(0), mask.getReplacement("ↂ".codePointAt(0)));
    assertEquals("0".codePointAt(0), mask.getReplacement("\u06DD".codePointAt(0)));
    mask = new RedactMaskFactory();
    assertEquals("_".codePointAt(0), mask.getReplacement(" ".codePointAt(0)));
  }

  @Test
  public void testStringMasking() throws Exception {
    RedactMaskFactory mask = new RedactMaskFactory();
    BytesColumnVector source = new BytesColumnVector();
    BytesColumnVector target = new BytesColumnVector();
    target.reset();
    byte[] input = "Mary had 1 little lamb!!".getBytes(StandardCharsets.UTF_8);
    source.setRef(0, input, 0, input.length);

    // Set a 4 byte chinese character (U+2070E), which is letter other
    input = "\uD841\uDF0E".getBytes(StandardCharsets.UTF_8);
    source.setRef(1, input, 0, input.length);
    for(int r=0; r < 2; ++r) {
      mask.maskString(source, r, target);
    }
    assertEquals("Xxxx xxx 9 xxxxxx xxxx..", new String(target.vector[0],
        target.start[0], target.length[0], StandardCharsets.UTF_8));
    assertEquals("ª", new String(target.vector[1],
        target.start[1], target.length[1], StandardCharsets.UTF_8));
  }

  @Test
  public void testStringMaskBufferOverflow() throws Exception {
    // set upper and lower letters to replace with 4 byte replacements
    // (U+267CC and U+28CCA)
    RedactMaskFactory mask = new RedactMaskFactory("\uD859\uDFCC\uD863\uDCCA");
    BytesColumnVector source = new BytesColumnVector();
    BytesColumnVector target = new BytesColumnVector();
    target.reset();

    // Set the input to 1024 copies of the input string.
    // input is 14 bytes * 1024 = 14336 bytes
    // output is (4 * 12 + 1 * 2) * 1024 = 51200 bytes
    byte[] input = "text overflow."
        .getBytes(StandardCharsets.UTF_8);
    for(int r=0; r < 1024; ++r) {
      source.setRef(r, input, 0, input.length);
    }
    for(int r=0; r < 1024; ++r) {
      mask.maskString(source, r, target);
    }

    // should have doubled twice to 64k
    assertEquals(64*1024, target.getValPreallocatedBytes().length);

    // Make sure all of the translations are correct
    String expected ="\uD863\uDCCA\uD863\uDCCA\uD863\uDCCA\uD863\uDCCA" +
        " \uD863\uDCCA\uD863\uDCCA\uD863\uDCCA\uD863\uDCCA" +
        "\uD863\uDCCA\uD863\uDCCA\uD863\uDCCA\uD863\uDCCA.";
    for(int r=0; r < 1024; ++r) {
      assertEquals(expected,
          new String(target.vector[r], target.start[r], target.length[r],
              StandardCharsets.UTF_8), "r = " + r);
    }

    // Make sure that the target keeps the larger output buffer.
    target.reset();
    assertEquals(64*1024, target.getValPreallocatedBytes().length);
  }
}
