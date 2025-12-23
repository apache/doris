package org.apache.orc.impl.mask;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test Unmask option
 */
public class TestUnmaskRange {

  public TestUnmaskRange() {
    super();
  }

  /* Test for Long */
  @Test
  public void testSimpleLongRangeMask() {
    RedactMaskFactory mask = new RedactMaskFactory("9", "", "0:2");
    long result = mask.maskLong(123456);
    assertEquals(123_999, result);
    assertEquals(-129_999, mask.maskLong(-123456));

    // negative index
    mask = new RedactMaskFactory("9", "", "-3:-1");
    result = mask.maskLong(123456);
    assertEquals(999_456, result);
    assertEquals(-999_456, mask.maskLong(-123456));

    // out of range mask, return the original mask
    mask = new RedactMaskFactory("9", "", "7:10");
    result = mask.maskLong(123456);
    assertEquals(999999, result);

    // if the masked value overflows, we get the overflow answer
    result = mask.maskLong(1_234_567_890_123_456_789L);
    assertEquals(999_999_999_999_999_999L, result);
  }

  @Test
  public void testDefaultRangeMask() {
    RedactMaskFactory mask = new RedactMaskFactory("9", "", "");
    long result = mask.maskLong(123456);
    assertEquals(999999, result);

    mask = new RedactMaskFactory("9");
    result = mask.maskLong(123456);
    assertEquals(999999, result);

  }

  @Test
  public void testCCRangeMask() {
    long cc = 4716885592186382L;
    long maskedCC = 4716_77777777_6382L;
    // Range unmask for first 4 and last 4 of credit card number
    final RedactMaskFactory mask = new RedactMaskFactory("Xx7", "", "0:3,-4:-1");
    long result = mask.maskLong(cc);

    assertEquals(String.valueOf(cc).length(), String.valueOf(result).length());
    assertEquals(4716_77777777_6382L, result);
  }

  /* Tests for Double */
  @Test
  public void testSimpleDoubleRangeMask() {
    RedactMaskFactory mask = new RedactMaskFactory("Xx7", "", "0:2");
    assertEquals(1237.77, mask.maskDouble(1234.99), 0.000001);
    assertEquals(12377.7, mask.maskDouble(12345.9), 0.000001);

    mask = new RedactMaskFactory("Xx7", "", "-3:-1");
    assertEquals(7774.9, mask.maskDouble(1234.9), 0.000001);

  }

  /* test for String */
  @Test
  public void testStringRangeMask() {

    BytesColumnVector source = new BytesColumnVector();
    BytesColumnVector target = new BytesColumnVector();
    target.reset();

    byte[] input = "Mary had 1 little lamb!!".getBytes(StandardCharsets.UTF_8);
    source.setRef(0, input, 0, input.length);

    // Set a 4 byte chinese character (U+2070E), which is letter other
    input = "\uD841\uDF0E".getBytes(StandardCharsets.UTF_8);
    source.setRef(1, input, 0, input.length);

    RedactMaskFactory mask = new RedactMaskFactory("", "", "0:3, -5:-1");
    for(int r=0; r < 2; ++r) {
      mask.maskString(source, r, target);
    }

    assertEquals("Mary xxx 9 xxxxxx xamb!!", new String(target.vector[0],
        target.start[0], target.length[0], StandardCharsets.UTF_8));
    assertEquals("\uD841\uDF0E", new String(target.vector[1],
        target.start[1], target.length[1], StandardCharsets.UTF_8));

    // test defaults, no-unmask range
    mask = new RedactMaskFactory();
    for(int r=0; r < 2; ++r) {
      mask.maskString(source, r, target);
    }

    assertEquals("Xxxx xxx 9 xxxxxx xxxx..", new String(target.vector[0],
        target.start[0], target.length[0], StandardCharsets.UTF_8));
    assertEquals("ª", new String(target.vector[1],
        target.start[1], target.length[1], StandardCharsets.UTF_8));


    // test out of range string mask
    mask = new RedactMaskFactory("", "", "-1:-5");
    for(int r=0; r < 2; ++r) {
      mask.maskString(source, r, target);
    }

    assertEquals("Xxxx xxx 9 xxxxxx xxxx..", new String(target.vector[0],
        target.start[0], target.length[0], StandardCharsets.UTF_8));
    assertEquals("ª", new String(target.vector[1],
        target.start[1], target.length[1], StandardCharsets.UTF_8));

  }

  /* test for Decimal */
  @Test
  public void testDecimalRangeMask() {

    RedactMaskFactory mask = new RedactMaskFactory("Xx7", "", "0:3");
    assertEquals(new HiveDecimalWritable("123477.777"),
        mask.maskDecimal(new HiveDecimalWritable("123456.789")));

    // try with a reverse index
    mask = new RedactMaskFactory("Xx7", "", "-3:-1, 0:3");
    assertEquals(new HiveDecimalWritable("123477777.777654"),
        mask.maskDecimal(new HiveDecimalWritable("123456789.987654")));

  }

}
