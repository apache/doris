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
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.orc.impl.mask;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSHA256Mask {

  final byte[] inputLong = (
      "Lorem ipsum dolor sit amet, consectetur adipiscing "
          + "elit. Curabitur quis vehicula ligula. In hac habitasse platea dictumst."
          + " Curabitur mollis finibus erat fringilla vestibulum. In eu leo eget"
          + " massa luctus convallis nec vitae ligula. Donec vitae diam convallis,"
          + " efficitur orci in, imperdiet turpis. In quis semper ex. Duis faucibus "
          + "tellus vitae molestie convallis. Fusce fermentum vestibulum lacus "
          + "vel malesuada. Pellentesque viverra odio a justo aliquet tempus.")
      .getBytes(StandardCharsets.UTF_8);

  final byte[] input32 = "Every flight begins with a fall."
      .getBytes(StandardCharsets.UTF_8);

  final byte[] inputShort = "\uD841\uDF0E".getBytes(StandardCharsets.UTF_8);

  final MessageDigest md;

  final byte[] expectedHash32 ;
  final byte[] expectedHashShort ;
  final byte[] expectedHashLong ;

  final byte[] expectedHash32_hex ;
  final byte[] expectedHashShort_hex ;
  final byte[] expectedHashLong_hex ;

  public TestSHA256Mask() {
    super();
    try {
      md = MessageDigest.getInstance("SHA-256");

      expectedHash32 = md.digest(input32);
      expectedHashShort = md.digest(inputShort);
      expectedHashLong = md.digest(inputLong);

      expectedHash32_hex = SHA256MaskFactory.printHexBinary(expectedHash32).getBytes(StandardCharsets.UTF_8);
      expectedHashShort_hex = SHA256MaskFactory.printHexBinary(expectedHashShort).getBytes(StandardCharsets.UTF_8);
      expectedHashLong_hex = SHA256MaskFactory.printHexBinary(expectedHashLong).getBytes(StandardCharsets.UTF_8);

    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Test to make sure that the output is always 64 bytes (equal to hash len) <br>
   * This is because String type does not have bounds on length.
   *
   * @throws Exception
   */
  @Test
  public void testStringSHA256Masking() throws Exception {
    final SHA256MaskFactory sha256Mask = new SHA256MaskFactory();
    final BytesColumnVector source = new BytesColumnVector();
    final BytesColumnVector target = new BytesColumnVector();

    target.reset();

    source.setRef(0, input32, 0, input32.length);
    source.setRef(1, inputShort, 0, inputShort.length);

    for (int r = 0; r < 2; ++r) {
      sha256Mask.maskString(source, r, target, TypeDescription.createString());
    }

    /* Make sure the the mask length is equal to 64 length of SHA-256 */
    assertEquals(64, target.length[0]);
    assertEquals(64, target.length[1]);

    /* gather the results into an array to compare */
    byte[] reasultInput32 = new byte[target.length[0]];
    System.arraycopy(target.vector[0], target.start[0], reasultInput32, 0, target.length[0]);
    byte[] reasultInputShort = new byte[target.length[1]];
    System.arraycopy(target.vector[1], target.start[1], reasultInputShort, 0, target.length[1]);

    /* prepare the expected byte[] to compare */
    final byte[] expected1 = new byte[target.length[0]];
    System.arraycopy(expectedHash32_hex, 0, expected1, 0, target.length[0]);
    final byte[] expected2 = new byte[target.length[1]];
    System.arraycopy(expectedHashShort_hex, 0, expected2, 0, target.length[1]);

    /* Test the actual output. NOTE: our varchar has max length 32 */
    assertArrayEquals(expected1, reasultInput32);
    assertArrayEquals(expected2, reasultInputShort);

  }

  /**
   * Test to make sure that the length of input is equal to the output. <br>
   * If input is shorter than the hash, truncate it. <br>
   * If the input is larger than hash (64) pad it with blank space. <br>
   *
   * @throws Exception
   */
  @Test
  public void testChar256Masking() throws Exception {
    final SHA256MaskFactory sha256Mask = new SHA256MaskFactory();
    final BytesColumnVector source = new BytesColumnVector();
    final BytesColumnVector target = new BytesColumnVector();

    target.reset();

    int[] length = new int[3];

    length[0] = input32.length;
    source.setRef(0, input32, 0, input32.length);

    length[1] = inputShort.length;
    source.setRef(1, inputShort, 0, inputShort.length);

    length[2] = inputLong.length;
    source.setRef(2, inputLong, 0, inputLong.length);

    for (int r = 0; r < 3; ++r) {
      sha256Mask.maskString(source, r, target,
          TypeDescription.createChar().withMaxLength(length[r]));
    }

    /* Make sure the the mask length is equal to 64 length of SHA-256 */
    assertEquals(length[0], target.length[0]);
    assertEquals(length[1], target.length[1]);
    assertEquals(length[2], target.length[2]);

  }

  @Test
  public void testVarChar256Masking() throws Exception {
    final SHA256MaskFactory sha256Mask = new SHA256MaskFactory();
    final BytesColumnVector source = new BytesColumnVector();
    final BytesColumnVector target = new BytesColumnVector();

    target.reset();

    source.setRef(0, input32, 0, input32.length);
    source.setRef(1, inputShort, 0, inputShort.length);
    source.setRef(2, inputLong, 0, inputLong.length);

    for (int r = 0; r < 3; ++r) {
      sha256Mask.maskString(source, r, target,
          TypeDescription.createVarchar().withMaxLength(32));
    }

    /* gather the results into an array to compare */
    byte[] reasultInput32 = new byte[target.length[0]];
    System.arraycopy(target.vector[0], target.start[0], reasultInput32, 0, target.length[0]);
    byte[] reasultInputShort = new byte[target.length[1]];
    System.arraycopy(target.vector[1], target.start[1], reasultInputShort, 0, target.length[1]);
    byte[] reasultInputLong = new byte[target.length[2]];
    System.arraycopy(target.vector[2], target.start[2], reasultInputLong, 0, target.length[2]);


    /* prepare the expected byte[] to compare */
    final byte[] expected1 = new byte[target.length[0]];
    System.arraycopy(expectedHash32_hex, 0, expected1, 0, target.length[0]);
    final byte[] expected2 = new byte[target.length[1]];
    System.arraycopy(expectedHashShort_hex, 0, expected2, 0, target.length[1]);
    final byte[] expected3 = new byte[target.length[2]];
    System.arraycopy(expectedHashLong_hex, 0, expected3, 0, target.length[2]);


    // Hash is 64 in length greater than max len 32, so make sure output length is 32
    assertEquals(32, target.length[0]);
    assertEquals(32, target.length[1]);
    assertEquals(32, target.length[2]);

    /* Test the actual output. NOTE: our varchar has max length 32 */
    assertArrayEquals(expected1, reasultInput32);
    assertArrayEquals(expected2, reasultInputShort);
    assertArrayEquals(expected3, reasultInputLong);

    for (int r = 0; r < 3; ++r) {
      sha256Mask.maskString(source, r, target,
          TypeDescription.createVarchar().withMaxLength(100));
    }

    /* gather the results into an array to compare */
    reasultInput32 = new byte[target.length[0]];
    System.arraycopy(target.vector[0], target.start[0], reasultInput32, 0, target.length[0]);
    reasultInputShort = new byte[target.length[1]];
    System.arraycopy(target.vector[1], target.start[1], reasultInputShort, 0, target.length[1]);
    reasultInputLong = new byte[target.length[2]];
    System.arraycopy(target.vector[2], target.start[2], reasultInputLong, 0, target.length[2]);

    /* Hash is 64 in length, less than max len 100 so the outpur will always be 64 */
    assertEquals(64, target.length[0]);
    assertEquals(64, target.length[1]);
    assertEquals(64, target.length[2]);

    /* Test the actual output */
    assertArrayEquals(expectedHash32_hex, reasultInput32);
    assertArrayEquals(expectedHashShort_hex, reasultInputShort);
    assertArrayEquals(expectedHashLong_hex, reasultInputLong);

  }

  @Test
  public void testBinary() {

    final SHA256MaskFactory sha256Mask = new SHA256MaskFactory();
    final BytesColumnVector source = new BytesColumnVector();
    final BytesColumnVector target = new BytesColumnVector();

    target.reset();

    source.setRef(0, input32, 0, input32.length);
    source.setRef(1, inputShort, 0, inputShort.length);
    source.setRef(2, inputLong, 0, inputLong.length);

    for (int r = 0; r < 3; ++r) {
      sha256Mask.maskBinary(source, r, target);
    }

    final byte[] reasultInput32 = ByteBuffer.wrap(target.vector[0], target.start[0], target.length[0]).array();
    final byte[] reasultInputShort = ByteBuffer.wrap(target.vector[1], target.start[1], target.length[1]).array();
    final byte[] reasultInputLong = ByteBuffer.wrap(target.vector[2], target.start[2], target.length[2]).array();


    /* Hash is 32 in length (length in binary), less than max len 100 so the output will always be 32 */
    assertEquals(32, target.length[0]);
    assertEquals(32, target.length[1]);
    assertEquals(32, target.length[2]);

    assertArrayEquals(expectedHash32, reasultInput32);
    assertArrayEquals(expectedHashShort, reasultInputShort);
    assertArrayEquals(expectedHashLong, reasultInputLong);


  }

}
