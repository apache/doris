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

import org.apache.hadoop.io.Text;
import org.apache.orc.StringDictTestingUtils;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestStringHashTableDictionary {

  /**
   * Basic test cases by adding bytes directly and uses real hash function.
   */
  @Test
  public void test0()
      throws Exception {
    StringHashTableDictionary htDict = new StringHashTableDictionary(5);

    List<Text> testTexts =
        Stream.of(new String[]{"Alice", "Bob", "Cindy", "David", "Eason"}).map(Text::new).collect(Collectors.toList());
    List<byte[]> testBytes = testTexts.stream().map(Text::getBytes).collect(Collectors.toList());

    assertEquals(0, htDict.getSizeInBytes());
    assertEquals(0, htDict.add(testBytes.get(0), 0, testBytes.get(0).length));
    assertEquals(1, htDict.add(testBytes.get(1), 0, testBytes.get(1).length));
    assertEquals(0, htDict.add(testBytes.get(0), 0, testBytes.get(0).length));
    assertEquals(1, htDict.add(testBytes.get(1), 0, testBytes.get(1).length));
    assertEquals(2, htDict.add(testBytes.get(2), 0, testBytes.get(2).length));

    Text text = new Text();
    htDict.getText(text, 0);
    assertEquals("Alice", text.toString());
    htDict.getText(text, 1);
    assertEquals("Bob", text.toString());
    htDict.getText(text, 2);
    assertEquals("Cindy", text.toString());

    assertEquals(htDict.size(), 3);

    // entering the fourth and fifth element which triggers rehash
    assertEquals(3, htDict.add(testBytes.get(3), 0, testBytes.get(3).length));
    htDict.getText(text, 3);
    assertEquals("David", text.toString());
    assertEquals(4, htDict.add(testBytes.get(4), 0, testBytes.get(4).length));
    htDict.getText(text, 4);
    assertEquals("Eason", text.toString());

    assertEquals(htDict.size(), 5);

    // Re-ensure no all previously existed string still have correct encoded value
    htDict.getText(text, 0);
    assertEquals("Alice", text.toString());
    htDict.getText(text, 1);
    assertEquals("Bob", text.toString());
    htDict.getText(text, 2);
    assertEquals("Cindy", text.toString());

    // Peaking the hashtable and obtain the order sequence since the hashArray object needs to be private.
    StringDictTestingUtils.checkContents(htDict, new int[]{1, 2, 3, 0 ,4}, "Bob", "Cindy", "David", "Alice", "Eason");

    htDict.clear();
    assertEquals(0, htDict.size());
  }

  /**
   * A extension for {@link StringHashTableDictionary} for testing purpose by overwriting the hash function,
   * just to save the effort of obtaining order sequence manually.
   */
  private static class SimpleHashDictionary extends StringHashTableDictionary {
    public SimpleHashDictionary(int initialCapacity) {
      super(initialCapacity);
    }

    /**
     * Obtain the prefix for each string as the hash value.
     * All the string being used in this test suite will contains its hash value as the prefix for the string content.
     * this way we know the order of the traverse() method.
     */
    @Override
    int getIndex(byte[] bytes, int offset, int length) {
      return (char) bytes[0] - '0';
    }
  }

  @Test
  public void test1()
      throws Exception {
    SimpleHashDictionary hashTableDictionary = new SimpleHashDictionary(5);
    // Non-resize trivial cases
    assertEquals(0, hashTableDictionary.getSizeInBytes());
    assertEquals(0, hashTableDictionary.add(new Text("2_Alice")));
    assertEquals(1, hashTableDictionary.add(new Text("3_Bob")));
    assertEquals(0, hashTableDictionary.add(new Text("2_Alice")));
    assertEquals(1, hashTableDictionary.add(new Text("3_Bob")));
    assertEquals(2, hashTableDictionary.add(new Text("1_Cindy")));

    Text text = new Text();
    hashTableDictionary.getText(text, 0);
    assertEquals("2_Alice", text.toString());
    hashTableDictionary.getText(text, 1);
    assertEquals("3_Bob", text.toString());
    hashTableDictionary.getText(text, 2);
    assertEquals("1_Cindy", text.toString());

    // entering the fourth and fifth element which triggers rehash
    assertEquals(3, hashTableDictionary.add(new Text("0_David")));
    hashTableDictionary.getText(text, 3);
    assertEquals("0_David", text.toString());
    assertEquals(4, hashTableDictionary.add(new Text("4_Eason")));
    hashTableDictionary.getText(text, 4);
    assertEquals("4_Eason", text.toString());

    // Re-ensure no all previously existed string still have correct encoded value
    hashTableDictionary.getText(text, 0);
    assertEquals("2_Alice", text.toString());
    hashTableDictionary.getText(text, 1);
    assertEquals("3_Bob", text.toString());
    hashTableDictionary.getText(text, 2);
    assertEquals("1_Cindy", text.toString());

    // The order of words are based on each string's prefix given their index in the hashArray will be based on that.
    StringDictTestingUtils
        .checkContents(hashTableDictionary, new int[]{3, 2, 0, 1, 4}, "0_David", "1_Cindy", "2_Alice", "3_Bob",
            "4_Eason");

    hashTableDictionary.clear();
    assertEquals(0, hashTableDictionary.size());
  }
}
