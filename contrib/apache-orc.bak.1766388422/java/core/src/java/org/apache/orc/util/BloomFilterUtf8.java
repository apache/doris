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

package org.apache.orc.util;

import java.nio.charset.StandardCharsets;

/**
 * This class represents the fix from ORC-101 where we fixed the bloom filter
 * from using the JVM's default character set to always using UTF-8.
 */
public class BloomFilterUtf8 extends BloomFilter {

  public BloomFilterUtf8(long expectedEntries, double fpp) {
    super(expectedEntries, fpp);
  }

  public BloomFilterUtf8(long[] bits, int numFuncs) {
    super(bits, numFuncs);
  }


  @Override
  public void addString(String val) {
    if (val == null) {
      add(null);
    } else {
      add(val.getBytes(StandardCharsets.UTF_8));
    }
  }

  @Override
  public boolean testString(String val) {
    if (val == null) {
      return test(null);
    } else {
      return test(val.getBytes(StandardCharsets.UTF_8));
    }
  }
}
