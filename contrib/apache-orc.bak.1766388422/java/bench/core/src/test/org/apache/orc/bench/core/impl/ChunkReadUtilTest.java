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

package org.apache.orc.bench.core.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChunkReadUtilTest {
  private static final Path workDir = new Path(System.getProperty("test.tmp.dir",
                                                                  "target" + File.separator + "test"
                                                                  + File.separator + "tmp"));
  private static final Path filePath = new Path(workDir, "chunk_read_file.orc");
  private static long fileLength;
  private static final int ROW_COUNT = 524288;
  private static final int COL_COUNT = 16;

  @BeforeAll
  public static void setup() throws IOException {
    fileLength = ChunkReadUtil.createORCFile(COL_COUNT, ROW_COUNT, filePath);
  }

  private static void readStart() {
    FileSystem.clearStatistics();
  }

  private static FileSystem.Statistics readEnd() {
    return FileSystem.getAllStatistics().get(0);
  }

  @Test
  public void testReadAll() throws IOException {
    Configuration conf = new Configuration();
    readStart();
    assertEquals(ROW_COUNT, ChunkReadUtil.readORCFile(filePath, conf, false));
    assertTrue((readEnd().getBytesRead() / (double) fileLength) > 1);
  }

  @Test
  public void testReadAlternate() throws IOException {
    Configuration conf = new Configuration();
    readStart();
    assertEquals(ROW_COUNT, ChunkReadUtil.readORCFile(filePath, conf, true));
    assertTrue((readEnd().getBytesRead() / (double) fileLength) < .5);
  }

  @Test
  public void testReadAlternateWMinSeekSize() throws IOException {
    Configuration conf = new Configuration();
    ChunkReadUtil.setConf(conf, 4 * 1024 * 1024, 10);
    readStart();
    assertEquals(ROW_COUNT, ChunkReadUtil.readORCFile(filePath, conf, true));
    double readFraction = readEnd().getBytesRead() / (double) fileLength;
    assertTrue(readFraction > 1 && readFraction < 1.01);
  }

  @Test
  public void testReadAlternateWMinSeekSizeDrop() throws IOException {
    Configuration conf = new Configuration();
    ChunkReadUtil.setConf(conf, 4 * 1024 * 1024, 0);
    readStart();
    assertEquals(ROW_COUNT, ChunkReadUtil.readORCFile(filePath, conf, true));
    double readFraction = readEnd().getBytesRead() / (double) fileLength;
    assertTrue(readFraction > 1 && readFraction < 1.01);
  }
}