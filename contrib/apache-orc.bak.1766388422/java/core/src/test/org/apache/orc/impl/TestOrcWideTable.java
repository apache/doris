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

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestOrcWideTable {

  @Test
  public void testBufferSizeFor1Col() throws IOException {
    assertEquals(128 * 1024, WriterImpl.getEstimatedBufferSize(512 * 1024 * 1024,
        1, 128*1024));
  }

  @Test
  public void testBufferSizeFor50Col() throws IOException {
    assertEquals(256 * 1024, WriterImpl.getEstimatedBufferSize(256 * 1024 * 1024,
        50, 256*1024));
  }

  @Test
  public void testBufferSizeFor1000Col() throws IOException {
    assertEquals(32 * 1024, WriterImpl.getEstimatedBufferSize(512 * 1024 * 1024,
        1000, 128*1024));
  }

  @Test
  public void testBufferSizeFor2000Col() throws IOException {
    assertEquals(16 * 1024, WriterImpl.getEstimatedBufferSize(512 * 1024 * 1024,
        2000, 256*1024));
  }

  @Test
  public void testBufferSizeFor4000Col() throws IOException {
    assertEquals(8 * 1024, WriterImpl.getEstimatedBufferSize(512 * 1024 * 1024,
        4000, 256*1024));
  }

  @Test
  public void testBufferSizeFor25000Col() throws IOException {
    assertEquals(4 * 1024, WriterImpl.getEstimatedBufferSize(512 * 1024 * 1024,
        25000, 256*1024));
  }
}
