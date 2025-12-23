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

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Interface for writing integers.
 */
public interface IntegerWriter {

  /**
   * Get position from the stream.
   * @param recorder
   * @throws IOException
   */
  void getPosition(PositionRecorder recorder) throws IOException;

  /**
   * Write the integer value
   * @param value
   * @throws IOException
   */
  void write(long value) throws IOException;

  /**
   * Flush the buffer
   * @throws IOException
   */
  void flush() throws IOException;

  /**
   * Estimate the amount of memory being used.
   * @return number of bytes
   */
  long estimateMemory();

  void changeIv(Consumer<byte[]> modifier);
}
