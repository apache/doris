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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;

import java.io.IOException;

/**
 * Interface for reading integers.
 */
public interface IntegerReader {

  /**
   * Seek to the position provided by index.
   * @param index
   * @throws IOException
   */
  void seek(PositionProvider index) throws IOException;

  /**
   * Skip number of specified rows.
   * @param numValues
   * @throws IOException
   */
  void skip(long numValues) throws IOException;

  /**
   * Check if there are any more values left.
   * @return
   * @throws IOException
   */
  boolean hasNext() throws IOException;

  /**
   * Return the next available value.
   * @return
   * @throws IOException
   */
  long next() throws IOException;

  /**
   * Return the next available vector for values.
   * @param column the column being read
   * @param data the vector to read into
   * @param length the number of numbers to read
   * @throws IOException
   */
  void nextVector(ColumnVector column,
                  long[] data,
                  int length
                  ) throws IOException;

  /**
   * Return the next available vector for values. Does not change the
   * value of column.isRepeating.
   * @param column the column being read
   * @param data the vector to read into
   * @param length the number of numbers to read
   * @throws IOException
   */
  void nextVector(ColumnVector column,
                  int[] data,
                  int length
                  ) throws IOException;
}
