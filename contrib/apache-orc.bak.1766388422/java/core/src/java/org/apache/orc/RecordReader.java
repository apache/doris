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

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import java.io.Closeable;
import java.io.IOException;

/**
 * A row-by-row iterator for ORC files.
 * @since 1.1.0
 */
public interface RecordReader extends Closeable {
  /**
   * Read the next row batch. The size of the batch to read cannot be
   * controlled by the callers. Caller need to look at
   * VectorizedRowBatch.size of the returned object to know the batch
   * size read.
   * @param batch a row batch object to read into
   * @return were more rows available to read?
   * @throws java.io.IOException
   * @since 1.1.0
   */
  boolean nextBatch(VectorizedRowBatch batch) throws IOException;

  /**
   * Get the row number of the row that will be returned by the following
   * call to next().
   * @return the row number from 0 to the number of rows in the file
   * @throws java.io.IOException
   * @since 1.1.0
   */
  long getRowNumber() throws IOException;

  /**
   * Get the progress of the reader through the rows.
   * @return a fraction between 0.0 and 1.0 of rows read
   * @throws java.io.IOException
   * @since 1.1.0
   */
  float getProgress() throws IOException;

  /**
   * Release the resources associated with the given reader.
   * @throws java.io.IOException
   * @since 1.1.0
   */
  @Override
  void close() throws IOException;

  /**
   * Seek to a particular row number.
   * @since 1.1.0
   */
  void seekToRow(long rowCount) throws IOException;
}
