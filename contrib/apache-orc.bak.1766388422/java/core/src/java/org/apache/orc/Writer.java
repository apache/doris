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
import java.nio.ByteBuffer;
import java.util.List;

/**
 * The interface for writing ORC files.
 * @since 1.1.0
 */
public interface Writer extends Closeable {

  /**
   * Get the schema for this writer
   * @return the file schema
   * @since 1.1.0
   */
  TypeDescription getSchema();

  /**
   * Add arbitrary meta-data to the ORC file. This may be called at any point
   * until the Writer is closed. If the same key is passed a second time, the
   * second value will replace the first.
   * @param key a key to label the data with.
   * @param value the contents of the metadata.
   * @since 1.1.0
   */
  void addUserMetadata(String key, ByteBuffer value);

  /**
   * Add a row batch to the ORC file.
   * @param batch the rows to add
   * @since 1.1.0
   */
  void addRowBatch(VectorizedRowBatch batch) throws IOException;

  /**
   * Flush all of the buffers and close the file. No methods on this writer
   * should be called afterwards.
   * @throws IOException
   * @since 1.1.0
   */
  @Override
  void close() throws IOException;

  /**
   * Return the deserialized data size. Raw data size will be compute when
   * writing the file footer. Hence raw data size value will be available only
   * after closing the writer.
   *
   * @return raw data size
   * @since 1.1.0
   */
  long getRawDataSize();

  /**
   * Return the number of rows in file. Row count gets updated when flushing
   * the stripes. To get accurate row count this method should be called after
   * closing the writer.
   *
   * @return row count
   * @since 1.1.0
   */
  long getNumberOfRows();

  /**
   * Write an intermediate footer on the file such that if the file is
   * truncated to the returned offset, it would be a valid ORC file.
   * @return the offset that would be a valid end location for an ORC file
   * @since 1.1.0
   */
  long writeIntermediateFooter() throws IOException;

  /**
   * Fast stripe append to ORC file. This interface is used for fast ORC file
   * merge with other ORC files. When merging, the file to be merged should pass
   * stripe in binary form along with stripe information and stripe statistics.
   * After appending last stripe of a file, use appendUserMetadata() to append
   * any user metadata.
   *
   * This form only supports files with no column encryption. Use {@link
   * #appendStripe(byte[], int, int, StripeInformation, StripeStatistics[])}
   * for files with encryption.
   *
   * @param stripe - stripe as byte array
   * @param offset - offset within byte array
   * @param length - length of stripe within byte array
   * @param stripeInfo - stripe information
   * @param stripeStatistics - unencrypted stripe statistics
   * @since 1.1.0
   */
  void appendStripe(byte[] stripe, int offset, int length,
      StripeInformation stripeInfo,
      OrcProto.StripeStatistics stripeStatistics) throws IOException;

  /**
   * Fast stripe append to ORC file. This interface is used for fast ORC file
   * merge with other ORC files. When merging, the file to be merged should pass
   * stripe in binary form along with stripe information and stripe statistics.
   * After appending last stripe of a file, use {@link #addUserMetadata(String,
   * ByteBuffer)} to append any user metadata.
   * @param stripe - stripe as byte array
   * @param offset - offset within byte array
   * @param length - length of stripe within byte array
   * @param stripeInfo - stripe information
   * @param stripeStatistics - stripe statistics with the last one being
   *                         for the unencrypted data and the others being for
   *                         each encryption variant.
   * @since 1.6.0
   */
  void appendStripe(byte[] stripe, int offset, int length,
                    StripeInformation stripeInfo,
                    StripeStatistics[] stripeStatistics) throws IOException;

  /**
   * Update the current user metadata with a list of new values.
   * @param userMetadata - user metadata
   * @deprecated use {@link #addUserMetadata(String, ByteBuffer)} instead
   * @since 1.1.0
   */
  void appendUserMetadata(List<OrcProto.UserMetadataItem> userMetadata);

  /**
   * Get the statistics about the columns in the file. The output of this is
   * based on the time at which it is called. It shall use all of the currently
   * written data to provide the statistics.
   *
   * Please note there are costs involved with invoking this method and should
   * be used judiciously.
   *
   * @return the information about the column
   * @since 1.1.0
   */
  ColumnStatistics[] getStatistics() throws IOException;

  /**
   * Get the stripe information about the file. The output of this is based on the time at which it
   * is called. It shall return stripes that have been completed.
   *
   * After the writer is closed this shall give the complete stripe information.
   *
   * @return stripe information
   * @throws IOException
   * @since 1.6.8
   */
  List<StripeInformation> getStripes() throws IOException;

  /**
   * Estimate the memory currently used by the writer to buffer the stripe.
   * `This method help write engine to control the refresh policy of the ORC.`
   * @return the number of bytes
   */
  long estimateMemory();
}
