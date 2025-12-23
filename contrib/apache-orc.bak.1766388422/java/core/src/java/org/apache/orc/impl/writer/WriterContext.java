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

package org.apache.orc.impl.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.DataMask;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.PhysicalWriter;
import org.apache.orc.impl.OutStream;
import org.apache.orc.impl.StreamName;

import java.io.IOException;

public interface WriterContext {

  /**
   * Create a stream to store part of a column.
   * @param name the name of the stream
   * @return The output outStream that the section needs to be written to.
   */
  OutStream createStream(StreamName name) throws IOException;

  /**
   * Get the stride rate of the row index.
   */
  int getRowIndexStride();

  /**
   * Should be building the row index.
   * @return true if we are building the index
   */
  boolean buildIndex();

  /**
   * Is the ORC file compressed?
   * @return are the streams compressed
   */
  boolean isCompressed();

  /**
   * Get the encoding strategy to use.
   * @return encoding strategy
   */
  OrcFile.EncodingStrategy getEncodingStrategy();

  /**
   * Get the bloom filter columns
   * @return bloom filter columns
   */
  boolean[] getBloomFilterColumns();

  /**
   * Get bloom filter false positive percentage.
   * @return fpp
   */
  double getBloomFilterFPP();

  /**
   * Get the writer's configuration.
   * @return configuration
   */
  Configuration getConfiguration();

  /**
   * Get the version of the file to write.
   */
  OrcFile.Version getVersion();

  OrcFile.BloomFilterVersion getBloomFilterVersion();

  void writeIndex(StreamName name,
                  OrcProto.RowIndex.Builder index) throws IOException;

  void writeBloomFilter(StreamName name,
                        OrcProto.BloomFilterIndex.Builder bloom
                        ) throws IOException;

  /**
   * Get the mask for the unencrypted variant.
   * @param columnId the column id
   * @return the mask to apply to the unencrypted data or null if there is none
   */
  DataMask getUnencryptedMask(int columnId);

  /**
   * Get the encryption for the given column.
   * @param columnId the root column id
   * @return the column encryption or null if it isn't encrypted
   */
  WriterEncryptionVariant getEncryption(int columnId);

  /**
   * Get the PhysicalWriter.
   * @return the file's physical writer.
   */
  PhysicalWriter getPhysicalWriter();

  /**
   * Set the encoding for the current stripe.
   * @param column the column identifier
   * @param variant the encryption variant
   * @param encoding the encoding for this stripe
   */
  void setEncoding(int column, WriterEncryptionVariant variant,
                   OrcProto.ColumnEncoding encoding);

  /**
   * Set the column statistics for the stripe or file.
   * @param name the name of the statistics stream
   * @param stats the statistics for this column in this stripe
   */
  void writeStatistics(StreamName name,
                       OrcProto.ColumnStatistics.Builder stats
                     ) throws IOException;

  /**
   * Should the writer use UTC as the timezone?
   */
  boolean getUseUTCTimestamp();

  /**
   * Get the dictionary key size threshold.
   * @param columnId the column id
   * @return the minimum ratio for using a dictionary
   */
  double getDictionaryKeySizeThreshold(int columnId);

  /**
   * Should we write the data using the proleptic Gregorian calendar?
   * @return true if we should use the proleptic Gregorian calendar
   */
  boolean getProlepticGregorian();
}
