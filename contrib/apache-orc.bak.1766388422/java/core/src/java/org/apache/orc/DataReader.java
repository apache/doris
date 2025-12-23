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

import org.apache.orc.impl.BufferChunkList;
import org.apache.orc.impl.InStream;

import java.io.IOException;
import java.nio.ByteBuffer;

/** An abstract data reader that IO formats can use to read bytes from underlying storage. */
public interface DataReader extends AutoCloseable, Cloneable {

  /** Opens the DataReader, making it ready to use. */
  void open() throws IOException;

  OrcProto.StripeFooter readStripeFooter(StripeInformation stripe) throws IOException;

  /**
   * Reads the data from the file.
   *
   * Note that for the cases such as zero-copy read, caller must release the disk
   * ranges produced after being done with them. Call isTrackingDiskRanges to
   * find out if this is needed.
   *
   * @param range List of disk ranges to read. Ranges with data will be ignored.
   * @param doForceDirect Whether the data should be read into direct buffers.
   * @return The list range with buffers filled in
   */
  BufferChunkList readFileData(BufferChunkList range,
                               boolean doForceDirect) throws IOException;

  /**
   * Whether the user should release buffers created by readFileData. See readFileData javadoc.
   */
  boolean isTrackingDiskRanges();

  /**
   * Releases buffers created by readFileData. See readFileData javadoc.
   * @param toRelease The buffer to release.
   */
  void releaseBuffer(ByteBuffer toRelease);

  /**
   * Clone the entire state of the DataReader with the assumption that the
   * clone will be closed at a different time. Thus, any file handles in the
   * implementation need to be cloned.
   * @return a new instance
   */
  DataReader clone();

  @Override
  void close() throws IOException;

  /**
   * Returns the compression options used by this DataReader.
   * The codec if present is owned by the DataReader and should not be returned
   * to the OrcCodecPool.
   * @return the compression options
   */
  InStream.StreamOptions getCompressionOptions();
}
