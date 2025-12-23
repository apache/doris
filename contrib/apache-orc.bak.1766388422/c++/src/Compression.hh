/**
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

#ifndef ORC_COMPRESSION_HH
#define ORC_COMPRESSION_HH

#include "io/InputStream.hh"
#include "io/OutputStream.hh"

namespace orc {

  /**
   * Create a decompressor for the given compression kind.
   * @param kind the compression type to implement
   * @param input the input stream that is the underlying source
   * @param bufferSize the maximum size of the buffer
   * @param pool the memory pool
   * @param metrics the reader metrics
   */
  std::unique_ptr<SeekableInputStream> createDecompressor(
      CompressionKind kind, std::unique_ptr<SeekableInputStream> input, uint64_t bufferSize,
      MemoryPool& pool, ReaderMetrics* metrics);

  /**
   * Create a compressor for the given compression kind.
   * @param kind the compression type to implement
   * @param outStream the output stream that is the underlying target
   * @param strategy compression strategy
   * @param bufferCapacity compression stream buffer total capacity
   * @param compressionBlockSize compression buffer block size
   * @param pool the memory pool
   */
  std::unique_ptr<BufferedOutputStream> createCompressor(CompressionKind kind,
                                                         OutputStream* outStream,
                                                         CompressionStrategy strategy,
                                                         uint64_t bufferCapacity,
                                                         uint64_t compressionBlockSize,
                                                         MemoryPool& pool, WriterMetrics* metrics);
}  // namespace orc

#endif
