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

#ifndef ORC_FILE_HH
#define ORC_FILE_HH

#include <string>

#include "orc/Reader.hh"
#include "orc/Writer.hh"
#include "orc/orc-config.hh"

/** /file orc/OrcFile.hh
    @brief The top level interface to ORC.
*/

namespace orc {

  /**
   * An abstract interface for providing ORC readers a stream of bytes.
   */
  class InputStream {
   public:
    virtual ~InputStream();

    /**
     * Get the total length of the file in bytes.
     */
    virtual uint64_t getLength() const = 0;

    /**
     * Get the natural size for reads.
     * @return the number of bytes that should be read at once
     */
    virtual uint64_t getNaturalReadSize() const = 0;

    /**
     * Read length bytes from the file starting at offset into
     * the buffer starting at buf.
     * @param buf the starting position of a buffer.
     * @param length the number of bytes to read.
     * @param offset the position in the stream to read from.
     */
    virtual void read(void* buf, uint64_t length, uint64_t offset) = 0;

    /**
     * Get the name of the stream for error messages.
     */
    virtual const std::string& getName() const = 0;

    virtual void beforeReadStripe(
        std::unique_ptr<StripeInformation> currentStripeInformation,
        const std::vector<bool>& selectedColumns,
        std::unordered_map<orc::StreamId, std::shared_ptr<InputStream>>& streams);
  };

  /**
   * An abstract interface for providing ORC writer a stream of bytes.
   */
  class OutputStream {
   public:
    virtual ~OutputStream();

    /**
     * Get the total length of bytes written.
     */
    virtual uint64_t getLength() const = 0;

    /**
     * Get the natural size for reads.
     * @return the number of bytes that should be written at once
     */
    virtual uint64_t getNaturalWriteSize() const = 0;

    /**
     * Write/Append length bytes pointed by buf to the file stream
     * @param buf the starting position of a buffer.
     * @param length the number of bytes to write.
     */
    virtual void write(const void* buf, size_t length) = 0;

    /**
     * Get the name of the stream for error messages.
     */
    virtual const std::string& getName() const = 0;

    /**
     * Close the stream and flush any pending data to the disk.
     */
    virtual void close() = 0;
  };

  /**
   * Create a stream to a local file or HDFS file if path begins with "hdfs://"
   * @param path the name of the file in the local file system or HDFS
   * @param metrics the metrics of the reader
   */
  std::unique_ptr<InputStream> readFile(const std::string& path, ReaderMetrics* metrics = nullptr);

  /**
   * Create a stream to a local file.
   * @param path the name of the file in the local file system
   * @param metrics the metrics of the reader
   */
  std::unique_ptr<InputStream> readLocalFile(const std::string& path,
                                             ReaderMetrics* metrics = nullptr);

  /**
   * Create a stream to an HDFS file.
   * @param path the uri of the file in HDFS
   * @param metrics the metrics of the reader
   */
  std::unique_ptr<InputStream> readHdfsFile(const std::string& path,
                                            ReaderMetrics* metrics = nullptr);

  /**
   * Create a reader to read the ORC file.
   * @param stream the stream to read
   * @param options the options for reading the file
   */
  std::unique_ptr<Reader> createReader(std::unique_ptr<InputStream> stream,
                                       const ReaderOptions& options);
  /**
   * Create a stream to write to a local file.
   * @param path the name of the file in the local file system
   */
  std::unique_ptr<OutputStream> writeLocalFile(const std::string& path);

  /**
   * Create a writer to write the ORC file.
   * @param type the type of data to be written
   * @param stream the stream to write to
   * @param options the options for writing the file
   */
  std::unique_ptr<Writer> createWriter(const Type& type, OutputStream* stream,
                                       const WriterOptions& options);
}  // namespace orc

#endif
