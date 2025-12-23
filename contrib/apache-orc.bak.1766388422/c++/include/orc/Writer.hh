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

#ifndef ORC_WRITER_HH
#define ORC_WRITER_HH

#include "orc/Common.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"
#include "orc/orc-config.hh"

#include <atomic>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace orc {

  // classes that hold data members so we can maintain binary compatibility
  struct WriterOptionsPrivate;

  enum CompressionStrategy { CompressionStrategy_SPEED = 0, CompressionStrategy_COMPRESSION };

  enum RleVersion { RleVersion_1 = 0, RleVersion_2 = 1 };

  class Timezone;

  /**
   * Expose the IO metrics for write operation.
   */
  struct WriterMetrics {
    // Record the number of IO requests written to the output file
    std::atomic<uint64_t> IOCount{0};
    // Record the lantency of IO blocking
    std::atomic<uint64_t> IOBlockingLatencyUs{0};
  };
  /**
   * Options for creating a Writer.
   */
  class WriterOptions {
   private:
    std::unique_ptr<WriterOptionsPrivate> privateBits;

   public:
    WriterOptions();
    WriterOptions(const WriterOptions&);
    WriterOptions(WriterOptions&);
    WriterOptions& operator=(const WriterOptions&);
    virtual ~WriterOptions();

    /**
     * Set the strip size.
     */
    WriterOptions& setStripeSize(uint64_t size);

    /**
     * Get the strip size.
     * @return if not set, return default value.
     */
    uint64_t getStripeSize() const;

    /**
     * Set the data compression block size.
     */
    WriterOptions& setCompressionBlockSize(uint64_t size);

    /**
     * Get the data compression block size.
     * @return if not set, return default value.
     */
    uint64_t getCompressionBlockSize() const;

    /**
     * Set row index stride (the number of rows per an entry in the row index). Use value 0 to
     * disable row index.
     */
    WriterOptions& setRowIndexStride(uint64_t stride);

    /**
     * Get the row index stride (the number of rows per an entry in the row index).
     * @return if not set, return default value.
     */
    uint64_t getRowIndexStride() const;

    /**
     * Set the dictionary key size threshold.
     * 0 to disable dictionary encoding.
     * 1 to always enable dictionary encoding.
     */
    WriterOptions& setDictionaryKeySizeThreshold(double val);

    /**
     * Get the dictionary key size threshold.
     */
    double getDictionaryKeySizeThreshold() const;

    /**
     * Set Orc file version
     */
    WriterOptions& setFileVersion(const FileVersion& version);

    /**
     * Get Orc file version
     */
    FileVersion getFileVersion() const;

    /**
     * Set compression kind.
     */
    WriterOptions& setCompression(CompressionKind comp);

    /**
     * Get the compression kind.
     * @return if not set, return default value which is ZLIB.
     */
    CompressionKind getCompression() const;

    /**
     * Set the compression strategy.
     */
    WriterOptions& setCompressionStrategy(CompressionStrategy strategy);

    /**
     * Get the compression strategy.
     * @return if not set, return default value which is speed.
     */
    CompressionStrategy getCompressionStrategy() const;

    /**
     * Get if the bitpacking should be aligned.
     * @return true if should be aligned, return false otherwise
     */
    bool getAlignedBitpacking() const;

    /**
     * Set the padding tolerance.
     */
    WriterOptions& setPaddingTolerance(double tolerance);

    /**
     * Get the padding tolerance.
     * @return if not set, return default value which is zero.
     */
    double getPaddingTolerance() const;

    /**
     * Set the memory pool.
     */
    WriterOptions& setMemoryPool(MemoryPool* memoryPool);

    /**
     * Get the memory pool.
     * @return if not set, return default memory pool.
     */
    MemoryPool* getMemoryPool() const;

    /**
     * Set the error stream.
     */
    WriterOptions& setErrorStream(std::ostream& errStream);

    /**
     * Get the error stream.
     * @return if not set, return std::err.
     */
    std::ostream* getErrorStream() const;

    /**
     * Get the RLE version.
     */
    RleVersion getRleVersion() const;

    /**
     * Get whether or not to write row group index
     * @return if not set, the default is false
     */
    bool getEnableIndex() const;

    /**
     * Get whether or not to enable dictionary encoding
     * @return if not set, the default is false
     */
    bool getEnableDictionary() const;

    /**
     * Set columns that use BloomFilter
     */
    WriterOptions& setColumnsUseBloomFilter(const std::set<uint64_t>& columns);

    /**
     * Get whether this column uses BloomFilter
     */
    bool isColumnUseBloomFilter(uint64_t column) const;

    /**
     * Set false positive probability of BloomFilter
     */
    WriterOptions& setBloomFilterFPP(double fpp);

    /**
     * Get false positive probability of BloomFilter
     */
    double getBloomFilterFPP() const;

    /**
     * Get version of BloomFilter
     */
    BloomFilterVersion getBloomFilterVersion() const;

    /**
     * Get writer timezone
     * @return writer timezone
     */
    const Timezone& getTimezone() const;

    /**
     * Get writer timezone name
     * @return writer timezone name
     */
    const std::string& getTimezoneName() const;

    /**
     * Set writer timezone
     * @param zone writer timezone name
     */
    WriterOptions& setTimezoneName(const std::string& zone);

    /**
     * Set the writer metrics.
     */
    WriterOptions& setWriterMetrics(WriterMetrics* metrics);

    /**
     * Get the writer metrics.
     * @return if not set, return nullptr.
     */
    WriterMetrics* getWriterMetrics() const;

    /**
     * Set use tight numeric vectorBatch or not.
     */
    WriterOptions& setUseTightNumericVector(bool useTightNumericVector);

    /**
     * Get whether or not to use dedicated columnVectorBatch
     * @return if not set, the default is false
     */
    bool getUseTightNumericVector() const;

    /**
     * Set the initial capacity of output buffer in the class BufferedOutputStream.
     * Each column contains one or more BufferOutputStream depending on its type,
     * and these buffers will automatically expand when more memory is required.
     */
    WriterOptions& setOutputBufferCapacity(uint64_t capacity);

    /**
     * Get the initial capacity of output buffer in the class BufferedOutputStream.
     * @return if not set, return default value which is 1 MB.
     */
    uint64_t getOutputBufferCapacity() const;
  };

  class Writer {
   public:
    virtual ~Writer();

    /**
     * Create a row batch for writing the columns into this file.
     * @param size the number of rows to write.
     * @return a new ColumnVectorBatch to write into.
     */
    virtual std::unique_ptr<ColumnVectorBatch> createRowBatch(uint64_t size) const = 0;

    /**
     * Add a row batch into current writer.
     * @param rowsToAdd the row batch data to write.
     */
    virtual void add(ColumnVectorBatch& rowsToAdd) = 0;

    /**
     * Close the writer and flush any pending data to the output stream.
     */
    virtual void close() = 0;

    /**
     * Add user metadata to the writer.
     */
    virtual void addUserMetadata(const std::string& name, const std::string& value) = 0;
  };
}  // namespace orc

#endif
