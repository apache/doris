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

#ifndef ORC_COLUMN_READER_HH
#define ORC_COLUMN_READER_HH

#include <unordered_map>

#include "orc/Vector.hh"

#include "ByteRLE.hh"
#include "Compression.hh"
#include "Timezone.hh"
#include "wrap/orc-proto-wrapper.hh"

namespace orc {

  class StripeStreams {
   public:
    virtual ~StripeStreams();

    /**
     * Get the array of booleans for which columns are selected.
     * @return the address of an array which contains true at the index of
     *    each columnId is selected.
     */
    virtual const std::vector<bool> getSelectedColumns() const = 0;

    /**
     * Get the encoding for the given column for this stripe.
     */
    virtual proto::ColumnEncoding getEncoding(uint64_t columnId) const = 0;

    /**
     * Get the stream for the given column/kind in this stripe.
     * @param columnId the id of the column
     * @param kind the kind of the stream
     * @param shouldStream should the reading page the stream in
     * @return the new stream
     */
    virtual std::unique_ptr<SeekableInputStream> getStream(uint64_t columnId,
                                                           proto::Stream_Kind kind,
                                                           bool shouldStream) const = 0;

    /**
     * Get the memory pool for this reader.
     */
    virtual MemoryPool& getMemoryPool() const = 0;

    /**
     * Get the reader metrics for this reader.
     */
    virtual ReaderMetrics* getReaderMetrics() const = 0;

    /**
     * Get the writer's timezone, so that we can convert their dates correctly.
     */
    virtual const Timezone& getWriterTimezone() const = 0;

    /**
     * Get the reader's timezone, so that we can convert their dates correctly.
     */
    virtual const Timezone& getReaderTimezone() const = 0;

    /**
     * Get the error stream.
     * @return a pointer to the stream that should get error messages
     */
    virtual std::ostream* getErrorStream() const = 0;

    /**
     * Should the reader throw when the scale overflows when reading Hive 0.11
     * decimals.
     * @return true if it should throw
     */
    virtual bool getThrowOnHive11DecimalOverflow() const = 0;

    /**
     * What is the scale forced on the Hive 0.11 decimals?
     * @return the number of scale digits
     */
    virtual int32_t getForcedScaleOnHive11Decimal() const = 0;

    /**
     * Whether decimals that have precision <=18 are encoded as fixed scale and values
     * encoded in RLE.
     */
    virtual bool isDecimalAsLong() const = 0;
  };

  /**
   * The interface for reading ORC data types.
   */
  class ColumnReader {
   protected:
    std::unique_ptr<ByteRleDecoder> notNullDecoder;
    const Type& type;
    uint64_t columnId;
    MemoryPool& memoryPool;
    ReaderMetrics* metrics;

    static bool shouldProcessChild(ReaderCategory readerCategory, const ReadPhase& readPhase) {
      return readPhase.contains(readerCategory) || readerCategory == ReaderCategory::FILTER_PARENT;
    }

    static int countNonNullRowsInRange(char* notNull, uint16_t start, uint16_t end) {
      int result = 0;
      while (start < end) {
        if (notNull[start++]) {
          result++;
        }
      }
      return result;
    }

   public:
    ColumnReader(const Type& type, StripeStreams& stipe, bool readPresentStream = true);

    virtual ~ColumnReader();

    const Type& getType() const {
      return type;
    }

    /**
     * Skip number of specified rows.
     * @param numValues the number of values to skip
     * @return the number of non-null values skipped
     */
    virtual uint64_t skip(uint64_t numValues, const ReadPhase& readPhase = ReadPhase::ALL);

    /**
     * Read the next group of values into this rowBatch.
     * @param rowBatch the memory to read into.
     * @param numValues the number of values to read
     * @param notNull if null, all values are not null. Otherwise, it is
     *           a mask (with at least numValues bytes) for which values to
     *           set.
     */
    virtual void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull,
                      const ReadPhase& readPhase = ReadPhase::ALL,
                      uint16_t* sel_rowid_idx = nullptr, size_t sel_size = 0);

    /**
     * Read the next group of values without decoding
     * @param rowBatch the memory to read into.
     * @param numValues the number of values to read
     * @param notNull if null, all values are not null. Otherwise, it is
     *           a mask (with at least numValues bytes) for which values to
     *           set.
     */
    virtual void nextEncoded(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull,
                             const ReadPhase& readPhase = ReadPhase::ALL,
                             uint16_t* sel_rowid_idx = nullptr, size_t sel_size = 0) {
      rowBatch.isEncoded = false;
      next(rowBatch, numValues, notNull, readPhase, sel_rowid_idx, sel_size);
    }

    /**
     * Seek to beginning of a row group in the current stripe
     * @param positions a list of PositionProviders storing the positions
     */
    virtual void seekToRowGroup(std::unordered_map<uint64_t, PositionProvider>& positions,
                                const ReadPhase& readPhase = ReadPhase::ALL);

    virtual void loadStringDicts(
        const std::unordered_map<uint64_t, std::string>& columnIdToNameMap,
        std::unordered_map<std::string, StringDictionary*>* columnNameToDictMap,
        const StringDictFilter* stringDictFilter) {}
  };

  /**
   * Create a reader for the given stripe.
   */
  std::unique_ptr<ColumnReader> buildReader(const Type& type, StripeStreams& stripe,
                                            bool useTightNumericVector = false,
                                            bool isTopLevel = false);

  void loadStringDicts(ColumnReader* columnReader,
                       const std::unordered_map<uint64_t, std::string>& columnIdToNameMap,
                       std::unordered_map<std::string, StringDictionary*>* columnNameToDictMap,
                       const StringDictFilter* stringDictFilter);
}  // namespace orc

#endif
