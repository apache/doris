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

#ifndef ORC_READER_IMPL_HH
#define ORC_READER_IMPL_HH

#include "orc/Exceptions.hh"
#include "orc/Int128.hh"
#include "orc/OrcFile.hh"
#include "orc/Reader.hh"

#include "ColumnReader.hh"
#include "RLE.hh"
#include "SchemaEvolution.hh"
#include "TypeImpl.hh"
#include "sargs/SargsApplier.hh"

namespace orc {

  static const uint64_t DIRECTORY_SIZE_GUESS = 16 * 1024;

  class ReaderContext {
   public:
    ReaderContext() = default;

    const ORCFilter* getFilterCallback() const {
      return filter;
    }

    ReaderContext& setFilterCallback(std::unordered_set<int> _filterColumnIds,
                                     const ORCFilter* _filter) {
      this->filterColumnIds = std::move(_filterColumnIds);
      this->filter = _filter;
      return *this;
    }

    const ORCFilter* getStringDictFilter() const {
      return filter;
    }

    ReaderContext& setStringDictFilter(const StringDictFilter* _stringDictFilter) {
      this->stringDictFilter = _stringDictFilter;
      return *this;
    }

   private:
    std::unordered_set<int> filterColumnIds;
    const ORCFilter* filter;
    const StringDictFilter* stringDictFilter;
  };

  /**
   * WriterVersion Implementation
   */
  class WriterVersionImpl {
   private:
    WriterVersion version;

   public:
    // Known Versions with issues resolved
    // The static method below is to fix global constructors Clang warning
    static const WriterVersionImpl& VERSION_HIVE_8732();

    WriterVersionImpl(WriterVersion ver) : version(ver) {}

    bool compareGT(const WriterVersion other) const {
      return version > other;
    }
  };

  /**
   * State shared between Reader and Row Reader
   */
  struct FileContents {
    std::unique_ptr<InputStream> stream;
    std::unique_ptr<proto::PostScript> postscript;
    std::unique_ptr<proto::Footer> footer;
    std::unique_ptr<Type> schema;
    uint64_t blockSize;
    CompressionKind compression;
    MemoryPool* pool;
    std::ostream* errorStream;
    /// Decimal64 in ORCv2 uses RLE to store values. This flag indicates whether
    /// this new encoding is used.
    bool isDecimalAsLong;
    std::unique_ptr<proto::Metadata> metadata;
    ReaderMetrics* readerMetrics;
    std::unique_ptr<SargsApplier> sargsApplier;
  };

  proto::StripeFooter getStripeFooter(const proto::StripeInformation& info,
                                      const FileContents& contents);

  class ReaderImpl;
  class Timezone;

  class ColumnSelector {
   private:
    std::map<std::string, uint64_t> nameIdMap;
    std::map<uint64_t, const Type*> idTypeMap;
    const FileContents* contents;
    std::vector<std::string> columns;

    // build map from type name and id, id to Type
    void buildTypeNameIdMap(const Type* type);
    std::string toDotColumnPath();

   public:
    // Select a field by name
    void updateSelectedByName(std::vector<bool>& selectedColumns, const std::string& name);
    // Select a field by id
    void updateSelectedByFieldId(std::vector<bool>& selectedColumns, uint64_t fieldId);
    // Select a type by id
    void updateSelectedByTypeId(std::vector<bool>& selectedColumns, uint64_t typeId);
    // Select a type by id and read intent map.
    void updateSelectedByTypeId(std::vector<bool>& selectedColumns, uint64_t typeId,
                                const RowReaderOptions::IdReadIntentMap& idReadIntentMap);

    // Select all of the recursive children of the given type.
    void selectChildren(std::vector<bool>& selectedColumns, const Type& type);
    // Select a type id of the given type.
    // This function may also select all of the recursive children of the given type
    // depending on the read intent of that type in idReadIntentMap.
    void selectChildren(std::vector<bool>& selectedColumns, const Type& type,
                        const RowReaderOptions::IdReadIntentMap& idReadIntentMap);

    // For each child of type, select it if one of its children
    // is selected.
    bool selectParents(std::vector<bool>& selectedColumns, const Type& type);

    /**
     * Constructor that selects columns.
     * @param contents of the file
     */
    ColumnSelector(const FileContents* contents);

    // Select the columns from the RowReaderoptions object
    void updateSelected(std::vector<bool>& selectedColumns, const RowReaderOptions& options);

    // Select the columns from the Readeroptions object
    void updateSelected(std::vector<bool>& selectedColumns, const ReaderOptions& options);
  };

  class RowReaderImpl : public RowReader {
   private:
    const Timezone& localTimezone;

    // contents
    std::shared_ptr<FileContents> contents;
    std::unordered_map<orc::StreamId, std::shared_ptr<InputStream>> streams;
    const bool throwOnHive11DecimalOverflow;
    const int32_t forcedScaleOnHive11Decimal;

    // inputs
    std::vector<bool> selectedColumns;

    // footer
    proto::Footer* footer;
    DataBuffer<uint64_t> firstRowOfStripe;
    mutable std::unique_ptr<Type> selectedSchema;
    bool skipBloomFilters;

    // reading state
    uint64_t previousRow;
    uint64_t firstStripe;
    uint64_t currentStripe;
    uint64_t lastStripe;  // the stripe AFTER the last one
    uint64_t processingStripe;
    uint64_t currentRowInStripe;
    uint64_t followRowInStripe;
    uint64_t rowsInCurrentStripe;
    // number of row groups between first stripe and last stripe
    uint64_t numRowGroupsInStripeRange;
    // numbfer of rows in range
    uint64_t rowTotalInRange;
    proto::StripeInformation currentStripeInfo;
    proto::StripeFooter currentStripeFooter;
    std::unique_ptr<ColumnReader> reader;

    bool enableEncodedBlock;
    bool useTightNumericVector;
    // internal methods
    void startNextStripe(const ReadPhase& readPhase);
    inline void markEndOfFile();

    // row index of current stripe with column id as the key
    std::unordered_map<uint64_t, proto::RowIndex> rowIndexes;
    std::map<uint32_t, BloomFilterIndex> bloomFilterIndex;
    std::shared_ptr<SearchArgument> sargs;
    std::unique_ptr<SargsApplier> sargsApplier;

    // desired timezone to return data of timestamp types.
    const Timezone& readerTimezone;

    std::unique_ptr<ReaderContext> readerContext;
    const ORCFilter* filter;
    ReadPhase startReadPhase;
    bool needsFollowColumnsRead;

    std::map<uint64_t, Type*> idTypeMap;
    std::map<std::string, Type*> nameTypeMap;
    std::vector<std::string> columns;

    const StringDictFilter* stringDictFilter;

    // load stripe index if not done so
    void loadStripeIndex();

    // In case of PPD, batch size should be aware of row group boundaries.
    // If only a subset of row groups are selected then the next read should
    // stop at the end of selected range.
    static uint64_t computeBatchSize(uint64_t requestedSize, uint64_t currentRowInStripe,
                                     uint64_t rowsInCurrentStripe, uint64_t rowIndexStride,
                                     const std::vector<uint64_t>& nextSkippedRows);

    // Skip non-selected rows
    static uint64_t advanceToNextRowGroup(uint64_t currentRowInStripe, uint64_t rowsInCurrentStripe,
                                          uint64_t rowIndexStride,
                                          const std::vector<uint64_t>& nextSkippedRows);

    friend class TestRowReader_advanceToNextRowGroup_Test;
    friend class TestRowReader_computeBatchSize_Test;

    // whether the current stripe is initialized
    inline bool isCurrentStripeInited() const {
      return currentStripe == processingStripe;
    }

    /**
     * Seek to the start of a row group in the current stripe
     * @param rowGroupEntryId the row group id to seek to
     */
    void seekToRowGroup(uint32_t rowGroupEntryId, const ReadPhase& readPhase);

    /**
     * Check if the file has bad bloom filters. We will skip using them in the
     * following reads.
     * @return true if it has.
     */
    bool hasBadBloomFilters();

    // build map from type name and id, id to Type
    void buildTypeNameIdMap(Type* type);

    std::string toDotColumnPath();

    void nextBatch(ColumnVectorBatch& data, int batchSize, const ReadPhase& readPhase,
                   uint16_t* sel_rowid_idx, void* arg);

    int computeRGIdx(uint64_t rowIndexStride, long rowIdx);

    ReadPhase prepareFollowReaders(uint64_t rowIndexStride, long toFollowRow, long fromFollowRow);

   public:
    /**
     * Constructor that lets the user specify additional options.
     * @param contents of the file
     * @param options options for reading
     */
    RowReaderImpl(std::shared_ptr<FileContents> contents, const RowReaderOptions& options,
                  const ORCFilter* filter = nullptr,
                  const StringDictFilter* stringDictFilter = nullptr);

    // Select the columns from the options object
    const std::vector<bool> getSelectedColumns() const override;

    const Type& getSelectedType() const override;

    std::unique_ptr<ColumnVectorBatch> createRowBatch(uint64_t size) const override;

    uint64_t nextBatch(ColumnVectorBatch& data, void* arg = nullptr) override;

    bool next(ColumnVectorBatch& data) override;

    CompressionKind getCompression() const;

    uint64_t getCompressionSize() const;

    uint64_t getRowNumber() const override;

    void seekToRow(uint64_t rowNumber) override;

    uint64_t getNumberOfRows() const override;

    const FileContents& getFileContents() const;
    bool getThrowOnHive11DecimalOverflow() const;
    bool getIsDecimalAsLong() const;
    int32_t getForcedScaleOnHive11Decimal() const;
  };

  class ReaderImpl : public Reader {
   private:
    // FileContents
    std::shared_ptr<FileContents> contents;

    // inputs
    const ReaderOptions options;
    const uint64_t fileLength;
    const uint64_t postscriptLength;

    // footer
    proto::Footer* footer;
    uint64_t numberOfStripes;
    std::unique_ptr<SargsApplier> sargsApplier;
    std::vector<int> getNeedReadStripes(const RowReaderOptions& opts) override;
    uint64_t getMemoryUse(int stripeIx, std::vector<bool>& selectedColumns);

    // internal methods
    void readMetadata() const;
    void checkOrcVersion();
    void getRowIndexStatistics(const proto::StripeInformation& stripeInfo, uint64_t stripeIndex,
                               const proto::StripeFooter& currentStripeFooter,
                               std::vector<std::vector<proto::ColumnStatistics>>* indexStats) const;

    // metadata
    mutable bool isMetadataLoaded;

   public:
    /**
     * Constructor that lets the user specify additional options.
     * @param contents of the file
     * @param options options for reading
     * @param fileLength the length of the file in bytes
     * @param postscriptLength the length of the postscript in bytes
     */
    ReaderImpl(std::shared_ptr<FileContents> contents, const ReaderOptions& options,
               uint64_t fileLength, uint64_t postscriptLength);

    const ReaderOptions& getReaderOptions() const;

    CompressionKind getCompression() const override;

    FileVersion getFormatVersion() const override;

    WriterId getWriterId() const override;

    uint32_t getWriterIdValue() const override;

    std::string getSoftwareVersion() const override;

    WriterVersion getWriterVersion() const override;

    uint64_t getNumberOfRows() const override;

    uint64_t getRowIndexStride() const override;

    std::list<std::string> getMetadataKeys() const override;

    std::string getMetadataValue(const std::string& key) const override;

    bool hasMetadataValue(const std::string& key) const override;

    uint64_t getCompressionSize() const override;

    uint64_t getNumberOfStripes() const override;

    std::unique_ptr<StripeInformation> getStripe(uint64_t) const override;

    uint64_t getNumberOfStripeStatistics() const override;

    const std::string& getStreamName() const override;

    std::unique_ptr<StripeStatistics> getStripeStatistics(uint64_t stripeIndex) const override;

    std::unique_ptr<RowReader> createRowReader(
        const ORCFilter* filter = nullptr,
        const StringDictFilter* stringDictFilter = nullptr) const override;

    std::unique_ptr<RowReader> createRowReader(
        const RowReaderOptions& options, const ORCFilter* filter = nullptr,
        const StringDictFilter* stringDictFilter = nullptr) const override;

    uint64_t getContentLength() const override;
    uint64_t getStripeStatisticsLength() const override;
    uint64_t getFileFooterLength() const override;
    uint64_t getFilePostscriptLength() const override;
    uint64_t getFileLength() const override;

    std::unique_ptr<Statistics> getStatistics() const override;

    std::unique_ptr<ColumnStatistics> getColumnStatistics(uint32_t columnId) const override;

    std::string getSerializedFileTail() const override;

    const Type& getType() const override;

    bool hasCorrectStatistics() const override;

    const ReaderMetrics* getReaderMetrics() const override {
      return contents->readerMetrics;
    }

    const proto::PostScript* getPostscript() const {
      return contents->postscript.get();
    }

    uint64_t getBlockSize() const {
      return contents->blockSize;
    }

    const proto::Footer* getFooter() const {
      return contents->footer.get();
    }

    const Type* getSchema() const {
      return contents->schema.get();
    }

    InputStream* getStream() const override {
      return contents->stream.get();
    }

    void setStream(std::unique_ptr<InputStream> inputStreamUPtr) override {
      contents->stream = std::move(inputStreamUPtr);
    }

    uint64_t getMemoryUse(int stripeIx = -1) override;

    uint64_t getMemoryUseByFieldId(const std::list<uint64_t>& include, int stripeIx = -1) override;

    uint64_t getMemoryUseByName(const std::list<std::string>& names, int stripeIx = -1) override;

    uint64_t getMemoryUseByTypeId(const std::list<uint64_t>& include, int stripeIx = -1) override;

    std::map<uint32_t, BloomFilterIndex> getBloomFilters(
        uint32_t stripeIndex, const std::set<uint32_t>& included) const override;

    void getSelectedColumns(const std::list<std::string>& names, std::vector<bool>& selectedColumns) override;
  };
}  // namespace orc

#endif
