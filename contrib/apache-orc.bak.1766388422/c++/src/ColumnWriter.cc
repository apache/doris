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

#include "orc/Int128.hh"
#include "orc/Writer.hh"

#include "ByteRLE.hh"
#include "ColumnWriter.hh"
#include "RLE.hh"
#include "Statistics.hh"
#include "Timezone.hh"

namespace orc {
  StreamsFactory::~StreamsFactory() {
    // PASS
  }

  class StreamsFactoryImpl : public StreamsFactory {
   public:
    StreamsFactoryImpl(const WriterOptions& writerOptions, OutputStream* outputStream)
        : options(writerOptions), outStream(outputStream) {}

    virtual std::unique_ptr<BufferedOutputStream> createStream(
        proto::Stream_Kind kind) const override;

   private:
    const WriterOptions& options;
    OutputStream* outStream;
  };

  std::unique_ptr<BufferedOutputStream> StreamsFactoryImpl::createStream(proto::Stream_Kind) const {
    // In the future, we can decide compression strategy and modifier
    // based on stream kind. But for now we just use the setting from
    // WriterOption
    return createCompressor(options.getCompression(), outStream, options.getCompressionStrategy(),
                            // BufferedOutputStream initial capacity
                            options.getOutputBufferCapacity(), options.getCompressionBlockSize(),
                            *options.getMemoryPool(), options.getWriterMetrics());
  }

  std::unique_ptr<StreamsFactory> createStreamsFactory(const WriterOptions& options,
                                                       OutputStream* outStream) {
    return std::make_unique<StreamsFactoryImpl>(options, outStream);
  }

  RowIndexPositionRecorder::~RowIndexPositionRecorder() {
    // PASS
  }

  proto::ColumnEncoding_Kind RleVersionMapper(RleVersion rleVersion) {
    switch (rleVersion) {
      case RleVersion_1:
        return proto::ColumnEncoding_Kind_DIRECT;
      case RleVersion_2:
        return proto::ColumnEncoding_Kind_DIRECT_V2;
      default:
        throw InvalidArgument("Invalid param");
    }
  }

  ColumnWriter::ColumnWriter(const Type& type, const StreamsFactory& factory,
                             const WriterOptions& options)
      : columnId(type.getColumnId()),
        colIndexStatistics(),
        colStripeStatistics(),
        colFileStatistics(),
        enableIndex(options.getEnableIndex()),
        rowIndex(),
        rowIndexEntry(),
        rowIndexPosition(),
        enableBloomFilter(false),
        memPool(*options.getMemoryPool()),
        indexStream(),
        bloomFilterStream(),
        hasNullValue(false) {
    std::unique_ptr<BufferedOutputStream> presentStream =
        factory.createStream(proto::Stream_Kind_PRESENT);
    notNullEncoder = createBooleanRleEncoder(std::move(presentStream));

    colIndexStatistics = createColumnStatistics(type);
    colStripeStatistics = createColumnStatistics(type);
    colFileStatistics = createColumnStatistics(type);

    if (enableIndex) {
      rowIndex = std::make_unique<proto::RowIndex>();
      rowIndexEntry = std::make_unique<proto::RowIndexEntry>();
      rowIndexPosition = std::make_unique<RowIndexPositionRecorder>(*rowIndexEntry);
      indexStream = factory.createStream(proto::Stream_Kind_ROW_INDEX);

      // BloomFilters for non-UTF8 strings and non-UTC timestamps are not supported
      if (options.isColumnUseBloomFilter(columnId) &&
          options.getBloomFilterVersion() == BloomFilterVersion::UTF8) {
        enableBloomFilter = true;
        bloomFilter.reset(
            new BloomFilterImpl(options.getRowIndexStride(), options.getBloomFilterFPP()));
        bloomFilterIndex.reset(new proto::BloomFilterIndex());
        bloomFilterStream = factory.createStream(proto::Stream_Kind_BLOOM_FILTER_UTF8);
      }
    }
  }

  ColumnWriter::~ColumnWriter() {
    // PASS
  }

  void ColumnWriter::add(ColumnVectorBatch& batch, uint64_t offset, uint64_t numValues,
                         const char* incomingMask) {
    const char* notNull = batch.notNull.data() + offset;
    notNullEncoder->add(notNull, numValues, incomingMask);
    hasNullValue |= batch.hasNulls;
    for (uint64_t i = 0; !hasNullValue && i < numValues; ++i) {
      if (!notNull[i]) {
        hasNullValue = true;
      }
    }
  }

  void ColumnWriter::flush(std::vector<proto::Stream>& streams) {
    if (!hasNullValue) {
      // supress the present stream
      notNullEncoder->suppress();
      return;
    }
    proto::Stream stream;
    stream.set_kind(proto::Stream_Kind_PRESENT);
    stream.set_column(static_cast<uint32_t>(columnId));
    stream.set_length(notNullEncoder->flush());
    streams.push_back(stream);
  }

  uint64_t ColumnWriter::getEstimatedSize() const {
    return notNullEncoder->getBufferSize();
  }

  void ColumnWriter::getStripeStatistics(std::vector<proto::ColumnStatistics>& stats) const {
    getProtoBufStatistics(stats, colStripeStatistics.get());
  }

  void ColumnWriter::mergeStripeStatsIntoFileStats() {
    colFileStatistics->merge(*colStripeStatistics);
    colStripeStatistics->reset();
  }

  void ColumnWriter::mergeRowGroupStatsIntoStripeStats() {
    colStripeStatistics->merge(*colIndexStatistics);
    colIndexStatistics->reset();
  }

  void ColumnWriter::getFileStatistics(std::vector<proto::ColumnStatistics>& stats) const {
    getProtoBufStatistics(stats, colFileStatistics.get());
  }

  void ColumnWriter::createRowIndexEntry() {
    proto::ColumnStatistics* indexStats = rowIndexEntry->mutable_statistics();
    colIndexStatistics->toProtoBuf(*indexStats);

    *rowIndex->add_entry() = *rowIndexEntry;

    rowIndexEntry->clear_positions();
    rowIndexEntry->clear_statistics();

    colStripeStatistics->merge(*colIndexStatistics);
    colIndexStatistics->reset();

    addBloomFilterEntry();

    recordPosition();
  }

  void ColumnWriter::addBloomFilterEntry() {
    if (enableBloomFilter) {
      BloomFilterUTF8Utils::serialize(*bloomFilter, *bloomFilterIndex->add_bloomfilter());
      bloomFilter->reset();
    }
  }

  void ColumnWriter::writeIndex(std::vector<proto::Stream>& streams) const {
    if (!hasNullValue) {
      // remove positions of present stream
      int presentCount = indexStream->isCompressed() ? 4 : 3;
      for (int i = 0; i != rowIndex->entry_size(); ++i) {
        proto::RowIndexEntry* entry = rowIndex->mutable_entry(i);
        std::vector<uint64_t> positions;
        for (int j = presentCount; j < entry->positions_size(); ++j) {
          positions.push_back(entry->positions(j));
        }
        entry->clear_positions();
        for (size_t j = 0; j != positions.size(); ++j) {
          entry->add_positions(positions[j]);
        }
      }
    }
    // write row index to output stream
    rowIndex->SerializeToZeroCopyStream(indexStream.get());

    // construct row index stream
    proto::Stream stream;
    stream.set_kind(proto::Stream_Kind_ROW_INDEX);
    stream.set_column(static_cast<uint32_t>(columnId));
    stream.set_length(indexStream->flush());
    streams.push_back(stream);

    // write BLOOM_FILTER_UTF8 stream
    if (enableBloomFilter) {
      if (!bloomFilterIndex->SerializeToZeroCopyStream(bloomFilterStream.get())) {
        throw std::logic_error("Failed to write bloom filter stream.");
      }
      stream.set_kind(proto::Stream_Kind_BLOOM_FILTER_UTF8);
      stream.set_column(static_cast<uint32_t>(columnId));
      stream.set_length(bloomFilterStream->flush());
      streams.push_back(stream);
    }
  }

  void ColumnWriter::recordPosition() const {
    notNullEncoder->recordPosition(rowIndexPosition.get());
  }

  void ColumnWriter::reset() {
    if (enableIndex) {
      // clear row index
      rowIndex->clear_entry();
      rowIndexEntry->clear_positions();
      rowIndexEntry->clear_statistics();

      // write current positions
      recordPosition();
    }

    if (enableBloomFilter) {
      bloomFilter->reset();
      bloomFilterIndex->clear_bloomfilter();
    }
  }

  void ColumnWriter::writeDictionary() {
    // PASS
  }

  class StructColumnWriter : public ColumnWriter {
   public:
    StructColumnWriter(const Type& type, const StreamsFactory& factory,
                       const WriterOptions& options);

    virtual void add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                     const char* incomingMask) override;

    virtual void flush(std::vector<proto::Stream>& streams) override;

    virtual uint64_t getEstimatedSize() const override;
    virtual void getColumnEncoding(std::vector<proto::ColumnEncoding>& encodings) const override;

    virtual void getStripeStatistics(std::vector<proto::ColumnStatistics>& stats) const override;

    virtual void getFileStatistics(std::vector<proto::ColumnStatistics>& stats) const override;

    virtual void mergeStripeStatsIntoFileStats() override;

    virtual void mergeRowGroupStatsIntoStripeStats() override;

    virtual void createRowIndexEntry() override;

    virtual void writeIndex(std::vector<proto::Stream>& streams) const override;

    virtual void writeDictionary() override;

    virtual void reset() override;

   private:
    std::vector<std::unique_ptr<ColumnWriter>> children;
  };

  StructColumnWriter::StructColumnWriter(const Type& type, const StreamsFactory& factory,
                                         const WriterOptions& options)
      : ColumnWriter(type, factory, options) {
    for (unsigned int i = 0; i < type.getSubtypeCount(); ++i) {
      const Type& child = *type.getSubtype(i);
      children.push_back(buildWriter(child, factory, options));
    }

    if (enableIndex) {
      recordPosition();
    }
  }

  void StructColumnWriter::add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                               const char* incomingMask) {
    const StructVectorBatch* structBatch = dynamic_cast<const StructVectorBatch*>(&rowBatch);
    if (structBatch == nullptr) {
      throw InvalidArgument("Failed to cast to StructVectorBatch");
    }

    ColumnWriter::add(rowBatch, offset, numValues, incomingMask);
    const char* notNull = structBatch->hasNulls ? structBatch->notNull.data() + offset : nullptr;
    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->add(*structBatch->fields[i], offset, numValues, notNull);
    }

    // update stats
    if (!notNull) {
      colIndexStatistics->increase(numValues);
    } else {
      uint64_t count = 0;
      for (uint64_t i = 0; i < numValues; ++i) {
        if (notNull[i]) {
          ++count;
        }
      }
      colIndexStatistics->increase(count);
      if (count < numValues) {
        colIndexStatistics->setHasNull(true);
      }
    }
  }

  void StructColumnWriter::flush(std::vector<proto::Stream>& streams) {
    ColumnWriter::flush(streams);
    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->flush(streams);
    }
  }

  void StructColumnWriter::writeIndex(std::vector<proto::Stream>& streams) const {
    ColumnWriter::writeIndex(streams);
    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->writeIndex(streams);
    }
  }

  uint64_t StructColumnWriter::getEstimatedSize() const {
    uint64_t size = ColumnWriter::getEstimatedSize();
    for (uint32_t i = 0; i < children.size(); ++i) {
      size += children[i]->getEstimatedSize();
    }
    return size;
  }

  void StructColumnWriter::getColumnEncoding(std::vector<proto::ColumnEncoding>& encodings) const {
    proto::ColumnEncoding encoding;
    encoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    encoding.set_dictionarysize(0);
    encodings.push_back(encoding);
    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->getColumnEncoding(encodings);
    }
  }

  void StructColumnWriter::getStripeStatistics(std::vector<proto::ColumnStatistics>& stats) const {
    ColumnWriter::getStripeStatistics(stats);

    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->getStripeStatistics(stats);
    }
  }

  void StructColumnWriter::mergeStripeStatsIntoFileStats() {
    ColumnWriter::mergeStripeStatsIntoFileStats();

    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->mergeStripeStatsIntoFileStats();
    }
  }

  void StructColumnWriter::getFileStatistics(std::vector<proto::ColumnStatistics>& stats) const {
    ColumnWriter::getFileStatistics(stats);

    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->getFileStatistics(stats);
    }
  }

  void StructColumnWriter::mergeRowGroupStatsIntoStripeStats() {
    ColumnWriter::mergeRowGroupStatsIntoStripeStats();

    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->mergeRowGroupStatsIntoStripeStats();
    }
  }

  void StructColumnWriter::createRowIndexEntry() {
    ColumnWriter::createRowIndexEntry();

    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->createRowIndexEntry();
    }
  }

  void StructColumnWriter::reset() {
    ColumnWriter::reset();

    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->reset();
    }
  }

  void StructColumnWriter::writeDictionary() {
    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->writeDictionary();
    }
  }

  template <typename BatchType>
  class IntegerColumnWriter : public ColumnWriter {
   public:
    IntegerColumnWriter(const Type& type, const StreamsFactory& factory,
                        const WriterOptions& options);

    virtual void add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                     const char* incomingMask) override;

    virtual void flush(std::vector<proto::Stream>& streams) override;

    virtual uint64_t getEstimatedSize() const override;

    virtual void getColumnEncoding(std::vector<proto::ColumnEncoding>& encodings) const override;

    virtual void recordPosition() const override;

   protected:
    std::unique_ptr<RleEncoder> rleEncoder;

   private:
    RleVersion rleVersion;
  };

  template <typename BatchType>
  IntegerColumnWriter<BatchType>::IntegerColumnWriter(const Type& type,
                                                      const StreamsFactory& factory,
                                                      const WriterOptions& options)
      : ColumnWriter(type, factory, options), rleVersion(options.getRleVersion()) {
    std::unique_ptr<BufferedOutputStream> dataStream =
        factory.createStream(proto::Stream_Kind_DATA);
    rleEncoder = createRleEncoder(std::move(dataStream), true, rleVersion, memPool,
                                  options.getAlignedBitpacking());

    if (enableIndex) {
      recordPosition();
    }
  }

  template <typename BatchType>
  void IntegerColumnWriter<BatchType>::add(ColumnVectorBatch& rowBatch, uint64_t offset,
                                           uint64_t numValues, const char* incomingMask) {
    const BatchType* intBatch = dynamic_cast<const BatchType*>(&rowBatch);
    if (intBatch == nullptr) {
      throw InvalidArgument("Failed to cast to IntegerVectorBatch");
    }
    IntegerColumnStatisticsImpl* intStats =
        dynamic_cast<IntegerColumnStatisticsImpl*>(colIndexStatistics.get());
    if (intStats == nullptr) {
      throw InvalidArgument("Failed to cast to IntegerColumnStatisticsImpl");
    }

    ColumnWriter::add(rowBatch, offset, numValues, incomingMask);

    const auto* data = intBatch->data.data() + offset;
    const char* notNull = intBatch->hasNulls ? intBatch->notNull.data() + offset : nullptr;

    rleEncoder->add(data, numValues, notNull);

    // update stats
    uint64_t count = 0;
    for (uint64_t i = 0; i < numValues; ++i) {
      if (notNull == nullptr || notNull[i]) {
        ++count;
        if (enableBloomFilter) {
          bloomFilter->addLong(static_cast<int64_t>(data[i]));
        }
        intStats->update(static_cast<int64_t>(data[i]), 1);
      }
    }
    intStats->increase(count);
    if (count < numValues) {
      intStats->setHasNull(true);
    }
  }

  template <typename BatchType>
  void IntegerColumnWriter<BatchType>::flush(std::vector<proto::Stream>& streams) {
    ColumnWriter::flush(streams);

    proto::Stream stream;
    stream.set_kind(proto::Stream_Kind_DATA);
    stream.set_column(static_cast<uint32_t>(columnId));
    stream.set_length(rleEncoder->flush());
    streams.push_back(stream);
  }

  template <typename BatchType>
  uint64_t IntegerColumnWriter<BatchType>::getEstimatedSize() const {
    uint64_t size = ColumnWriter::getEstimatedSize();
    size += rleEncoder->getBufferSize();
    return size;
  }

  template <typename BatchType>
  void IntegerColumnWriter<BatchType>::getColumnEncoding(
      std::vector<proto::ColumnEncoding>& encodings) const {
    proto::ColumnEncoding encoding;
    encoding.set_kind(RleVersionMapper(rleVersion));
    encoding.set_dictionarysize(0);
    if (enableBloomFilter) {
      encoding.set_bloomencoding(BloomFilterVersion::UTF8);
    }
    encodings.push_back(encoding);
  }

  template <typename BatchType>
  void IntegerColumnWriter<BatchType>::recordPosition() const {
    ColumnWriter::recordPosition();
    rleEncoder->recordPosition(rowIndexPosition.get());
  }

  template <typename BatchType>
  class ByteColumnWriter : public ColumnWriter {
   public:
    ByteColumnWriter(const Type& type, const StreamsFactory& factory, const WriterOptions& options);

    virtual void add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                     const char* incomingMask) override;

    virtual void flush(std::vector<proto::Stream>& streams) override;

    virtual uint64_t getEstimatedSize() const override;

    virtual void getColumnEncoding(std::vector<proto::ColumnEncoding>& encodings) const override;

    virtual void recordPosition() const override;

   private:
    std::unique_ptr<ByteRleEncoder> byteRleEncoder;
  };

  template <typename BatchType>
  ByteColumnWriter<BatchType>::ByteColumnWriter(const Type& type, const StreamsFactory& factory,
                                                const WriterOptions& options)
      : ColumnWriter(type, factory, options) {
    std::unique_ptr<BufferedOutputStream> dataStream =
        factory.createStream(proto::Stream_Kind_DATA);
    byteRleEncoder = createByteRleEncoder(std::move(dataStream));

    if (enableIndex) {
      recordPosition();
    }
  }

  template <typename BatchType>
  void ByteColumnWriter<BatchType>::add(ColumnVectorBatch& rowBatch, uint64_t offset,
                                        uint64_t numValues, const char* incomingMask) {
    BatchType* byteBatch = dynamic_cast<BatchType*>(&rowBatch);
    if (byteBatch == nullptr) {
      throw InvalidArgument("Failed to cast to IntegerVectorBatch");
    }
    IntegerColumnStatisticsImpl* intStats =
        dynamic_cast<IntegerColumnStatisticsImpl*>(colIndexStatistics.get());
    if (intStats == nullptr) {
      throw InvalidArgument("Failed to cast to IntegerColumnStatisticsImpl");
    }

    ColumnWriter::add(rowBatch, offset, numValues, incomingMask);

    auto* data = byteBatch->data.data() + offset;
    const char* notNull = byteBatch->hasNulls ? byteBatch->notNull.data() + offset : nullptr;

    char* byteData = reinterpret_cast<char*>(data);
    for (uint64_t i = 0; i < numValues; ++i) {
      byteData[i] = static_cast<char>(data[i]);
    }
    byteRleEncoder->add(byteData, numValues, notNull);

    uint64_t count = 0;
    for (uint64_t i = 0; i < numValues; ++i) {
      if (notNull == nullptr || notNull[i]) {
        ++count;
        if (enableBloomFilter) {
          bloomFilter->addLong(data[i]);
        }
        intStats->update(static_cast<int64_t>(byteData[i]), 1);
      }
    }
    intStats->increase(count);
    if (count < numValues) {
      intStats->setHasNull(true);
    }
  }

  template <typename BatchType>
  void ByteColumnWriter<BatchType>::flush(std::vector<proto::Stream>& streams) {
    ColumnWriter::flush(streams);

    proto::Stream stream;
    stream.set_kind(proto::Stream_Kind_DATA);
    stream.set_column(static_cast<uint32_t>(columnId));
    stream.set_length(byteRleEncoder->flush());
    streams.push_back(stream);
  }

  template <typename BatchType>
  uint64_t ByteColumnWriter<BatchType>::getEstimatedSize() const {
    uint64_t size = ColumnWriter::getEstimatedSize();
    size += byteRleEncoder->getBufferSize();
    return size;
  }

  template <typename BatchType>
  void ByteColumnWriter<BatchType>::getColumnEncoding(
      std::vector<proto::ColumnEncoding>& encodings) const {
    proto::ColumnEncoding encoding;
    encoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    encoding.set_dictionarysize(0);
    if (enableBloomFilter) {
      encoding.set_bloomencoding(BloomFilterVersion::UTF8);
    }
    encodings.push_back(encoding);
  }

  template <typename BatchType>
  void ByteColumnWriter<BatchType>::recordPosition() const {
    ColumnWriter::recordPosition();
    byteRleEncoder->recordPosition(rowIndexPosition.get());
  }

  template <typename BatchType>
  class BooleanColumnWriter : public ColumnWriter {
   public:
    BooleanColumnWriter(const Type& type, const StreamsFactory& factory,
                        const WriterOptions& options);

    virtual void add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                     const char* incomingMask) override;

    virtual void flush(std::vector<proto::Stream>& streams) override;

    virtual uint64_t getEstimatedSize() const override;

    virtual void getColumnEncoding(std::vector<proto::ColumnEncoding>& encodings) const override;

    virtual void recordPosition() const override;

   private:
    std::unique_ptr<ByteRleEncoder> rleEncoder;
  };

  template <typename BatchType>
  BooleanColumnWriter<BatchType>::BooleanColumnWriter(const Type& type,
                                                      const StreamsFactory& factory,
                                                      const WriterOptions& options)
      : ColumnWriter(type, factory, options) {
    std::unique_ptr<BufferedOutputStream> dataStream =
        factory.createStream(proto::Stream_Kind_DATA);
    rleEncoder = createBooleanRleEncoder(std::move(dataStream));

    if (enableIndex) {
      recordPosition();
    }
  }

  template <typename BatchType>
  void BooleanColumnWriter<BatchType>::add(ColumnVectorBatch& rowBatch, uint64_t offset,
                                           uint64_t numValues, const char* incomingMask) {
    BatchType* byteBatch = dynamic_cast<BatchType*>(&rowBatch);
    if (byteBatch == nullptr) {
      std::stringstream ss;
      ss << "Failed to cast to " << typeid(BatchType).name();
      throw InvalidArgument(ss.str());
    }
    BooleanColumnStatisticsImpl* boolStats =
        dynamic_cast<BooleanColumnStatisticsImpl*>(colIndexStatistics.get());
    if (boolStats == nullptr) {
      throw InvalidArgument("Failed to cast to BooleanColumnStatisticsImpl");
    }

    ColumnWriter::add(rowBatch, offset, numValues, incomingMask);

    auto* data = byteBatch->data.data() + offset;
    const char* notNull = byteBatch->hasNulls ? byteBatch->notNull.data() + offset : nullptr;

    char* byteData = reinterpret_cast<char*>(data);
    for (uint64_t i = 0; i < numValues; ++i) {
      byteData[i] = static_cast<char>(data[i]);
    }
    rleEncoder->add(byteData, numValues, notNull);

    uint64_t count = 0;
    for (uint64_t i = 0; i < numValues; ++i) {
      if (notNull == nullptr || notNull[i]) {
        ++count;
        if (enableBloomFilter) {
          bloomFilter->addLong(data[i]);
        }
        boolStats->update(byteData[i] != 0, 1);
      }
    }
    boolStats->increase(count);
    if (count < numValues) {
      boolStats->setHasNull(true);
    }
  }

  template <typename BatchType>
  void BooleanColumnWriter<BatchType>::flush(std::vector<proto::Stream>& streams) {
    ColumnWriter::flush(streams);

    proto::Stream stream;
    stream.set_kind(proto::Stream_Kind_DATA);
    stream.set_column(static_cast<uint32_t>(columnId));
    stream.set_length(rleEncoder->flush());
    streams.push_back(stream);
  }

  template <typename BatchType>
  uint64_t BooleanColumnWriter<BatchType>::getEstimatedSize() const {
    uint64_t size = ColumnWriter::getEstimatedSize();
    size += rleEncoder->getBufferSize();
    return size;
  }

  template <typename BatchType>
  void BooleanColumnWriter<BatchType>::getColumnEncoding(
      std::vector<proto::ColumnEncoding>& encodings) const {
    proto::ColumnEncoding encoding;
    encoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    encoding.set_dictionarysize(0);
    if (enableBloomFilter) {
      encoding.set_bloomencoding(BloomFilterVersion::UTF8);
    }
    encodings.push_back(encoding);
  }

  template <typename BatchType>
  void BooleanColumnWriter<BatchType>::recordPosition() const {
    ColumnWriter::recordPosition();
    rleEncoder->recordPosition(rowIndexPosition.get());
  }

  template <typename ValueType, typename BatchType>
  class FloatingColumnWriter : public ColumnWriter {
   public:
    FloatingColumnWriter(const Type& type, const StreamsFactory& factory,
                         const WriterOptions& options, bool isFloat);

    virtual void add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                     const char* incomingMask) override;

    virtual void flush(std::vector<proto::Stream>& streams) override;

    virtual uint64_t getEstimatedSize() const override;

    virtual void getColumnEncoding(std::vector<proto::ColumnEncoding>& encodings) const override;

    virtual void recordPosition() const override;

   private:
    bool isFloat;
    std::unique_ptr<AppendOnlyBufferedStream> dataStream;
    DataBuffer<char> buffer;
  };

  template <typename ValueType, typename BatchType>
  FloatingColumnWriter<ValueType, BatchType>::FloatingColumnWriter(const Type& type,
                                                                   const StreamsFactory& factory,
                                                                   const WriterOptions& options,
                                                                   bool isFloatType)
      : ColumnWriter(type, factory, options),
        isFloat(isFloatType),
        buffer(*options.getMemoryPool()) {
    dataStream.reset(new AppendOnlyBufferedStream(factory.createStream(proto::Stream_Kind_DATA)));
    buffer.resize(isFloat ? 4 : 8);

    if (enableIndex) {
      recordPosition();
    }
  }

  // Floating point types are stored using IEEE 754 floating point bit layout.
  // Float columns use 4 bytes per value and double columns use 8 bytes.
  template <typename FLOAT_TYPE, typename INTEGER_TYPE>
  inline void encodeFloatNum(FLOAT_TYPE input, char* output) {
    INTEGER_TYPE* intBits = reinterpret_cast<INTEGER_TYPE*>(&input);
    for (size_t i = 0; i < sizeof(INTEGER_TYPE); ++i) {
      output[i] = static_cast<char>(((*intBits) >> (8 * i)) & 0xff);
    }
  }

  template <typename ValueType, typename BatchType>
  void FloatingColumnWriter<ValueType, BatchType>::add(ColumnVectorBatch& rowBatch, uint64_t offset,
                                                       uint64_t numValues,
                                                       const char* incomingMask) {
    const BatchType* dblBatch = dynamic_cast<const BatchType*>(&rowBatch);
    if (dblBatch == nullptr) {
      throw InvalidArgument("Failed to cast to FloatingVectorBatch");
    }
    DoubleColumnStatisticsImpl* doubleStats =
        dynamic_cast<DoubleColumnStatisticsImpl*>(colIndexStatistics.get());
    if (doubleStats == nullptr) {
      throw InvalidArgument("Failed to cast to DoubleColumnStatisticsImpl");
    }

    ColumnWriter::add(rowBatch, offset, numValues, incomingMask);

    const ValueType* doubleData = dblBatch->data.data() + offset;
    const char* notNull = dblBatch->hasNulls ? dblBatch->notNull.data() + offset : nullptr;

    size_t bytes = isFloat ? 4 : 8;
    char* data = buffer.data();
    uint64_t count = 0;
    for (uint64_t i = 0; i < numValues; ++i) {
      if (!notNull || notNull[i]) {
        if (isFloat) {
          encodeFloatNum<float, int32_t>(static_cast<float>(doubleData[i]), data);
        } else {
          encodeFloatNum<double, int64_t>(static_cast<double>(doubleData[i]), data);
        }
        dataStream->write(data, bytes);
        ++count;
        if (enableBloomFilter) {
          bloomFilter->addDouble(static_cast<double>(doubleData[i]));
        }
        doubleStats->update(static_cast<double>(doubleData[i]));
      }
    }
    doubleStats->increase(count);
    if (count < numValues) {
      doubleStats->setHasNull(true);
    }
  }

  template <typename ValueType, typename BatchType>
  void FloatingColumnWriter<ValueType, BatchType>::flush(std::vector<proto::Stream>& streams) {
    ColumnWriter::flush(streams);

    proto::Stream stream;
    stream.set_kind(proto::Stream_Kind_DATA);
    stream.set_column(static_cast<uint32_t>(columnId));
    stream.set_length(dataStream->flush());
    streams.push_back(stream);
  }

  template <typename ValueType, typename BatchType>
  uint64_t FloatingColumnWriter<ValueType, BatchType>::getEstimatedSize() const {
    uint64_t size = ColumnWriter::getEstimatedSize();
    size += dataStream->getSize();
    return size;
  }

  template <typename ValueType, typename BatchType>
  void FloatingColumnWriter<ValueType, BatchType>::getColumnEncoding(
      std::vector<proto::ColumnEncoding>& encodings) const {
    proto::ColumnEncoding encoding;
    encoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    encoding.set_dictionarysize(0);
    if (enableBloomFilter) {
      encoding.set_bloomencoding(BloomFilterVersion::UTF8);
    }
    encodings.push_back(encoding);
  }

  template <typename ValueType, typename BatchType>
  void FloatingColumnWriter<ValueType, BatchType>::recordPosition() const {
    ColumnWriter::recordPosition();
    dataStream->recordPosition(rowIndexPosition.get());
  }

  /**
   * Implementation of increasing sorted string dictionary
   */
  class SortedStringDictionary {
   public:
    struct DictEntry {
      DictEntry(const char* str, size_t len) : data(str), length(len) {}
      const char* data;
      size_t length;
    };

    SortedStringDictionary() : totalLength(0) {}

    // insert a new string into dictionary, return its insertion order
    size_t insert(const char* data, size_t len);

    // write dictionary data & length to output buffer
    void flush(AppendOnlyBufferedStream* dataStream, RleEncoder* lengthEncoder) const;

    // reorder input index buffer from insertion order to dictionary order
    void reorder(std::vector<int64_t>& idxBuffer) const;

    // get dict entries in insertion order
    void getEntriesInInsertionOrder(std::vector<const DictEntry*>&) const;

    // return count of entries
    size_t size() const;

    // return total length of strings in the dictioanry
    uint64_t length() const;

    void clear();

   private:
    struct LessThan {
      bool operator()(const DictEntry& left, const DictEntry& right) const {
        int ret = memcmp(left.data, right.data, std::min(left.length, right.length));
        if (ret != 0) {
          return ret < 0;
        }
        return left.length < right.length;
      }
    };

    std::map<DictEntry, size_t, LessThan> dict;
    std::vector<std::vector<char>> data;
    uint64_t totalLength;

    // use friend class here to avoid being bothered by const function calls
    friend class StringColumnWriter;
    friend class CharColumnWriter;
    friend class VarCharColumnWriter;
    // store indexes of insertion order in the dictionary for not-null rows
    std::vector<int64_t> idxInDictBuffer;
  };

  // insert a new string into dictionary, return its insertion order
  size_t SortedStringDictionary::insert(const char* str, size_t len) {
    auto ret = dict.insert({DictEntry(str, len), dict.size()});
    if (ret.second) {
      // make a copy to internal storage
      data.push_back(std::vector<char>(len));
      memcpy(data.back().data(), str, len);
      // update dictionary entry to link pointer to internal storage
      DictEntry* entry = const_cast<DictEntry*>(&(ret.first->first));
      entry->data = data.back().data();
      totalLength += len;
    }
    return ret.first->second;
  }

  // write dictionary data & length to output buffer
  void SortedStringDictionary::flush(AppendOnlyBufferedStream* dataStream,
                                     RleEncoder* lengthEncoder) const {
    for (auto it = dict.cbegin(); it != dict.cend(); ++it) {
      dataStream->write(it->first.data, it->first.length);
      lengthEncoder->write(static_cast<int64_t>(it->first.length));
    }
  }

  /**
   * Reorder input index buffer from insertion order to dictionary order
   *
   * We require this function because string values are buffered by indexes
   * in their insertion order. Until the entire dictionary is complete can
   * we get their sorted indexes in the dictionary in that ORC specification
   * demands dictionary should be ordered. Therefore this function transforms
   * the indexes from insertion order to dictionary value order for final
   * output.
   */
  void SortedStringDictionary::reorder(std::vector<int64_t>& idxBuffer) const {
    // iterate the dictionary to get mapping from insertion order to value order
    std::vector<size_t> mapping(dict.size());
    size_t dictIdx = 0;
    for (auto it = dict.cbegin(); it != dict.cend(); ++it) {
      mapping[it->second] = dictIdx++;
    }

    // do the transformation
    for (size_t i = 0; i != idxBuffer.size(); ++i) {
      idxBuffer[i] = static_cast<int64_t>(mapping[static_cast<size_t>(idxBuffer[i])]);
    }
  }

  // get dict entries in insertion order
  void SortedStringDictionary::getEntriesInInsertionOrder(
      std::vector<const DictEntry*>& entries) const {
    entries.resize(dict.size());
    for (auto it = dict.cbegin(); it != dict.cend(); ++it) {
      entries[it->second] = &(it->first);
    }
  }

  // return count of entries
  size_t SortedStringDictionary::size() const {
    return dict.size();
  }

  // return total length of strings in the dictioanry
  uint64_t SortedStringDictionary::length() const {
    return totalLength;
  }

  void SortedStringDictionary::clear() {
    totalLength = 0;
    data.clear();
    dict.clear();
  }

  class StringColumnWriter : public ColumnWriter {
   public:
    StringColumnWriter(const Type& type, const StreamsFactory& factory,
                       const WriterOptions& options);

    virtual void add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                     const char* incomingMask) override;

    virtual void flush(std::vector<proto::Stream>& streams) override;

    virtual uint64_t getEstimatedSize() const override;

    virtual void getColumnEncoding(std::vector<proto::ColumnEncoding>& encodings) const override;

    virtual void recordPosition() const override;

    virtual void createRowIndexEntry() override;

    virtual void writeDictionary() override;

    virtual void reset() override;

   private:
    /**
     * dictionary related functions
     */
    bool checkDictionaryKeyRatio();
    void createDirectStreams();
    void createDictStreams();
    void deleteDictStreams();
    void fallbackToDirectEncoding();

   protected:
    RleVersion rleVersion;
    bool useCompression;
    const StreamsFactory& streamsFactory;
    bool alignedBitPacking;

    // direct encoding streams
    std::unique_ptr<RleEncoder> directLengthEncoder;
    std::unique_ptr<AppendOnlyBufferedStream> directDataStream;

    // dictionary encoding streams
    std::unique_ptr<RleEncoder> dictDataEncoder;
    std::unique_ptr<RleEncoder> dictLengthEncoder;
    std::unique_ptr<AppendOnlyBufferedStream> dictStream;

    /**
     * dictionary related variables
     */
    SortedStringDictionary dictionary;
    // whether or not dictionary checking is done
    bool doneDictionaryCheck;
    // whether or not it should be used
    bool useDictionary;
    // keys in the dictionary should not exceed this ratio
    double dictSizeThreshold;

    // record start row of each row group; null rows are skipped
    mutable std::vector<size_t> startOfRowGroups;
  };

  StringColumnWriter::StringColumnWriter(const Type& type, const StreamsFactory& factory,
                                         const WriterOptions& options)
      : ColumnWriter(type, factory, options),
        rleVersion(options.getRleVersion()),
        useCompression(options.getCompression() != CompressionKind_NONE),
        streamsFactory(factory),
        alignedBitPacking(options.getAlignedBitpacking()),
        doneDictionaryCheck(false),
        useDictionary(options.getEnableDictionary()),
        dictSizeThreshold(options.getDictionaryKeySizeThreshold()) {
    if (type.getKind() == TypeKind::BINARY) {
      useDictionary = false;
      doneDictionaryCheck = true;
    }

    if (useDictionary) {
      createDictStreams();
    } else {
      doneDictionaryCheck = true;
      createDirectStreams();
    }

    if (enableIndex) {
      recordPosition();
    }
  }

  void StringColumnWriter::add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                               const char* incomingMask) {
    const StringVectorBatch* stringBatch = dynamic_cast<const StringVectorBatch*>(&rowBatch);
    if (stringBatch == nullptr) {
      throw InvalidArgument("Failed to cast to StringVectorBatch");
    }

    StringColumnStatisticsImpl* strStats =
        dynamic_cast<StringColumnStatisticsImpl*>(colIndexStatistics.get());
    if (strStats == nullptr) {
      throw InvalidArgument("Failed to cast to StringColumnStatisticsImpl");
    }

    ColumnWriter::add(rowBatch, offset, numValues, incomingMask);

    char* const* data = stringBatch->data.data() + offset;
    const int64_t* length = stringBatch->length.data() + offset;
    const char* notNull = stringBatch->hasNulls ? stringBatch->notNull.data() + offset : nullptr;

    if (!useDictionary) {
      directLengthEncoder->add(length, numValues, notNull);
    }

    uint64_t count = 0;
    for (uint64_t i = 0; i < numValues; ++i) {
      if (!notNull || notNull[i]) {
        const size_t len = static_cast<size_t>(length[i]);
        if (useDictionary) {
          size_t index = dictionary.insert(data[i], len);
          dictionary.idxInDictBuffer.push_back(static_cast<int64_t>(index));
        } else {
          directDataStream->write(data[i], len);
        }
        if (enableBloomFilter) {
          bloomFilter->addBytes(data[i], static_cast<int64_t>(len));
        }
        strStats->update(data[i], len);
        ++count;
      }
    }
    strStats->increase(count);
    if (count < numValues) {
      strStats->setHasNull(true);
    }
  }

  void StringColumnWriter::flush(std::vector<proto::Stream>& streams) {
    ColumnWriter::flush(streams);

    if (useDictionary) {
      proto::Stream data;
      data.set_kind(proto::Stream_Kind_DATA);
      data.set_column(static_cast<uint32_t>(columnId));
      data.set_length(dictDataEncoder->flush());
      streams.push_back(data);

      proto::Stream dict;
      dict.set_kind(proto::Stream_Kind_DICTIONARY_DATA);
      dict.set_column(static_cast<uint32_t>(columnId));
      dict.set_length(dictStream->flush());
      streams.push_back(dict);

      proto::Stream length;
      length.set_kind(proto::Stream_Kind_LENGTH);
      length.set_column(static_cast<uint32_t>(columnId));
      length.set_length(dictLengthEncoder->flush());
      streams.push_back(length);
    } else {
      proto::Stream length;
      length.set_kind(proto::Stream_Kind_LENGTH);
      length.set_column(static_cast<uint32_t>(columnId));
      length.set_length(directLengthEncoder->flush());
      streams.push_back(length);

      proto::Stream data;
      data.set_kind(proto::Stream_Kind_DATA);
      data.set_column(static_cast<uint32_t>(columnId));
      data.set_length(directDataStream->flush());
      streams.push_back(data);
    }
  }

  uint64_t StringColumnWriter::getEstimatedSize() const {
    uint64_t size = ColumnWriter::getEstimatedSize();
    if (!useDictionary) {
      size += directLengthEncoder->getBufferSize();
      size += directDataStream->getSize();
    } else {
      size += dictionary.length();
      size += dictionary.size() * sizeof(int32_t);
      size += dictionary.idxInDictBuffer.size() * sizeof(int32_t);
      if (useCompression) {
        size /= 3;  // estimated ratio is 3:1
      }
    }
    return size;
  }

  void StringColumnWriter::getColumnEncoding(std::vector<proto::ColumnEncoding>& encodings) const {
    proto::ColumnEncoding encoding;
    if (!useDictionary) {
      encoding.set_kind(rleVersion == RleVersion_1 ? proto::ColumnEncoding_Kind_DIRECT
                                                   : proto::ColumnEncoding_Kind_DIRECT_V2);
    } else {
      encoding.set_kind(rleVersion == RleVersion_1 ? proto::ColumnEncoding_Kind_DICTIONARY
                                                   : proto::ColumnEncoding_Kind_DICTIONARY_V2);
    }
    encoding.set_dictionarysize(static_cast<uint32_t>(dictionary.size()));
    if (enableBloomFilter) {
      encoding.set_bloomencoding(BloomFilterVersion::UTF8);
    }
    encodings.push_back(encoding);
  }

  void StringColumnWriter::recordPosition() const {
    ColumnWriter::recordPosition();
    if (!useDictionary) {
      directDataStream->recordPosition(rowIndexPosition.get());
      directLengthEncoder->recordPosition(rowIndexPosition.get());
    } else {
      if (enableIndex) {
        startOfRowGroups.push_back(dictionary.idxInDictBuffer.size());
      }
    }
  }

  bool StringColumnWriter::checkDictionaryKeyRatio() {
    if (!doneDictionaryCheck) {
      useDictionary = dictionary.size() <=
                      static_cast<size_t>(static_cast<double>(dictionary.idxInDictBuffer.size()) *
                                          dictSizeThreshold);
      doneDictionaryCheck = true;
    }

    return useDictionary;
  }

  void StringColumnWriter::createRowIndexEntry() {
    if (useDictionary && !doneDictionaryCheck) {
      if (!checkDictionaryKeyRatio()) {
        fallbackToDirectEncoding();
      }
    }
    ColumnWriter::createRowIndexEntry();
  }

  void StringColumnWriter::reset() {
    ColumnWriter::reset();

    dictionary.clear();
    dictionary.idxInDictBuffer.resize(0);
    startOfRowGroups.clear();
    startOfRowGroups.push_back(0);
  }

  void StringColumnWriter::createDirectStreams() {
    std::unique_ptr<BufferedOutputStream> directLengthStream =
        streamsFactory.createStream(proto::Stream_Kind_LENGTH);
    directLengthEncoder = createRleEncoder(std::move(directLengthStream), false, rleVersion,
                                           memPool, alignedBitPacking);
    directDataStream.reset(
        new AppendOnlyBufferedStream(streamsFactory.createStream(proto::Stream_Kind_DATA)));
  }

  void StringColumnWriter::createDictStreams() {
    std::unique_ptr<BufferedOutputStream> dictDataStream =
        streamsFactory.createStream(proto::Stream_Kind_DATA);
    dictDataEncoder =
        createRleEncoder(std::move(dictDataStream), false, rleVersion, memPool, alignedBitPacking);
    std::unique_ptr<BufferedOutputStream> dictLengthStream =
        streamsFactory.createStream(proto::Stream_Kind_LENGTH);
    dictLengthEncoder = createRleEncoder(std::move(dictLengthStream), false, rleVersion, memPool,
                                         alignedBitPacking);
    dictStream.reset(new AppendOnlyBufferedStream(
        streamsFactory.createStream(proto::Stream_Kind_DICTIONARY_DATA)));
  }

  void StringColumnWriter::deleteDictStreams() {
    dictDataEncoder.reset(nullptr);
    dictLengthEncoder.reset(nullptr);
    dictStream.reset(nullptr);

    dictionary.clear();
    dictionary.idxInDictBuffer.clear();
    startOfRowGroups.clear();
  }

  void StringColumnWriter::writeDictionary() {
    if (useDictionary && !doneDictionaryCheck) {
      // when index is disabled, dictionary check happens while writing 1st stripe
      if (!checkDictionaryKeyRatio()) {
        fallbackToDirectEncoding();
        return;
      }
    }

    if (useDictionary) {
      // flush dictionary data & length streams
      dictionary.flush(dictStream.get(), dictLengthEncoder.get());

      // convert index from insertion order to dictionary order
      dictionary.reorder(dictionary.idxInDictBuffer);

      // write data sequences
      int64_t* data = dictionary.idxInDictBuffer.data();
      if (enableIndex) {
        size_t prevOffset = 0;
        for (size_t i = 0; i < startOfRowGroups.size(); ++i) {
          // write sequences in batch for a row group stride
          size_t offset = startOfRowGroups[i];
          dictDataEncoder->add(data + prevOffset, offset - prevOffset, nullptr);

          // update index positions
          int rowGroupId = static_cast<int>(i);
          proto::RowIndexEntry* indexEntry = (rowGroupId < rowIndex->entry_size())
                                                 ? rowIndex->mutable_entry(rowGroupId)
                                                 : rowIndexEntry.get();

          // add positions for direct streams
          RowIndexPositionRecorder recorder(*indexEntry);
          dictDataEncoder->recordPosition(&recorder);

          prevOffset = offset;
        }

        dictDataEncoder->add(data + prevOffset, dictionary.idxInDictBuffer.size() - prevOffset,
                             nullptr);
      } else {
        dictDataEncoder->add(data, dictionary.idxInDictBuffer.size(), nullptr);
      }
    }
  }

  void StringColumnWriter::fallbackToDirectEncoding() {
    createDirectStreams();

    if (enableIndex) {
      // fallback happens at the 1st row group;
      // simply complete positions for direct streams
      proto::RowIndexEntry* indexEntry = rowIndexEntry.get();
      RowIndexPositionRecorder recorder(*indexEntry);
      directDataStream->recordPosition(&recorder);
      directLengthEncoder->recordPosition(&recorder);
    }

    // get dictionary entries in insertion order
    std::vector<const SortedStringDictionary::DictEntry*> entries;
    dictionary.getEntriesInInsertionOrder(entries);

    // store each length of the data into a vector
    const SortedStringDictionary::DictEntry* dictEntry = nullptr;
    for (uint64_t i = 0; i != dictionary.idxInDictBuffer.size(); ++i) {
      // write one row data in direct encoding
      dictEntry = entries[static_cast<size_t>(dictionary.idxInDictBuffer[i])];
      directDataStream->write(dictEntry->data, dictEntry->length);
      directLengthEncoder->write(static_cast<int64_t>(dictEntry->length));
    }

    deleteDictStreams();
  }

  struct Utf8Utils {
    /**
     * Counts how many utf-8 chars of the input data
     */
    static uint64_t charLength(const char* data, uint64_t length) {
      uint64_t chars = 0;
      for (uint64_t i = 0; i < length; i++) {
        if (isUtfStartByte(data[i])) {
          chars++;
        }
      }
      return chars;
    }

    /**
     * Return the number of bytes required to read at most maxCharLength
     * characters in full from a utf-8 encoded byte array provided
     * by data. This does not validate utf-8 data, but
     * operates correctly on already valid utf-8 data.
     *
     * @param maxCharLength number of characters required
     * @param data the bytes of UTF-8
     * @param length the length of data to truncate
     */
    static uint64_t truncateBytesTo(uint64_t maxCharLength, const char* data, uint64_t length) {
      uint64_t chars = 0;
      if (length <= maxCharLength) {
        return length;
      }
      for (uint64_t i = 0; i < length; i++) {
        if (isUtfStartByte(data[i])) {
          chars++;
        }
        if (chars > maxCharLength) {
          return i;
        }
      }
      // everything fits
      return length;
    }

    /**
     * Checks if b is the first byte of a UTF-8 character.
     */
    inline static bool isUtfStartByte(char b) {
      return (b & 0xC0) != 0x80;
    }

    /**
     * Find the start of the last character that ends in the current string.
     * @param text the bytes of the utf-8
     * @param from the first byte location
     * @param until the last byte location
     * @return the index of the last character
     */
    static uint64_t findLastCharacter(const char* text, uint64_t from, uint64_t until) {
      uint64_t posn = until;
      /* we don't expect characters more than 5 bytes */
      while (posn >= from) {
        if (isUtfStartByte(text[posn])) {
          return posn;
        }
        posn -= 1;
      }
      /* beginning of a valid char not found */
      throw std::logic_error("Could not truncate string, beginning of a valid char not found");
    }
  };

  class CharColumnWriter : public StringColumnWriter {
   public:
    CharColumnWriter(const Type& type, const StreamsFactory& factory, const WriterOptions& options)
        : StringColumnWriter(type, factory, options),
          maxLength(type.getMaximumLength()),
          padBuffer(*options.getMemoryPool()) {
      // utf-8 is currently 4 bytes long, but it could be up to 6
      padBuffer.resize(maxLength * 6);
    }

    virtual void add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                     const char* incomingMask) override;

   private:
    uint64_t maxLength;
    DataBuffer<char> padBuffer;
  };

  void CharColumnWriter::add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                             const char* incomingMask) {
    StringVectorBatch* charsBatch = dynamic_cast<StringVectorBatch*>(&rowBatch);
    if (charsBatch == nullptr) {
      throw InvalidArgument("Failed to cast to StringVectorBatch");
    }

    StringColumnStatisticsImpl* strStats =
        dynamic_cast<StringColumnStatisticsImpl*>(colIndexStatistics.get());
    if (strStats == nullptr) {
      throw InvalidArgument("Failed to cast to StringColumnStatisticsImpl");
    }

    ColumnWriter::add(rowBatch, offset, numValues, incomingMask);

    char** data = charsBatch->data.data() + offset;
    int64_t* length = charsBatch->length.data() + offset;
    const char* notNull = charsBatch->hasNulls ? charsBatch->notNull.data() + offset : nullptr;

    uint64_t count = 0;
    for (uint64_t i = 0; i < numValues; ++i) {
      if (!notNull || notNull[i]) {
        const char* charData = nullptr;
        uint64_t originLength = static_cast<uint64_t>(length[i]);
        uint64_t charLength = Utf8Utils::charLength(data[i], originLength);
        if (charLength >= maxLength) {
          charData = data[i];
          length[i] =
              static_cast<int64_t>(Utf8Utils::truncateBytesTo(maxLength, data[i], originLength));
        } else {
          charData = padBuffer.data();
          // the padding is exactly 1 byte per char
          length[i] = length[i] + static_cast<int64_t>(maxLength - charLength);
          memcpy(padBuffer.data(), data[i], originLength);
          memset(padBuffer.data() + originLength, ' ',
                 static_cast<size_t>(length[i]) - originLength);
        }

        if (useDictionary) {
          size_t index = dictionary.insert(charData, static_cast<size_t>(length[i]));
          dictionary.idxInDictBuffer.push_back(static_cast<int64_t>(index));
        } else {
          directDataStream->write(charData, static_cast<size_t>(length[i]));
        }

        if (enableBloomFilter) {
          bloomFilter->addBytes(data[i], length[i]);
        }
        strStats->update(charData, static_cast<size_t>(length[i]));
        ++count;
      }
    }

    if (!useDictionary) {
      directLengthEncoder->add(length, numValues, notNull);
    }

    strStats->increase(count);
    if (count < numValues) {
      strStats->setHasNull(true);
    }
  }

  class VarCharColumnWriter : public StringColumnWriter {
   public:
    VarCharColumnWriter(const Type& type, const StreamsFactory& factory,
                        const WriterOptions& options)
        : StringColumnWriter(type, factory, options), maxLength(type.getMaximumLength()) {
      // PASS
    }

    virtual void add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                     const char* incomingMask) override;

   private:
    uint64_t maxLength;
  };

  void VarCharColumnWriter::add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                                const char* incomingMask) {
    StringVectorBatch* charsBatch = dynamic_cast<StringVectorBatch*>(&rowBatch);
    if (charsBatch == nullptr) {
      throw InvalidArgument("Failed to cast to StringVectorBatch");
    }

    StringColumnStatisticsImpl* strStats =
        dynamic_cast<StringColumnStatisticsImpl*>(colIndexStatistics.get());
    if (strStats == nullptr) {
      throw InvalidArgument("Failed to cast to StringColumnStatisticsImpl");
    }

    ColumnWriter::add(rowBatch, offset, numValues, incomingMask);

    char* const* data = charsBatch->data.data() + offset;
    int64_t* length = charsBatch->length.data() + offset;
    const char* notNull = charsBatch->hasNulls ? charsBatch->notNull.data() + offset : nullptr;

    uint64_t count = 0;
    for (uint64_t i = 0; i < numValues; ++i) {
      if (!notNull || notNull[i]) {
        uint64_t itemLength =
            Utf8Utils::truncateBytesTo(maxLength, data[i], static_cast<uint64_t>(length[i]));
        length[i] = static_cast<int64_t>(itemLength);

        if (useDictionary) {
          size_t index = dictionary.insert(data[i], static_cast<size_t>(length[i]));
          dictionary.idxInDictBuffer.push_back(static_cast<int64_t>(index));
        } else {
          directDataStream->write(data[i], static_cast<size_t>(length[i]));
        }

        if (enableBloomFilter) {
          bloomFilter->addBytes(data[i], length[i]);
        }
        strStats->update(data[i], static_cast<size_t>(length[i]));
        ++count;
      }
    }

    if (!useDictionary) {
      directLengthEncoder->add(length, numValues, notNull);
    }

    strStats->increase(count);
    if (count < numValues) {
      strStats->setHasNull(true);
    }
  }

  class BinaryColumnWriter : public StringColumnWriter {
   public:
    BinaryColumnWriter(const Type& type, const StreamsFactory& factory,
                       const WriterOptions& options)
        : StringColumnWriter(type, factory, options) {
      // PASS
    }

    virtual void add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                     const char* incomingMask) override;
  };

  void BinaryColumnWriter::add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                               const char* incomingMask) {
    StringVectorBatch* binBatch = dynamic_cast<StringVectorBatch*>(&rowBatch);
    if (binBatch == nullptr) {
      throw InvalidArgument("Failed to cast to StringVectorBatch");
    }

    BinaryColumnStatisticsImpl* binStats =
        dynamic_cast<BinaryColumnStatisticsImpl*>(colIndexStatistics.get());
    if (binStats == nullptr) {
      throw InvalidArgument("Failed to cast to BinaryColumnStatisticsImpl");
    }

    ColumnWriter::add(rowBatch, offset, numValues, incomingMask);

    char** data = binBatch->data.data() + offset;
    int64_t* length = binBatch->length.data() + offset;
    const char* notNull = binBatch->hasNulls ? binBatch->notNull.data() + offset : nullptr;

    uint64_t count = 0;
    for (uint64_t i = 0; i < numValues; ++i) {
      uint64_t unsignedLength = static_cast<uint64_t>(length[i]);
      if (!notNull || notNull[i]) {
        directDataStream->write(data[i], unsignedLength);

        if (enableBloomFilter) {
          bloomFilter->addBytes(data[i], length[i]);
        }
        binStats->update(unsignedLength);
        ++count;
      }
    }
    directLengthEncoder->add(length, numValues, notNull);
    binStats->increase(count);
    if (count < numValues) {
      binStats->setHasNull(true);
    }
  }

  class TimestampColumnWriter : public ColumnWriter {
   public:
    TimestampColumnWriter(const Type& type, const StreamsFactory& factory,
                          const WriterOptions& options, bool isInstantType);

    virtual void add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                     const char* incomingMask) override;

    virtual void flush(std::vector<proto::Stream>& streams) override;

    virtual uint64_t getEstimatedSize() const override;

    virtual void getColumnEncoding(std::vector<proto::ColumnEncoding>& encodings) const override;

    virtual void recordPosition() const override;

   protected:
    std::unique_ptr<RleEncoder> secRleEncoder, nanoRleEncoder;

   private:
    RleVersion rleVersion;
    const Timezone& timezone;
    const bool isUTC;
  };

  TimestampColumnWriter::TimestampColumnWriter(const Type& type, const StreamsFactory& factory,
                                               const WriterOptions& options, bool isInstantType)
      : ColumnWriter(type, factory, options),
        rleVersion(options.getRleVersion()),
        timezone(isInstantType ? getTimezoneByName("GMT") : options.getTimezone()),
        isUTC(isInstantType || options.getTimezoneName() == "GMT") {
    std::unique_ptr<BufferedOutputStream> dataStream =
        factory.createStream(proto::Stream_Kind_DATA);
    std::unique_ptr<BufferedOutputStream> secondaryStream =
        factory.createStream(proto::Stream_Kind_SECONDARY);
    secRleEncoder = createRleEncoder(std::move(dataStream), true, rleVersion, memPool,
                                     options.getAlignedBitpacking());
    nanoRleEncoder = createRleEncoder(std::move(secondaryStream), false, rleVersion, memPool,
                                      options.getAlignedBitpacking());

    if (enableIndex) {
      recordPosition();
    }
  }

  // Because the number of nanoseconds often has a large number of trailing zeros,
  // the number has trailing decimal zero digits removed and the last three bits
  // are used to record how many zeros were removed if the trailing zeros are
  // more than 2. Thus 1000 nanoseconds would be serialized as 0x0a and
  // 100000 would be serialized as 0x0c.
  static int64_t formatNano(int64_t nanos) {
    if (nanos == 0) {
      return 0;
    } else if (nanos % 100 != 0) {
      return (nanos) << 3;
    } else {
      nanos /= 100;
      int64_t trailingZeros = 1;
      while (nanos % 10 == 0 && trailingZeros < 7) {
        nanos /= 10;
        trailingZeros += 1;
      }
      return (nanos) << 3 | trailingZeros;
    }
  }

  void TimestampColumnWriter::add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                                  const char* incomingMask) {
    TimestampVectorBatch* tsBatch = dynamic_cast<TimestampVectorBatch*>(&rowBatch);
    if (tsBatch == nullptr) {
      throw InvalidArgument("Failed to cast to TimestampVectorBatch");
    }

    TimestampColumnStatisticsImpl* tsStats =
        dynamic_cast<TimestampColumnStatisticsImpl*>(colIndexStatistics.get());
    if (tsStats == nullptr) {
      throw InvalidArgument("Failed to cast to TimestampColumnStatisticsImpl");
    }

    ColumnWriter::add(rowBatch, offset, numValues, incomingMask);

    const char* notNull = tsBatch->hasNulls ? tsBatch->notNull.data() + offset : nullptr;
    int64_t* secs = tsBatch->data.data() + offset;
    int64_t* nanos = tsBatch->nanoseconds.data() + offset;

    uint64_t count = 0;
    for (uint64_t i = 0; i < numValues; ++i) {
      if (notNull == nullptr || notNull[i]) {
        // TimestampVectorBatch already stores data in UTC
        int64_t millsUTC = secs[i] * 1000 + nanos[i] / 1000000;
        if (!isUTC) {
          millsUTC = timezone.convertToUTC(secs[i]) * 1000 + nanos[i] / 1000000;
        }
        ++count;
        if (enableBloomFilter) {
          bloomFilter->addLong(millsUTC);
        }
        tsStats->update(millsUTC, static_cast<int32_t>(nanos[i] % 1000000));

        if (secs[i] < 0 && nanos[i] > 999999) {
          secs[i] += 1;
        }

        secs[i] -= timezone.getEpoch();
        nanos[i] = formatNano(nanos[i]);
      }
    }
    tsStats->increase(count);
    if (count < numValues) {
      tsStats->setHasNull(true);
    }

    secRleEncoder->add(secs, numValues, notNull);
    nanoRleEncoder->add(nanos, numValues, notNull);
  }

  void TimestampColumnWriter::flush(std::vector<proto::Stream>& streams) {
    ColumnWriter::flush(streams);

    proto::Stream dataStream;
    dataStream.set_kind(proto::Stream_Kind_DATA);
    dataStream.set_column(static_cast<uint32_t>(columnId));
    dataStream.set_length(secRleEncoder->flush());
    streams.push_back(dataStream);

    proto::Stream secondaryStream;
    secondaryStream.set_kind(proto::Stream_Kind_SECONDARY);
    secondaryStream.set_column(static_cast<uint32_t>(columnId));
    secondaryStream.set_length(nanoRleEncoder->flush());
    streams.push_back(secondaryStream);
  }

  uint64_t TimestampColumnWriter::getEstimatedSize() const {
    uint64_t size = ColumnWriter::getEstimatedSize();
    size += secRleEncoder->getBufferSize();
    size += nanoRleEncoder->getBufferSize();
    return size;
  }

  void TimestampColumnWriter::getColumnEncoding(
      std::vector<proto::ColumnEncoding>& encodings) const {
    proto::ColumnEncoding encoding;
    encoding.set_kind(RleVersionMapper(rleVersion));
    encoding.set_dictionarysize(0);
    if (enableBloomFilter) {
      encoding.set_bloomencoding(BloomFilterVersion::UTF8);
    }
    encodings.push_back(encoding);
  }

  void TimestampColumnWriter::recordPosition() const {
    ColumnWriter::recordPosition();
    secRleEncoder->recordPosition(rowIndexPosition.get());
    nanoRleEncoder->recordPosition(rowIndexPosition.get());
  }

  class DateColumnWriter : public IntegerColumnWriter<LongVectorBatch> {
   public:
    DateColumnWriter(const Type& type, const StreamsFactory& factory, const WriterOptions& options);

    virtual void add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                     const char* incomingMask) override;
  };

  DateColumnWriter::DateColumnWriter(const Type& type, const StreamsFactory& factory,
                                     const WriterOptions& options)
      : IntegerColumnWriter<LongVectorBatch>(type, factory, options) {
    // PASS
  }

  void DateColumnWriter::add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                             const char* incomingMask) {
    const LongVectorBatch* longBatch = dynamic_cast<const LongVectorBatch*>(&rowBatch);
    if (longBatch == nullptr) {
      throw InvalidArgument("Failed to cast to LongVectorBatch");
    }

    DateColumnStatisticsImpl* dateStats =
        dynamic_cast<DateColumnStatisticsImpl*>(colIndexStatistics.get());
    if (dateStats == nullptr) {
      throw InvalidArgument("Failed to cast to DateColumnStatisticsImpl");
    }

    ColumnWriter::add(rowBatch, offset, numValues, incomingMask);

    const int64_t* data = longBatch->data.data() + offset;
    const char* notNull = longBatch->hasNulls ? longBatch->notNull.data() + offset : nullptr;

    rleEncoder->add(data, numValues, notNull);

    uint64_t count = 0;
    for (uint64_t i = 0; i < numValues; ++i) {
      if (!notNull || notNull[i]) {
        ++count;
        dateStats->update(static_cast<int32_t>(data[i]));
        if (enableBloomFilter) {
          bloomFilter->addLong(data[i]);
        }
      }
    }
    dateStats->increase(count);
    if (count < numValues) {
      dateStats->setHasNull(true);
    }
  }

  class Decimal64ColumnWriter : public ColumnWriter {
   public:
    static const uint32_t MAX_PRECISION_64 = 18;
    static const uint32_t MAX_PRECISION_128 = 38;

    Decimal64ColumnWriter(const Type& type, const StreamsFactory& factory,
                          const WriterOptions& options);

    virtual void add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                     const char* incomingMask) override;

    virtual void flush(std::vector<proto::Stream>& streams) override;

    virtual uint64_t getEstimatedSize() const override;

    virtual void getColumnEncoding(std::vector<proto::ColumnEncoding>& encodings) const override;

    virtual void recordPosition() const override;

   protected:
    RleVersion rleVersion;
    uint64_t precision;
    uint64_t scale;
    std::unique_ptr<AppendOnlyBufferedStream> valueStream;
    std::unique_ptr<RleEncoder> scaleEncoder;

   private:
    char buffer[10];
  };

  Decimal64ColumnWriter::Decimal64ColumnWriter(const Type& type, const StreamsFactory& factory,
                                               const WriterOptions& options)
      : ColumnWriter(type, factory, options),
        rleVersion(options.getRleVersion()),
        precision(type.getPrecision()),
        scale(type.getScale()) {
    valueStream.reset(new AppendOnlyBufferedStream(factory.createStream(proto::Stream_Kind_DATA)));
    std::unique_ptr<BufferedOutputStream> scaleStream =
        factory.createStream(proto::Stream_Kind_SECONDARY);
    scaleEncoder = createRleEncoder(std::move(scaleStream), true, rleVersion, memPool,
                                    options.getAlignedBitpacking());

    if (enableIndex) {
      recordPosition();
    }
  }

  void Decimal64ColumnWriter::add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                                  const char* incomingMask) {
    const Decimal64VectorBatch* decBatch = dynamic_cast<const Decimal64VectorBatch*>(&rowBatch);
    if (decBatch == nullptr) {
      throw InvalidArgument("Failed to cast to Decimal64VectorBatch");
    }

    DecimalColumnStatisticsImpl* decStats =
        dynamic_cast<DecimalColumnStatisticsImpl*>(colIndexStatistics.get());
    if (decStats == nullptr) {
      throw InvalidArgument("Failed to cast to DecimalColumnStatisticsImpl");
    }

    ColumnWriter::add(rowBatch, offset, numValues, incomingMask);

    const char* notNull = decBatch->hasNulls ? decBatch->notNull.data() + offset : nullptr;
    const int64_t* values = decBatch->values.data() + offset;

    uint64_t count = 0;
    for (uint64_t i = 0; i < numValues; ++i) {
      if (!notNull || notNull[i]) {
        int64_t val = zigZag(values[i]);
        char* data = buffer;
        while (true) {
          if ((val & ~0x7f) == 0) {
            *(data++) = (static_cast<char>(val));
            break;
          } else {
            *(data++) = static_cast<char>(0x80 | (val & 0x7f));
            // cast val to unsigned so as to force 0-fill right shift
            val = (static_cast<uint64_t>(val) >> 7);
          }
        }
        valueStream->write(buffer, static_cast<size_t>(data - buffer));
        ++count;
        if (enableBloomFilter) {
          std::string decimal = Decimal(values[i], static_cast<int32_t>(scale)).toString(true);
          bloomFilter->addBytes(decimal.c_str(), static_cast<int64_t>(decimal.size()));
        }
        decStats->update(Decimal(values[i], static_cast<int32_t>(scale)));
      }
    }
    decStats->increase(count);
    if (count < numValues) {
      decStats->setHasNull(true);
    }
    std::vector<int64_t> scales(numValues, static_cast<int64_t>(scale));
    scaleEncoder->add(scales.data(), numValues, notNull);
  }

  void Decimal64ColumnWriter::flush(std::vector<proto::Stream>& streams) {
    ColumnWriter::flush(streams);

    proto::Stream dataStream;
    dataStream.set_kind(proto::Stream_Kind_DATA);
    dataStream.set_column(static_cast<uint32_t>(columnId));
    dataStream.set_length(valueStream->flush());
    streams.push_back(dataStream);

    proto::Stream secondaryStream;
    secondaryStream.set_kind(proto::Stream_Kind_SECONDARY);
    secondaryStream.set_column(static_cast<uint32_t>(columnId));
    secondaryStream.set_length(scaleEncoder->flush());
    streams.push_back(secondaryStream);
  }

  uint64_t Decimal64ColumnWriter::getEstimatedSize() const {
    uint64_t size = ColumnWriter::getEstimatedSize();
    size += valueStream->getSize();
    size += scaleEncoder->getBufferSize();
    return size;
  }

  void Decimal64ColumnWriter::getColumnEncoding(
      std::vector<proto::ColumnEncoding>& encodings) const {
    proto::ColumnEncoding encoding;
    encoding.set_kind(RleVersionMapper(rleVersion));
    encoding.set_dictionarysize(0);
    if (enableBloomFilter) {
      encoding.set_bloomencoding(BloomFilterVersion::UTF8);
    }
    encodings.push_back(encoding);
  }

  void Decimal64ColumnWriter::recordPosition() const {
    ColumnWriter::recordPosition();
    valueStream->recordPosition(rowIndexPosition.get());
    scaleEncoder->recordPosition(rowIndexPosition.get());
  }

  class Decimal64ColumnWriterV2 : public ColumnWriter {
   public:
    Decimal64ColumnWriterV2(const Type& type, const StreamsFactory& factory,
                            const WriterOptions& options);

    virtual void add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                     const char* incomingMask) override;

    virtual void flush(std::vector<proto::Stream>& streams) override;

    virtual uint64_t getEstimatedSize() const override;

    virtual void getColumnEncoding(std::vector<proto::ColumnEncoding>& encodings) const override;

    virtual void recordPosition() const override;

   protected:
    uint64_t precision;
    uint64_t scale;
    std::unique_ptr<RleEncoder> valueEncoder;
  };

  Decimal64ColumnWriterV2::Decimal64ColumnWriterV2(const Type& type, const StreamsFactory& factory,
                                                   const WriterOptions& options)
      : ColumnWriter(type, factory, options),
        precision(type.getPrecision()),
        scale(type.getScale()) {
    std::unique_ptr<BufferedOutputStream> dataStream =
        factory.createStream(proto::Stream_Kind_DATA);
    valueEncoder = createRleEncoder(std::move(dataStream), true, RleVersion_2, memPool,
                                    options.getAlignedBitpacking());

    if (enableIndex) {
      recordPosition();
    }
  }

  void Decimal64ColumnWriterV2::add(ColumnVectorBatch& rowBatch, uint64_t offset,
                                    uint64_t numValues, const char* incomingMask) {
    const Decimal64VectorBatch* decBatch = dynamic_cast<const Decimal64VectorBatch*>(&rowBatch);
    if (decBatch == nullptr) {
      throw InvalidArgument("Failed to cast to Decimal64VectorBatch");
    }

    DecimalColumnStatisticsImpl* decStats =
        dynamic_cast<DecimalColumnStatisticsImpl*>(colIndexStatistics.get());
    if (decStats == nullptr) {
      throw InvalidArgument("Failed to cast to DecimalColumnStatisticsImpl");
    }

    ColumnWriter::add(rowBatch, offset, numValues, incomingMask);

    const int64_t* data = decBatch->values.data() + offset;
    const char* notNull = decBatch->hasNulls ? decBatch->notNull.data() + offset : nullptr;

    valueEncoder->add(data, numValues, notNull);

    uint64_t count = 0;
    for (uint64_t i = 0; i < numValues; ++i) {
      if (!notNull || notNull[i]) {
        ++count;
        if (enableBloomFilter) {
          std::string decimal = Decimal(data[i], static_cast<int32_t>(scale)).toString(true);
          bloomFilter->addBytes(decimal.c_str(), static_cast<int64_t>(decimal.size()));
        }
        decStats->update(Decimal(data[i], static_cast<int32_t>(scale)));
      }
    }
    decStats->increase(count);
    if (count < numValues) {
      decStats->setHasNull(true);
    }
  }

  void Decimal64ColumnWriterV2::flush(std::vector<proto::Stream>& streams) {
    ColumnWriter::flush(streams);

    proto::Stream dataStream;
    dataStream.set_kind(proto::Stream_Kind_DATA);
    dataStream.set_column(static_cast<uint32_t>(columnId));
    dataStream.set_length(valueEncoder->flush());
    streams.push_back(dataStream);
  }

  uint64_t Decimal64ColumnWriterV2::getEstimatedSize() const {
    uint64_t size = ColumnWriter::getEstimatedSize();
    size += valueEncoder->getBufferSize();
    return size;
  }

  void Decimal64ColumnWriterV2::getColumnEncoding(
      std::vector<proto::ColumnEncoding>& encodings) const {
    proto::ColumnEncoding encoding;
    encoding.set_kind(RleVersionMapper(RleVersion_2));
    encoding.set_dictionarysize(0);
    if (enableBloomFilter) {
      encoding.set_bloomencoding(BloomFilterVersion::UTF8);
    }
    encodings.push_back(encoding);
  }

  void Decimal64ColumnWriterV2::recordPosition() const {
    ColumnWriter::recordPosition();
    valueEncoder->recordPosition(rowIndexPosition.get());
  }

  class Decimal128ColumnWriter : public Decimal64ColumnWriter {
   public:
    Decimal128ColumnWriter(const Type& type, const StreamsFactory& factory,
                           const WriterOptions& options);

    virtual void add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                     const char* incomingMask) override;

   private:
    char buffer[20];
  };

  Decimal128ColumnWriter::Decimal128ColumnWriter(const Type& type, const StreamsFactory& factory,
                                                 const WriterOptions& options)
      : Decimal64ColumnWriter(type, factory, options) {
    // PASS
  }

  // Zigzag encoding moves the sign bit to the least significant bit using the
  // expression (val « 1) ^ (val » 63) and derives its name from the fact that
  // positive and negative numbers alternate once encoded.
  Int128 zigZagInt128(const Int128& value) {
    bool isNegative = value < 0;
    Int128 val = value.abs();
    val <<= 1;
    if (isNegative) {
      val -= 1;
    }
    return val;
  }

  void Decimal128ColumnWriter::add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                                   const char* incomingMask) {
    const Decimal128VectorBatch* decBatch = dynamic_cast<const Decimal128VectorBatch*>(&rowBatch);
    if (decBatch == nullptr) {
      throw InvalidArgument("Failed to cast to Decimal128VectorBatch");
    }

    DecimalColumnStatisticsImpl* decStats =
        dynamic_cast<DecimalColumnStatisticsImpl*>(colIndexStatistics.get());
    if (decStats == nullptr) {
      throw InvalidArgument("Failed to cast to DecimalColumnStatisticsImpl");
    }

    ColumnWriter::add(rowBatch, offset, numValues, incomingMask);

    const char* notNull = decBatch->hasNulls ? decBatch->notNull.data() + offset : nullptr;
    const Int128* values = decBatch->values.data() + offset;

    // The current encoding of decimal columns stores the integer representation
    // of the value as an unbounded length zigzag encoded base 128 varint.
    uint64_t count = 0;
    for (uint64_t i = 0; i < numValues; ++i) {
      if (!notNull || notNull[i]) {
        Int128 val = zigZagInt128(values[i]);
        char* data = buffer;
        while (true) {
          if ((val & ~0x7f) == 0) {
            *(data++) = (static_cast<char>(val.getLowBits()));
            break;
          } else {
            *(data++) = static_cast<char>(0x80 | (val.getLowBits() & 0x7f));
            val >>= 7;
          }
        }
        valueStream->write(buffer, static_cast<size_t>(data - buffer));

        ++count;
        if (enableBloomFilter) {
          std::string decimal = Decimal(values[i], static_cast<int32_t>(scale)).toString(true);
          bloomFilter->addBytes(decimal.c_str(), static_cast<int64_t>(decimal.size()));
        }
        decStats->update(Decimal(values[i], static_cast<int32_t>(scale)));
      }
    }
    decStats->increase(count);
    if (count < numValues) {
      decStats->setHasNull(true);
    }
    std::vector<int64_t> scales(numValues, static_cast<int64_t>(scale));
    scaleEncoder->add(scales.data(), numValues, notNull);
  }

  class ListColumnWriter : public ColumnWriter {
   public:
    ListColumnWriter(const Type& type, const StreamsFactory& factory, const WriterOptions& options);
    ~ListColumnWriter() override;

    virtual void add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                     const char* incomingMask) override;

    virtual void flush(std::vector<proto::Stream>& streams) override;

    virtual uint64_t getEstimatedSize() const override;

    virtual void getColumnEncoding(std::vector<proto::ColumnEncoding>& encodings) const override;

    virtual void getStripeStatistics(std::vector<proto::ColumnStatistics>& stats) const override;

    virtual void getFileStatistics(std::vector<proto::ColumnStatistics>& stats) const override;

    virtual void mergeStripeStatsIntoFileStats() override;

    virtual void mergeRowGroupStatsIntoStripeStats() override;

    virtual void createRowIndexEntry() override;

    virtual void writeIndex(std::vector<proto::Stream>& streams) const override;

    virtual void recordPosition() const override;

    virtual void writeDictionary() override;

    virtual void reset() override;

   private:
    std::unique_ptr<RleEncoder> lengthEncoder;
    RleVersion rleVersion;
    std::unique_ptr<ColumnWriter> child;
  };

  ListColumnWriter::ListColumnWriter(const Type& type, const StreamsFactory& factory,
                                     const WriterOptions& options)
      : ColumnWriter(type, factory, options), rleVersion(options.getRleVersion()) {
    std::unique_ptr<BufferedOutputStream> lengthStream =
        factory.createStream(proto::Stream_Kind_LENGTH);
    lengthEncoder = createRleEncoder(std::move(lengthStream), false, rleVersion, memPool,
                                     options.getAlignedBitpacking());

    if (type.getSubtypeCount() == 1) {
      child = buildWriter(*type.getSubtype(0), factory, options);
    }

    if (enableIndex) {
      recordPosition();
    }
  }

  ListColumnWriter::~ListColumnWriter() {
    // PASS
  }

  void ListColumnWriter::add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                             const char* incomingMask) {
    ListVectorBatch* listBatch = dynamic_cast<ListVectorBatch*>(&rowBatch);
    if (listBatch == nullptr) {
      throw InvalidArgument("Failed to cast to ListVectorBatch");
    }
    CollectionColumnStatisticsImpl* collectionStats =
        dynamic_cast<CollectionColumnStatisticsImpl*>(colIndexStatistics.get());
    if (collectionStats == nullptr) {
      throw InvalidArgument("Failed to cast to CollectionColumnStatisticsImpl");
    }

    ColumnWriter::add(rowBatch, offset, numValues, incomingMask);

    int64_t* offsets = listBatch->offsets.data() + offset;
    const char* notNull = listBatch->hasNulls ? listBatch->notNull.data() + offset : nullptr;

    uint64_t elemOffset = static_cast<uint64_t>(offsets[0]);
    uint64_t totalNumValues = static_cast<uint64_t>(offsets[numValues] - offsets[0]);

    // translate offsets to lengths
    for (uint64_t i = 0; i != numValues; ++i) {
      offsets[i] = offsets[i + 1] - offsets[i];
    }

    // unnecessary to deal with null as elements are packed together
    if (child.get()) {
      child->add(*listBatch->elements, elemOffset, totalNumValues, nullptr);
    }
    lengthEncoder->add(offsets, numValues, notNull);

    if (enableIndex) {
      if (!notNull) {
        collectionStats->increase(numValues);
      } else {
        uint64_t count = 0;
        for (uint64_t i = 0; i < numValues; ++i) {
          if (notNull[i]) {
            ++count;
            collectionStats->update(static_cast<uint64_t>(offsets[i]));
            if (enableBloomFilter) {
              bloomFilter->addLong(offsets[i]);
            }
          }
        }
        collectionStats->increase(count);
        if (count < numValues) {
          collectionStats->setHasNull(true);
        }
      }
    }
  }

  void ListColumnWriter::flush(std::vector<proto::Stream>& streams) {
    ColumnWriter::flush(streams);

    proto::Stream stream;
    stream.set_kind(proto::Stream_Kind_LENGTH);
    stream.set_column(static_cast<uint32_t>(columnId));
    stream.set_length(lengthEncoder->flush());
    streams.push_back(stream);

    if (child.get()) {
      child->flush(streams);
    }
  }

  void ListColumnWriter::writeIndex(std::vector<proto::Stream>& streams) const {
    ColumnWriter::writeIndex(streams);
    if (child.get()) {
      child->writeIndex(streams);
    }
  }

  uint64_t ListColumnWriter::getEstimatedSize() const {
    uint64_t size = ColumnWriter::getEstimatedSize();
    if (child.get()) {
      size += lengthEncoder->getBufferSize();
      size += child->getEstimatedSize();
    }
    return size;
  }

  void ListColumnWriter::getColumnEncoding(std::vector<proto::ColumnEncoding>& encodings) const {
    proto::ColumnEncoding encoding;
    encoding.set_kind(RleVersionMapper(rleVersion));
    encoding.set_dictionarysize(0);
    if (enableBloomFilter) {
      encoding.set_bloomencoding(BloomFilterVersion::UTF8);
    }
    encodings.push_back(encoding);
    if (child.get()) {
      child->getColumnEncoding(encodings);
    }
  }

  void ListColumnWriter::getStripeStatistics(std::vector<proto::ColumnStatistics>& stats) const {
    ColumnWriter::getStripeStatistics(stats);
    if (child.get()) {
      child->getStripeStatistics(stats);
    }
  }

  void ListColumnWriter::mergeStripeStatsIntoFileStats() {
    ColumnWriter::mergeStripeStatsIntoFileStats();
    if (child.get()) {
      child->mergeStripeStatsIntoFileStats();
    }
  }

  void ListColumnWriter::getFileStatistics(std::vector<proto::ColumnStatistics>& stats) const {
    ColumnWriter::getFileStatistics(stats);
    if (child.get()) {
      child->getFileStatistics(stats);
    }
  }

  void ListColumnWriter::mergeRowGroupStatsIntoStripeStats() {
    ColumnWriter::mergeRowGroupStatsIntoStripeStats();
    if (child.get()) {
      child->mergeRowGroupStatsIntoStripeStats();
    }
  }

  void ListColumnWriter::createRowIndexEntry() {
    ColumnWriter::createRowIndexEntry();
    if (child.get()) {
      child->createRowIndexEntry();
    }
  }

  void ListColumnWriter::recordPosition() const {
    ColumnWriter::recordPosition();
    lengthEncoder->recordPosition(rowIndexPosition.get());
  }

  void ListColumnWriter::reset() {
    ColumnWriter::reset();
    if (child) {
      child->reset();
    }
  }

  void ListColumnWriter::writeDictionary() {
    if (child) {
      child->writeDictionary();
    }
  }

  class MapColumnWriter : public ColumnWriter {
   public:
    MapColumnWriter(const Type& type, const StreamsFactory& factory, const WriterOptions& options);
    ~MapColumnWriter() override;

    virtual void add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                     const char* incomingMask) override;

    virtual void flush(std::vector<proto::Stream>& streams) override;

    virtual uint64_t getEstimatedSize() const override;

    virtual void getColumnEncoding(std::vector<proto::ColumnEncoding>& encodings) const override;

    virtual void getStripeStatistics(std::vector<proto::ColumnStatistics>& stats) const override;

    virtual void getFileStatistics(std::vector<proto::ColumnStatistics>& stats) const override;

    virtual void mergeStripeStatsIntoFileStats() override;

    virtual void mergeRowGroupStatsIntoStripeStats() override;

    virtual void createRowIndexEntry() override;

    virtual void writeIndex(std::vector<proto::Stream>& streams) const override;

    virtual void recordPosition() const override;

    virtual void writeDictionary() override;

    virtual void reset() override;

   private:
    std::unique_ptr<ColumnWriter> keyWriter;
    std::unique_ptr<ColumnWriter> elemWriter;
    std::unique_ptr<RleEncoder> lengthEncoder;
    RleVersion rleVersion;
  };

  MapColumnWriter::MapColumnWriter(const Type& type, const StreamsFactory& factory,
                                   const WriterOptions& options)
      : ColumnWriter(type, factory, options), rleVersion(options.getRleVersion()) {
    std::unique_ptr<BufferedOutputStream> lengthStream =
        factory.createStream(proto::Stream_Kind_LENGTH);
    lengthEncoder = createRleEncoder(std::move(lengthStream), false, rleVersion, memPool,
                                     options.getAlignedBitpacking());

    if (type.getSubtypeCount() > 0) {
      keyWriter = buildWriter(*type.getSubtype(0), factory, options);
    }

    if (type.getSubtypeCount() > 1) {
      elemWriter = buildWriter(*type.getSubtype(1), factory, options);
    }

    if (enableIndex) {
      recordPosition();
    }
  }

  MapColumnWriter::~MapColumnWriter() {
    // PASS
  }

  void MapColumnWriter::add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                            const char* incomingMask) {
    MapVectorBatch* mapBatch = dynamic_cast<MapVectorBatch*>(&rowBatch);
    if (mapBatch == nullptr) {
      throw InvalidArgument("Failed to cast to MapVectorBatch");
    }
    CollectionColumnStatisticsImpl* collectionStats =
        dynamic_cast<CollectionColumnStatisticsImpl*>(colIndexStatistics.get());
    if (collectionStats == nullptr) {
      throw InvalidArgument("Failed to cast to CollectionColumnStatisticsImpl");
    }

    ColumnWriter::add(rowBatch, offset, numValues, incomingMask);

    int64_t* offsets = mapBatch->offsets.data() + offset;
    const char* notNull = mapBatch->hasNulls ? mapBatch->notNull.data() + offset : nullptr;

    uint64_t elemOffset = static_cast<uint64_t>(offsets[0]);
    uint64_t totalNumValues = static_cast<uint64_t>(offsets[numValues] - offsets[0]);

    // translate offsets to lengths
    for (uint64_t i = 0; i != numValues; ++i) {
      offsets[i] = offsets[i + 1] - offsets[i];
    }

    lengthEncoder->add(offsets, numValues, notNull);

    // unnecessary to deal with null as keys and values are packed together
    if (keyWriter.get()) {
      keyWriter->add(*mapBatch->keys, elemOffset, totalNumValues, nullptr);
    }
    if (elemWriter.get()) {
      elemWriter->add(*mapBatch->elements, elemOffset, totalNumValues, nullptr);
    }

    if (enableIndex) {
      if (!notNull) {
        collectionStats->increase(numValues);
      } else {
        uint64_t count = 0;
        for (uint64_t i = 0; i < numValues; ++i) {
          if (notNull[i]) {
            ++count;
            collectionStats->update(static_cast<uint64_t>(offsets[i]));
            if (enableBloomFilter) {
              bloomFilter->addLong(offsets[i]);
            }
          }
        }
        collectionStats->increase(count);
        if (count < numValues) {
          collectionStats->setHasNull(true);
        }
      }
    }
  }

  void MapColumnWriter::flush(std::vector<proto::Stream>& streams) {
    ColumnWriter::flush(streams);

    proto::Stream stream;
    stream.set_kind(proto::Stream_Kind_LENGTH);
    stream.set_column(static_cast<uint32_t>(columnId));
    stream.set_length(lengthEncoder->flush());
    streams.push_back(stream);

    if (keyWriter.get()) {
      keyWriter->flush(streams);
    }
    if (elemWriter.get()) {
      elemWriter->flush(streams);
    }
  }

  void MapColumnWriter::writeIndex(std::vector<proto::Stream>& streams) const {
    ColumnWriter::writeIndex(streams);
    if (keyWriter.get()) {
      keyWriter->writeIndex(streams);
    }
    if (elemWriter.get()) {
      elemWriter->writeIndex(streams);
    }
  }

  uint64_t MapColumnWriter::getEstimatedSize() const {
    uint64_t size = ColumnWriter::getEstimatedSize();
    size += lengthEncoder->getBufferSize();
    if (keyWriter.get()) {
      size += keyWriter->getEstimatedSize();
    }
    if (elemWriter.get()) {
      size += elemWriter->getEstimatedSize();
    }
    return size;
  }

  void MapColumnWriter::getColumnEncoding(std::vector<proto::ColumnEncoding>& encodings) const {
    proto::ColumnEncoding encoding;
    encoding.set_kind(RleVersionMapper(rleVersion));
    encoding.set_dictionarysize(0);
    if (enableBloomFilter) {
      encoding.set_bloomencoding(BloomFilterVersion::UTF8);
    }
    encodings.push_back(encoding);
    if (keyWriter.get()) {
      keyWriter->getColumnEncoding(encodings);
    }
    if (elemWriter.get()) {
      elemWriter->getColumnEncoding(encodings);
    }
  }

  void MapColumnWriter::getStripeStatistics(std::vector<proto::ColumnStatistics>& stats) const {
    ColumnWriter::getStripeStatistics(stats);
    if (keyWriter.get()) {
      keyWriter->getStripeStatistics(stats);
    }
    if (elemWriter.get()) {
      elemWriter->getStripeStatistics(stats);
    }
  }

  void MapColumnWriter::mergeStripeStatsIntoFileStats() {
    ColumnWriter::mergeStripeStatsIntoFileStats();
    if (keyWriter.get()) {
      keyWriter->mergeStripeStatsIntoFileStats();
    }
    if (elemWriter.get()) {
      elemWriter->mergeStripeStatsIntoFileStats();
    }
  }

  void MapColumnWriter::getFileStatistics(std::vector<proto::ColumnStatistics>& stats) const {
    ColumnWriter::getFileStatistics(stats);
    if (keyWriter.get()) {
      keyWriter->getFileStatistics(stats);
    }
    if (elemWriter.get()) {
      elemWriter->getFileStatistics(stats);
    }
  }

  void MapColumnWriter::mergeRowGroupStatsIntoStripeStats() {
    ColumnWriter::mergeRowGroupStatsIntoStripeStats();
    if (keyWriter.get()) {
      keyWriter->mergeRowGroupStatsIntoStripeStats();
    }
    if (elemWriter.get()) {
      elemWriter->mergeRowGroupStatsIntoStripeStats();
    }
  }

  void MapColumnWriter::createRowIndexEntry() {
    ColumnWriter::createRowIndexEntry();
    if (keyWriter.get()) {
      keyWriter->createRowIndexEntry();
    }
    if (elemWriter.get()) {
      elemWriter->createRowIndexEntry();
    }
  }

  void MapColumnWriter::recordPosition() const {
    ColumnWriter::recordPosition();
    lengthEncoder->recordPosition(rowIndexPosition.get());
  }

  void MapColumnWriter::reset() {
    ColumnWriter::reset();
    if (keyWriter) {
      keyWriter->reset();
    }
    if (elemWriter) {
      elemWriter->reset();
    }
  }

  void MapColumnWriter::writeDictionary() {
    if (keyWriter) {
      keyWriter->writeDictionary();
    }
    if (elemWriter) {
      elemWriter->writeDictionary();
    }
  }

  class UnionColumnWriter : public ColumnWriter {
   public:
    UnionColumnWriter(const Type& type, const StreamsFactory& factory,
                      const WriterOptions& options);

    virtual void add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                     const char* incomingMask) override;

    virtual void flush(std::vector<proto::Stream>& streams) override;

    virtual uint64_t getEstimatedSize() const override;

    virtual void getColumnEncoding(std::vector<proto::ColumnEncoding>& encodings) const override;

    virtual void getStripeStatistics(std::vector<proto::ColumnStatistics>& stats) const override;

    virtual void getFileStatistics(std::vector<proto::ColumnStatistics>& stats) const override;

    virtual void mergeStripeStatsIntoFileStats() override;

    virtual void mergeRowGroupStatsIntoStripeStats() override;

    virtual void createRowIndexEntry() override;

    virtual void writeIndex(std::vector<proto::Stream>& streams) const override;

    virtual void recordPosition() const override;

    virtual void writeDictionary() override;

    virtual void reset() override;

   private:
    std::unique_ptr<ByteRleEncoder> rleEncoder;
    std::vector<std::unique_ptr<ColumnWriter>> children;
  };

  UnionColumnWriter::UnionColumnWriter(const Type& type, const StreamsFactory& factory,
                                       const WriterOptions& options)
      : ColumnWriter(type, factory, options) {
    std::unique_ptr<BufferedOutputStream> dataStream =
        factory.createStream(proto::Stream_Kind_DATA);
    rleEncoder = createByteRleEncoder(std::move(dataStream));

    for (uint64_t i = 0; i != type.getSubtypeCount(); ++i) {
      children.push_back(buildWriter(*type.getSubtype(i), factory, options));
    }

    if (enableIndex) {
      recordPosition();
    }
  }

  void UnionColumnWriter::add(ColumnVectorBatch& rowBatch, uint64_t offset, uint64_t numValues,
                              const char* incomingMask) {
    UnionVectorBatch* unionBatch = dynamic_cast<UnionVectorBatch*>(&rowBatch);
    if (unionBatch == nullptr) {
      throw InvalidArgument("Failed to cast to UnionVectorBatch");
    }

    ColumnWriter::add(rowBatch, offset, numValues, incomingMask);

    const char* notNull = unionBatch->hasNulls ? unionBatch->notNull.data() + offset : nullptr;
    unsigned char* tags = unionBatch->tags.data() + offset;
    uint64_t* offsets = unionBatch->offsets.data() + offset;

    std::vector<int64_t> childOffset(children.size(), -1);
    std::vector<uint64_t> childLength(children.size(), 0);

    for (uint64_t i = 0; i != numValues; ++i) {
      if (childOffset[tags[i]] == -1) {
        childOffset[tags[i]] = static_cast<int64_t>(offsets[i]);
      }
      ++childLength[tags[i]];
    }

    rleEncoder->add(reinterpret_cast<char*>(tags), numValues, notNull);

    for (uint32_t i = 0; i < children.size(); ++i) {
      if (childLength[i] > 0) {
        children[i]->add(*unionBatch->children[i], static_cast<uint64_t>(childOffset[i]),
                         childLength[i], nullptr);
      }
    }

    // update stats
    if (enableIndex) {
      if (!notNull) {
        colIndexStatistics->increase(numValues);
      } else {
        uint64_t count = 0;
        for (uint64_t i = 0; i < numValues; ++i) {
          if (notNull[i]) {
            ++count;
            if (enableBloomFilter) {
              bloomFilter->addLong(tags[i]);
            }
          }
        }
        colIndexStatistics->increase(count);
        if (count < numValues) {
          colIndexStatistics->setHasNull(true);
        }
      }
    }
  }

  void UnionColumnWriter::flush(std::vector<proto::Stream>& streams) {
    ColumnWriter::flush(streams);

    proto::Stream stream;
    stream.set_kind(proto::Stream_Kind_DATA);
    stream.set_column(static_cast<uint32_t>(columnId));
    stream.set_length(rleEncoder->flush());
    streams.push_back(stream);

    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->flush(streams);
    }
  }

  void UnionColumnWriter::writeIndex(std::vector<proto::Stream>& streams) const {
    ColumnWriter::writeIndex(streams);
    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->writeIndex(streams);
    }
  }

  uint64_t UnionColumnWriter::getEstimatedSize() const {
    uint64_t size = ColumnWriter::getEstimatedSize();
    size += rleEncoder->getBufferSize();
    for (uint32_t i = 0; i < children.size(); ++i) {
      size += children[i]->getEstimatedSize();
    }
    return size;
  }

  void UnionColumnWriter::getColumnEncoding(std::vector<proto::ColumnEncoding>& encodings) const {
    proto::ColumnEncoding encoding;
    encoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    encoding.set_dictionarysize(0);
    if (enableBloomFilter) {
      encoding.set_bloomencoding(BloomFilterVersion::UTF8);
    }
    encodings.push_back(encoding);
    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->getColumnEncoding(encodings);
    }
  }

  void UnionColumnWriter::getStripeStatistics(std::vector<proto::ColumnStatistics>& stats) const {
    ColumnWriter::getStripeStatistics(stats);
    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->getStripeStatistics(stats);
    }
  }

  void UnionColumnWriter::mergeStripeStatsIntoFileStats() {
    ColumnWriter::mergeStripeStatsIntoFileStats();
    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->mergeStripeStatsIntoFileStats();
    }
  }

  void UnionColumnWriter::getFileStatistics(std::vector<proto::ColumnStatistics>& stats) const {
    ColumnWriter::getFileStatistics(stats);
    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->getFileStatistics(stats);
    }
  }

  void UnionColumnWriter::mergeRowGroupStatsIntoStripeStats() {
    ColumnWriter::mergeRowGroupStatsIntoStripeStats();
    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->mergeRowGroupStatsIntoStripeStats();
    }
  }

  void UnionColumnWriter::createRowIndexEntry() {
    ColumnWriter::createRowIndexEntry();
    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->createRowIndexEntry();
    }
  }

  void UnionColumnWriter::recordPosition() const {
    ColumnWriter::recordPosition();
    rleEncoder->recordPosition(rowIndexPosition.get());
  }

  void UnionColumnWriter::reset() {
    ColumnWriter::reset();
    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->reset();
    }
  }

  void UnionColumnWriter::writeDictionary() {
    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->writeDictionary();
    }
  }

  std::unique_ptr<ColumnWriter> buildWriter(const Type& type, const StreamsFactory& factory,
                                            const WriterOptions& options) {
    switch (static_cast<int64_t>(type.getKind())) {
      case STRUCT:
        return std::make_unique<StructColumnWriter>(type, factory, options);
      case SHORT:
        if (options.getUseTightNumericVector()) {
          return std::make_unique<IntegerColumnWriter<ShortVectorBatch>>(type, factory, options);
        }
      case INT:
        if (options.getUseTightNumericVector()) {
          return std::make_unique<IntegerColumnWriter<IntVectorBatch>>(type, factory, options);
        }
      case LONG:
        return std::make_unique<IntegerColumnWriter<LongVectorBatch>>(type, factory, options);
      case BYTE:
        if (options.getUseTightNumericVector()) {
          return std::make_unique<ByteColumnWriter<ByteVectorBatch>>(type, factory, options);
        }
        return std::make_unique<ByteColumnWriter<LongVectorBatch>>(type, factory, options);
      case BOOLEAN: {
        if (options.getUseTightNumericVector()) {
          return std::make_unique<BooleanColumnWriter<ByteVectorBatch>>(type, factory, options);
        } else {
          return std::make_unique<BooleanColumnWriter<LongVectorBatch>>(type, factory, options);
        }
      }
      case DOUBLE:
        return std::make_unique<FloatingColumnWriter<double, DoubleVectorBatch>>(type, factory,
                                                                                 options, false);
      case FLOAT:
        if (options.getUseTightNumericVector()) {
          return std::make_unique<FloatingColumnWriter<float, FloatVectorBatch>>(type, factory,
                                                                                 options, true);
        }
        return std::make_unique<FloatingColumnWriter<double, DoubleVectorBatch>>(type, factory,
                                                                                 options, true);
      case BINARY:
        return std::make_unique<BinaryColumnWriter>(type, factory, options);
      case STRING:
        return std::make_unique<StringColumnWriter>(type, factory, options);
      case CHAR:
        return std::make_unique<CharColumnWriter>(type, factory, options);
      case VARCHAR:
        return std::make_unique<VarCharColumnWriter>(type, factory, options);
      case DATE:
        return std::make_unique<DateColumnWriter>(type, factory, options);
      case TIMESTAMP:
        return std::make_unique<TimestampColumnWriter>(type, factory, options, false);
      case TIMESTAMP_INSTANT:
        return std::make_unique<TimestampColumnWriter>(type, factory, options, true);
      case DECIMAL:
        if (type.getPrecision() <= Decimal64ColumnWriter::MAX_PRECISION_64) {
          if (options.getFileVersion() == FileVersion::UNSTABLE_PRE_2_0()) {
            return std::make_unique<Decimal64ColumnWriterV2>(type, factory, options);
          }
          return std::make_unique<Decimal64ColumnWriter>(type, factory, options);
        } else if (type.getPrecision() <= Decimal64ColumnWriter::MAX_PRECISION_128) {
          return std::make_unique<Decimal128ColumnWriter>(type, factory, options);
        } else {
          throw NotImplementedYet(
              "Decimal precision more than 38 is not "
              "supported");
        }
      case LIST:
        return std::make_unique<ListColumnWriter>(type, factory, options);
      case MAP:
        return std::make_unique<MapColumnWriter>(type, factory, options);
      case UNION:
        return std::make_unique<UnionColumnWriter>(type, factory, options);
      default:
        throw NotImplementedYet(
            "Type is not supported yet for creating "
            "ColumnWriter.");
    }
  }
}  // namespace orc
