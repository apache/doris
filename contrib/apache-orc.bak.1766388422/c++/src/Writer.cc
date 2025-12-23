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

#include "orc/Common.hh"
#include "orc/OrcFile.hh"

#include "ColumnWriter.hh"
#include "Timezone.hh"
#include "Utils.hh"

#include <memory>

namespace orc {

  struct WriterOptionsPrivate {
    uint64_t stripeSize;
    uint64_t compressionBlockSize;
    uint64_t rowIndexStride;
    CompressionKind compression;
    CompressionStrategy compressionStrategy;
    MemoryPool* memoryPool;
    double paddingTolerance;
    std::ostream* errorStream;
    FileVersion fileVersion;
    double dictionaryKeySizeThreshold;
    bool enableIndex;
    std::set<uint64_t> columnsUseBloomFilter;
    double bloomFilterFalsePositiveProb;
    BloomFilterVersion bloomFilterVersion;
    std::string timezone;
    WriterMetrics* metrics;
    bool useTightNumericVector;
    uint64_t outputBufferCapacity;

    WriterOptionsPrivate() : fileVersion(FileVersion::v_0_12()) {  // default to Hive_0_12
      stripeSize = 64 * 1024 * 1024;                               // 64M
      compressionBlockSize = 64 * 1024;                            // 64K
      rowIndexStride = 10000;
      compression = CompressionKind_ZLIB;
      compressionStrategy = CompressionStrategy_SPEED;
      memoryPool = getDefaultPool();
      paddingTolerance = 0.0;
      errorStream = &std::cerr;
      dictionaryKeySizeThreshold = 0.0;
      enableIndex = true;
      bloomFilterFalsePositiveProb = 0.05;
      bloomFilterVersion = UTF8;
      // Writer timezone uses "GMT" by default to get rid of potential issues
      // introduced by moving timestamps between different timezones.
      // Explictly set the writer timezone if the use case depends on it.
      timezone = "GMT";
      metrics = nullptr;
      useTightNumericVector = false;
      outputBufferCapacity = 1024 * 1024;
    }
  };

  WriterOptions::WriterOptions()
      : privateBits(std::unique_ptr<WriterOptionsPrivate>(new WriterOptionsPrivate())) {
    // PASS
  }

  WriterOptions::WriterOptions(const WriterOptions& rhs)
      : privateBits(std::unique_ptr<WriterOptionsPrivate>(
            new WriterOptionsPrivate(*(rhs.privateBits.get())))) {
    // PASS
  }

  WriterOptions::WriterOptions(WriterOptions& rhs) {
    // swap privateBits with rhs
    privateBits.swap(rhs.privateBits);
  }

  WriterOptions& WriterOptions::operator=(const WriterOptions& rhs) {
    if (this != &rhs) {
      privateBits.reset(new WriterOptionsPrivate(*(rhs.privateBits.get())));
    }
    return *this;
  }

  WriterOptions::~WriterOptions() {
    // PASS
  }
  RleVersion WriterOptions::getRleVersion() const {
    if (privateBits->fileVersion == FileVersion::v_0_11()) {
      return RleVersion_1;
    }

    return RleVersion_2;
  }

  WriterOptions& WriterOptions::setStripeSize(uint64_t size) {
    privateBits->stripeSize = size;
    return *this;
  }

  uint64_t WriterOptions::getStripeSize() const {
    return privateBits->stripeSize;
  }

  WriterOptions& WriterOptions::setCompressionBlockSize(uint64_t size) {
    privateBits->compressionBlockSize = size;
    return *this;
  }

  uint64_t WriterOptions::getCompressionBlockSize() const {
    return privateBits->compressionBlockSize;
  }

  WriterOptions& WriterOptions::setRowIndexStride(uint64_t stride) {
    privateBits->rowIndexStride = stride;
    privateBits->enableIndex = (stride != 0);
    return *this;
  }

  uint64_t WriterOptions::getRowIndexStride() const {
    return privateBits->rowIndexStride;
  }

  WriterOptions& WriterOptions::setDictionaryKeySizeThreshold(double val) {
    privateBits->dictionaryKeySizeThreshold = val;
    return *this;
  }

  double WriterOptions::getDictionaryKeySizeThreshold() const {
    return privateBits->dictionaryKeySizeThreshold;
  }

  WriterOptions& WriterOptions::setFileVersion(const FileVersion& version) {
    // Only Hive_0_11 and Hive_0_12 version are supported currently
    if (version.getMajor() == 0 && (version.getMinor() == 11 || version.getMinor() == 12)) {
      privateBits->fileVersion = version;
      return *this;
    }
    if (version == FileVersion::UNSTABLE_PRE_2_0()) {
      *privateBits->errorStream << "Warning: ORC files written in "
                                << FileVersion::UNSTABLE_PRE_2_0().toString()
                                << " will not be readable by other versions of the software."
                                << " It is only for developer testing.\n";
      privateBits->fileVersion = version;
      return *this;
    }
    throw std::logic_error("Unsupported file version specified.");
  }

  FileVersion WriterOptions::getFileVersion() const {
    return privateBits->fileVersion;
  }

  WriterOptions& WriterOptions::setCompression(CompressionKind comp) {
    privateBits->compression = comp;
    return *this;
  }

  CompressionKind WriterOptions::getCompression() const {
    return privateBits->compression;
  }

  WriterOptions& WriterOptions::setCompressionStrategy(CompressionStrategy strategy) {
    privateBits->compressionStrategy = strategy;
    return *this;
  }

  CompressionStrategy WriterOptions::getCompressionStrategy() const {
    return privateBits->compressionStrategy;
  }

  bool WriterOptions::getAlignedBitpacking() const {
    return privateBits->compressionStrategy == CompressionStrategy ::CompressionStrategy_SPEED;
  }

  WriterOptions& WriterOptions::setPaddingTolerance(double tolerance) {
    privateBits->paddingTolerance = tolerance;
    return *this;
  }

  double WriterOptions::getPaddingTolerance() const {
    return privateBits->paddingTolerance;
  }

  WriterOptions& WriterOptions::setMemoryPool(MemoryPool* memoryPool) {
    privateBits->memoryPool = memoryPool;
    return *this;
  }

  MemoryPool* WriterOptions::getMemoryPool() const {
    return privateBits->memoryPool;
  }

  WriterOptions& WriterOptions::setErrorStream(std::ostream& errStream) {
    privateBits->errorStream = &errStream;
    return *this;
  }

  std::ostream* WriterOptions::getErrorStream() const {
    return privateBits->errorStream;
  }

  bool WriterOptions::getEnableIndex() const {
    return privateBits->enableIndex;
  }

  bool WriterOptions::getEnableDictionary() const {
    return privateBits->dictionaryKeySizeThreshold > 0.0;
  }

  WriterOptions& WriterOptions::setColumnsUseBloomFilter(const std::set<uint64_t>& columns) {
    privateBits->columnsUseBloomFilter = columns;
    return *this;
  }

  bool WriterOptions::isColumnUseBloomFilter(uint64_t column) const {
    return privateBits->columnsUseBloomFilter.find(column) !=
           privateBits->columnsUseBloomFilter.end();
  }

  WriterOptions& WriterOptions::setBloomFilterFPP(double fpp) {
    privateBits->bloomFilterFalsePositiveProb = fpp;
    return *this;
  }

  double WriterOptions::getBloomFilterFPP() const {
    return privateBits->bloomFilterFalsePositiveProb;
  }

  // delibrately not provide setter to write bloom filter version because
  // we only support UTF8 for now.
  BloomFilterVersion WriterOptions::getBloomFilterVersion() const {
    return privateBits->bloomFilterVersion;
  }

  const Timezone& WriterOptions::getTimezone() const {
    return getTimezoneByName(privateBits->timezone);
  }

  const std::string& WriterOptions::getTimezoneName() const {
    return privateBits->timezone;
  }

  WriterOptions& WriterOptions::setTimezoneName(const std::string& zone) {
    privateBits->timezone = zone;
    return *this;
  }

  WriterMetrics* WriterOptions::getWriterMetrics() const {
    return privateBits->metrics;
  }

  WriterOptions& WriterOptions::setWriterMetrics(WriterMetrics* metrics) {
    privateBits->metrics = metrics;
    return *this;
  }

  WriterOptions& WriterOptions::setUseTightNumericVector(bool useTightNumericVector) {
    privateBits->useTightNumericVector = useTightNumericVector;
    return *this;
  }

  bool WriterOptions::getUseTightNumericVector() const {
    return privateBits->useTightNumericVector;
  }

  WriterOptions& WriterOptions::setOutputBufferCapacity(uint64_t capacity) {
    privateBits->outputBufferCapacity = capacity;
    return *this;
  }

  uint64_t WriterOptions::getOutputBufferCapacity() const {
    return privateBits->outputBufferCapacity;
  }

  Writer::~Writer() {
    // PASS
  }

  class WriterImpl : public Writer {
   private:
    std::unique_ptr<ColumnWriter> columnWriter;
    std::unique_ptr<BufferedOutputStream> compressionStream;
    std::unique_ptr<BufferedOutputStream> bufferedStream;
    std::unique_ptr<StreamsFactory> streamsFactory;
    OutputStream* outStream;
    WriterOptions options;
    const Type& type;
    uint64_t stripeRows, totalRows, indexRows;
    uint64_t currentOffset;
    proto::Footer fileFooter;
    proto::PostScript postScript;
    proto::StripeInformation stripeInfo;
    proto::Metadata metadata;

    static const char* magicId;
    static const WriterId writerId;
    bool useTightNumericVector;

   public:
    WriterImpl(const Type& type, OutputStream* stream, const WriterOptions& options);

    std::unique_ptr<ColumnVectorBatch> createRowBatch(uint64_t size) const override;

    void add(ColumnVectorBatch& rowsToAdd) override;

    void close() override;

    void addUserMetadata(const std::string& name, const std::string& value) override;

   private:
    void init();
    void initStripe();
    void writeStripe();
    void writeMetadata();
    void writeFileFooter();
    void writePostscript();
    void buildFooterType(const Type& t, proto::Footer& footer, uint32_t& index);
    static proto::CompressionKind convertCompressionKind(const CompressionKind& kind);
  };

  const char* WriterImpl::magicId = "ORC";

  const WriterId WriterImpl::writerId = WriterId::ORC_CPP_WRITER;

  WriterImpl::WriterImpl(const Type& t, OutputStream* stream, const WriterOptions& opts)
      : outStream(stream), options(opts), type(t) {
    streamsFactory = createStreamsFactory(options, outStream);
    columnWriter = buildWriter(type, *streamsFactory, options);
    stripeRows = totalRows = indexRows = 0;
    currentOffset = 0;

    useTightNumericVector = opts.getUseTightNumericVector();

    // compression stream for stripe footer, file footer and metadata
    compressionStream =
        createCompressor(options.getCompression(), outStream, options.getCompressionStrategy(),
                         options.getOutputBufferCapacity(), options.getCompressionBlockSize(),
                         *options.getMemoryPool(), options.getWriterMetrics());

    // uncompressed stream for post script
    bufferedStream.reset(new BufferedOutputStream(*options.getMemoryPool(), outStream,
                                                  1024,  // buffer capacity: 1024 bytes
                                                  options.getCompressionBlockSize(),
                                                  options.getWriterMetrics()));

    init();
  }

  std::unique_ptr<ColumnVectorBatch> WriterImpl::createRowBatch(uint64_t size) const {
    return type.createRowBatch(size, *options.getMemoryPool(), false, useTightNumericVector);
  }

  void WriterImpl::add(ColumnVectorBatch& rowsToAdd) {
    if (options.getEnableIndex()) {
      uint64_t pos = 0;
      uint64_t chunkSize = 0;
      uint64_t rowIndexStride = options.getRowIndexStride();
      while (pos < rowsToAdd.numElements) {
        chunkSize = std::min(rowsToAdd.numElements - pos, rowIndexStride - indexRows);
        columnWriter->add(rowsToAdd, pos, chunkSize, nullptr);

        pos += chunkSize;
        indexRows += chunkSize;
        stripeRows += chunkSize;

        if (indexRows >= rowIndexStride) {
          columnWriter->createRowIndexEntry();
          indexRows = 0;
        }
      }
    } else {
      stripeRows += rowsToAdd.numElements;
      columnWriter->add(rowsToAdd, 0, rowsToAdd.numElements, nullptr);
    }

    if (columnWriter->getEstimatedSize() >= options.getStripeSize()) {
      writeStripe();
    }
  }

  void WriterImpl::close() {
    if (stripeRows > 0) {
      writeStripe();
    }
    writeMetadata();
    writeFileFooter();
    writePostscript();
    outStream->close();
  }

  void WriterImpl::addUserMetadata(const std::string& name, const std::string& value) {
    proto::UserMetadataItem* userMetadataItem = fileFooter.add_metadata();
    userMetadataItem->set_name(name);
    userMetadataItem->set_value(value);
  }

  void WriterImpl::init() {
    // Write file header
    const static size_t magicIdLength = strlen(WriterImpl::magicId);
    {
      SCOPED_STOPWATCH(options.getWriterMetrics(), IOBlockingLatencyUs, IOCount);
      outStream->write(WriterImpl::magicId, magicIdLength);
    }
    currentOffset += magicIdLength;

    // Initialize file footer
    fileFooter.set_headerlength(currentOffset);
    fileFooter.set_contentlength(0);
    fileFooter.set_numberofrows(0);
    fileFooter.set_rowindexstride(static_cast<uint32_t>(options.getRowIndexStride()));
    fileFooter.set_writer(writerId);
    fileFooter.set_softwareversion(ORC_VERSION);

    uint32_t index = 0;
    buildFooterType(type, fileFooter, index);

    // Initialize post script
    postScript.set_footerlength(0);
    postScript.set_compression(WriterImpl::convertCompressionKind(options.getCompression()));
    postScript.set_compressionblocksize(options.getCompressionBlockSize());

    postScript.add_version(options.getFileVersion().getMajor());
    postScript.add_version(options.getFileVersion().getMinor());

    postScript.set_writerversion(WriterVersion_ORC_135);
    postScript.set_magic("ORC");

    // Initialize first stripe
    initStripe();
  }

  void WriterImpl::initStripe() {
    stripeInfo.set_offset(currentOffset);
    stripeInfo.set_indexlength(0);
    stripeInfo.set_datalength(0);
    stripeInfo.set_footerlength(0);
    stripeInfo.set_numberofrows(0);

    stripeRows = indexRows = 0;
  }

  void WriterImpl::writeStripe() {
    if (options.getEnableIndex() && indexRows != 0) {
      columnWriter->createRowIndexEntry();
      indexRows = 0;
    } else {
      columnWriter->mergeRowGroupStatsIntoStripeStats();
    }

    // dictionary should be written before any stream is flushed
    columnWriter->writeDictionary();

    std::vector<proto::Stream> streams;
    // write ROW_INDEX streams
    if (options.getEnableIndex()) {
      columnWriter->writeIndex(streams);
    }
    // write streams like PRESENT, DATA, etc.
    columnWriter->flush(streams);

    // generate and write stripe footer
    proto::StripeFooter stripeFooter;
    for (uint32_t i = 0; i < streams.size(); ++i) {
      *stripeFooter.add_streams() = streams[i];
    }

    std::vector<proto::ColumnEncoding> encodings;
    columnWriter->getColumnEncoding(encodings);

    for (uint32_t i = 0; i < encodings.size(); ++i) {
      *stripeFooter.add_columns() = encodings[i];
    }

    stripeFooter.set_writertimezone(options.getTimezoneName());

    // add stripe statistics to metadata
    proto::StripeStatistics* stripeStats = metadata.add_stripestats();
    std::vector<proto::ColumnStatistics> colStats;
    columnWriter->getStripeStatistics(colStats);
    for (uint32_t i = 0; i != colStats.size(); ++i) {
      *stripeStats->add_colstats() = colStats[i];
    }
    // merge stripe stats into file stats and clear stripe stats
    columnWriter->mergeStripeStatsIntoFileStats();

    if (!stripeFooter.SerializeToZeroCopyStream(compressionStream.get())) {
      throw std::logic_error("Failed to write stripe footer.");
    }
    uint64_t footerLength = compressionStream->flush();

    // calculate data length and index length
    uint64_t dataLength = 0;
    uint64_t indexLength = 0;
    for (uint32_t i = 0; i < streams.size(); ++i) {
      if (streams[i].kind() == proto::Stream_Kind_ROW_INDEX ||
          streams[i].kind() == proto::Stream_Kind_BLOOM_FILTER_UTF8) {
        indexLength += streams[i].length();
      } else {
        dataLength += streams[i].length();
      }
    }

    // update stripe info
    stripeInfo.set_indexlength(indexLength);
    stripeInfo.set_datalength(dataLength);
    stripeInfo.set_footerlength(footerLength);
    stripeInfo.set_numberofrows(stripeRows);

    *fileFooter.add_stripes() = stripeInfo;

    currentOffset = currentOffset + indexLength + dataLength + footerLength;
    totalRows += stripeRows;

    columnWriter->reset();

    initStripe();
  }

  void WriterImpl::writeMetadata() {
    if (!metadata.SerializeToZeroCopyStream(compressionStream.get())) {
      throw std::logic_error("Failed to write metadata.");
    }
    postScript.set_metadatalength(compressionStream.get()->flush());
  }

  void WriterImpl::writeFileFooter() {
    fileFooter.set_contentlength(currentOffset - fileFooter.headerlength());
    fileFooter.set_numberofrows(totalRows);

    // update file statistics
    std::vector<proto::ColumnStatistics> colStats;
    columnWriter->getFileStatistics(colStats);
    for (uint32_t i = 0; i != colStats.size(); ++i) {
      *fileFooter.add_statistics() = colStats[i];
    }

    if (!fileFooter.SerializeToZeroCopyStream(compressionStream.get())) {
      throw std::logic_error("Failed to write file footer.");
    }
    postScript.set_footerlength(compressionStream->flush());
  }

  void WriterImpl::writePostscript() {
    if (!postScript.SerializeToZeroCopyStream(bufferedStream.get())) {
      throw std::logic_error("Failed to write post script.");
    }
    unsigned char psLength = static_cast<unsigned char>(bufferedStream->flush());
    SCOPED_STOPWATCH(options.getWriterMetrics(), IOBlockingLatencyUs, IOCount);
    outStream->write(&psLength, sizeof(unsigned char));
  }

  void WriterImpl::buildFooterType(const Type& t, proto::Footer& footer, uint32_t& index) {
    proto::Type protoType;
    protoType.set_maximumlength(static_cast<uint32_t>(t.getMaximumLength()));
    protoType.set_precision(static_cast<uint32_t>(t.getPrecision()));
    protoType.set_scale(static_cast<uint32_t>(t.getScale()));

    switch (t.getKind()) {
      case BOOLEAN: {
        protoType.set_kind(proto::Type_Kind_BOOLEAN);
        break;
      }
      case BYTE: {
        protoType.set_kind(proto::Type_Kind_BYTE);
        break;
      }
      case SHORT: {
        protoType.set_kind(proto::Type_Kind_SHORT);
        break;
      }
      case INT: {
        protoType.set_kind(proto::Type_Kind_INT);
        break;
      }
      case LONG: {
        protoType.set_kind(proto::Type_Kind_LONG);
        break;
      }
      case FLOAT: {
        protoType.set_kind(proto::Type_Kind_FLOAT);
        break;
      }
      case DOUBLE: {
        protoType.set_kind(proto::Type_Kind_DOUBLE);
        break;
      }
      case STRING: {
        protoType.set_kind(proto::Type_Kind_STRING);
        break;
      }
      case BINARY: {
        protoType.set_kind(proto::Type_Kind_BINARY);
        break;
      }
      case TIMESTAMP: {
        protoType.set_kind(proto::Type_Kind_TIMESTAMP);
        break;
      }
      case TIMESTAMP_INSTANT: {
        protoType.set_kind(proto::Type_Kind_TIMESTAMP_INSTANT);
        break;
      }
      case LIST: {
        protoType.set_kind(proto::Type_Kind_LIST);
        break;
      }
      case MAP: {
        protoType.set_kind(proto::Type_Kind_MAP);
        break;
      }
      case STRUCT: {
        protoType.set_kind(proto::Type_Kind_STRUCT);
        break;
      }
      case UNION: {
        protoType.set_kind(proto::Type_Kind_UNION);
        break;
      }
      case DECIMAL: {
        protoType.set_kind(proto::Type_Kind_DECIMAL);
        break;
      }
      case DATE: {
        protoType.set_kind(proto::Type_Kind_DATE);
        break;
      }
      case VARCHAR: {
        protoType.set_kind(proto::Type_Kind_VARCHAR);
        break;
      }
      case CHAR: {
        protoType.set_kind(proto::Type_Kind_CHAR);
        break;
      }
      default:
        throw std::logic_error("Unknown type.");
    }

    for (auto& key : t.getAttributeKeys()) {
      const auto& value = t.getAttributeValue(key);
      auto protoAttr = protoType.add_attributes();
      protoAttr->set_key(key);
      protoAttr->set_value(value);
    }

    int pos = static_cast<int>(index);
    *footer.add_types() = protoType;

    for (uint64_t i = 0; i < t.getSubtypeCount(); ++i) {
      // only add subtypes' field names if this type is STRUCT
      if (t.getKind() == STRUCT) {
        footer.mutable_types(pos)->add_fieldnames(t.getFieldName(i));
      }
      footer.mutable_types(pos)->add_subtypes(++index);
      buildFooterType(*t.getSubtype(i), footer, index);
    }
  }

  proto::CompressionKind WriterImpl::convertCompressionKind(const CompressionKind& kind) {
    return static_cast<proto::CompressionKind>(kind);
  }

  std::unique_ptr<Writer> createWriter(const Type& type, OutputStream* stream,
                                       const WriterOptions& options) {
    return std::unique_ptr<Writer>(new WriterImpl(type, stream, options));
  }

}  // namespace orc
