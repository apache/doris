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

#ifndef ORC_STRIPE_STREAM_HH
#define ORC_STRIPE_STREAM_HH

#include "orc/Int128.hh"
#include "orc/OrcFile.hh"
#include "orc/Reader.hh"

#include "ColumnReader.hh"
#include "Timezone.hh"
#include "TypeImpl.hh"

namespace orc {

  class RowReaderImpl;

  /**
   * StripeStream Implementation
   */

  class StripeStreamsImpl : public StripeStreams {
   private:
    const RowReaderImpl& reader;
    const proto::StripeInformation& stripeInfo;
    const proto::StripeFooter& footer;
    const uint64_t stripeIndex;
    const uint64_t stripeStart;
    InputStream& input;
    const std::unordered_map<orc::StreamId, std::shared_ptr<InputStream>>& streams;
    const Timezone& writerTimezone;
    const Timezone& readerTimezone;

   public:
    StripeStreamsImpl(
        const RowReaderImpl& reader, uint64_t index, const proto::StripeInformation& stripeInfo,
        const proto::StripeFooter& footer, uint64_t stripeStart, InputStream& input,
        const std::unordered_map<orc::StreamId, std::shared_ptr<InputStream>>& streams,
        const Timezone& writerTimezone, const Timezone& readerTimezone);

    virtual ~StripeStreamsImpl() override;

    virtual const std::vector<bool> getSelectedColumns() const override;

    virtual proto::ColumnEncoding getEncoding(uint64_t columnId) const override;

    virtual std::unique_ptr<SeekableInputStream> getStream(uint64_t columnId,
                                                           proto::Stream_Kind kind,
                                                           bool shouldStream) const override;

    MemoryPool& getMemoryPool() const override;

    ReaderMetrics* getReaderMetrics() const override;

    const Timezone& getWriterTimezone() const override;

    const Timezone& getReaderTimezone() const override;

    std::ostream* getErrorStream() const override;

    bool getThrowOnHive11DecimalOverflow() const override;

    bool isDecimalAsLong() const override;

    int32_t getForcedScaleOnHive11Decimal() const override;
  };

  /**
   * StreamInformation Implementation
   */

  class StreamInformationImpl : public StreamInformation {
   private:
    StreamKind kind;
    uint64_t column;
    uint64_t offset;
    uint64_t length;

   public:
    StreamInformationImpl(uint64_t _offset, const proto::Stream& stream)
        : kind(static_cast<StreamKind>(stream.kind())),
          column(stream.column()),
          offset(_offset),
          length(stream.length()) {
      // PASS
    }

    ~StreamInformationImpl() override;

    StreamKind getKind() const override {
      return kind;
    }

    uint64_t getColumnId() const override {
      return column;
    }

    uint64_t getOffset() const override {
      return offset;
    }

    uint64_t getLength() const override {
      return length;
    }
  };

  /**
   * StripeInformation Implementation
   */

  class StripeInformationImpl : public StripeInformation {
    uint64_t offset;
    uint64_t indexLength;
    uint64_t dataLength;
    uint64_t footerLength;
    uint64_t numRows;
    InputStream* stream;
    MemoryPool& memory;
    CompressionKind compression;
    uint64_t blockSize;
    ReaderMetrics* metrics;
    mutable proto::StripeFooter* stripeFooter;
    mutable std::unique_ptr<proto::StripeFooter> managedStripeFooter;
    void ensureStripeFooterLoaded() const;

   public:
    StripeInformationImpl(uint64_t _offset, uint64_t _indexLength, uint64_t _dataLength,
                          uint64_t _footerLength, uint64_t _numRows, InputStream* _stream,
                          MemoryPool& _memory, CompressionKind _compression, uint64_t _blockSize,
                          ReaderMetrics* _metrics, proto::StripeFooter* _stripeFooter)
        : offset(_offset),
          indexLength(_indexLength),
          dataLength(_dataLength),
          footerLength(_footerLength),
          numRows(_numRows),
          stream(_stream),
          memory(_memory),
          compression(_compression),
          blockSize(_blockSize),
          metrics(_metrics),
          stripeFooter(_stripeFooter) {
      // PASS
    }

    virtual ~StripeInformationImpl() override {
      // PASS
    }

    uint64_t getOffset() const override {
      return offset;
    }

    uint64_t getLength() const override {
      return indexLength + dataLength + footerLength;
    }
    uint64_t getIndexLength() const override {
      return indexLength;
    }

    uint64_t getDataLength() const override {
      return dataLength;
    }

    uint64_t getFooterLength() const override {
      return footerLength;
    }

    uint64_t getNumberOfRows() const override {
      return numRows;
    }

    uint64_t getNumberOfStreams() const override {
      ensureStripeFooterLoaded();
      return static_cast<uint64_t>(stripeFooter->streams_size());
    }

    std::unique_ptr<StreamInformation> getStreamInformation(uint64_t streamId) const override;

    ColumnEncodingKind getColumnEncoding(uint64_t colId) const override {
      ensureStripeFooterLoaded();
      return static_cast<ColumnEncodingKind>(stripeFooter->columns(static_cast<int>(colId)).kind());
    }

    uint64_t getDictionarySize(uint64_t colId) const override {
      ensureStripeFooterLoaded();
      return static_cast<ColumnEncodingKind>(
          stripeFooter->columns(static_cast<int>(colId)).dictionarysize());
    }

    const std::string& getWriterTimezone() const override {
      ensureStripeFooterLoaded();
      return stripeFooter->writertimezone();
    }
  };

}  // namespace orc

#endif
