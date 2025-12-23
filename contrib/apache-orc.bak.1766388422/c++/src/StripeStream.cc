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

#include "StripeStream.hh"
#include "RLE.hh"
#include "Reader.hh"
#include "orc/Exceptions.hh"

#include "wrap/coded-stream-wrapper.h"

namespace orc {

  StripeStreamsImpl::StripeStreamsImpl(
      const RowReaderImpl& _reader, uint64_t _index, const proto::StripeInformation& _stripeInfo,
      const proto::StripeFooter& _footer, uint64_t _stripeStart, InputStream& _input,
      const std::unordered_map<orc::StreamId, std::shared_ptr<InputStream>>& _streams,
      const Timezone& _writerTimezone, const Timezone& _readerTimezone)
      : reader(_reader),
        stripeInfo(_stripeInfo),
        footer(_footer),
        stripeIndex(_index),
        stripeStart(_stripeStart),
        input(_input),
        streams(_streams),
        writerTimezone(_writerTimezone),
        readerTimezone(_readerTimezone) {
    // PASS
  }

  StripeStreamsImpl::~StripeStreamsImpl() {
    // PASS
  }

  StreamInformation::~StreamInformation() {
    // PASS
  }

  StripeInformation::~StripeInformation() {
    // PASS
  }

  StreamInformationImpl::~StreamInformationImpl() {
    // PASS
  }

  const std::vector<bool> StripeStreamsImpl::getSelectedColumns() const {
    return reader.getSelectedColumns();
  }

  proto::ColumnEncoding StripeStreamsImpl::getEncoding(uint64_t columnId) const {
    return footer.columns(static_cast<int>(columnId));
  }

  const Timezone& StripeStreamsImpl::getWriterTimezone() const {
    return writerTimezone;
  }

  const Timezone& StripeStreamsImpl::getReaderTimezone() const {
    return readerTimezone;
  }

  std::ostream* StripeStreamsImpl::getErrorStream() const {
    return reader.getFileContents().errorStream;
  }

  std::unique_ptr<SeekableInputStream> StripeStreamsImpl::getStream(uint64_t columnId,
                                                                    proto::Stream_Kind kind,
                                                                    bool shouldStream) const {
    uint64_t offset = stripeStart;
    uint64_t dataEnd = stripeInfo.offset() + stripeInfo.indexlength() + stripeInfo.datalength();
    MemoryPool* pool = reader.getFileContents().pool;
    for (int i = 0; i < footer.streams_size(); ++i) {
      const proto::Stream& stream = footer.streams(i);
      if (stream.has_kind() && stream.kind() == kind &&
          stream.column() == static_cast<uint64_t>(columnId)) {
        auto iter = streams.find({columnId, static_cast<StreamKind>(kind)});
        InputStream* inputStream = (iter != streams.end()) ? iter->second.get() : &input;
        uint64_t streamLength = stream.length();
        uint64_t myBlock = shouldStream ? inputStream->getNaturalReadSize() : streamLength;
        if (offset + streamLength > dataEnd) {
          std::stringstream msg;
          msg << "Malformed stream meta at stream index " << i << " in stripe " << stripeIndex
              << ": streamOffset=" << offset << ", streamLength=" << streamLength
              << ", stripeOffset=" << stripeInfo.offset()
              << ", stripeIndexLength=" << stripeInfo.indexlength()
              << ", stripeDataLength=" << stripeInfo.datalength();
          throw ParseError(msg.str());
        }
        return createDecompressor(reader.getCompression(),
                                  std::make_unique<SeekableFileInputStream>(
                                      inputStream, offset, stream.length(), *pool, myBlock),
                                  reader.getCompressionSize(), *pool,
                                  reader.getFileContents().readerMetrics);
      }
      offset += stream.length();
    }
    return nullptr;
  }

  MemoryPool& StripeStreamsImpl::getMemoryPool() const {
    return *reader.getFileContents().pool;
  }

  ReaderMetrics* StripeStreamsImpl::getReaderMetrics() const {
    return reader.getFileContents().readerMetrics;
  }

  bool StripeStreamsImpl::getThrowOnHive11DecimalOverflow() const {
    return reader.getThrowOnHive11DecimalOverflow();
  }

  bool StripeStreamsImpl::isDecimalAsLong() const {
    return reader.getIsDecimalAsLong();
  }

  int32_t StripeStreamsImpl::getForcedScaleOnHive11Decimal() const {
    return reader.getForcedScaleOnHive11Decimal();
  }

  void StripeInformationImpl::ensureStripeFooterLoaded() const {
    if (stripeFooter == nullptr && managedStripeFooter.get() == nullptr) {
      std::unique_ptr<SeekableInputStream> pbStream =
          createDecompressor(compression,
                             std::make_unique<SeekableFileInputStream>(
                                 stream, offset + indexLength + dataLength, footerLength, memory),
                             blockSize, memory, metrics);
      managedStripeFooter = std::make_unique<proto::StripeFooter>();
      stripeFooter = managedStripeFooter.get();
      if (!stripeFooter->ParseFromZeroCopyStream(pbStream.get())) {
        throw ParseError("Failed to parse the stripe footer");
      }
    }
  }

  std::unique_ptr<StreamInformation> StripeInformationImpl::getStreamInformation(
      uint64_t streamId) const {
    ensureStripeFooterLoaded();
    uint64_t streamOffset = offset;
    for (uint64_t s = 0; s < streamId; ++s) {
      streamOffset += stripeFooter->streams(static_cast<int>(s)).length();
    }
    return std::make_unique<StreamInformationImpl>(
        streamOffset, stripeFooter->streams(static_cast<int>(streamId)));
  }

}  // namespace orc
