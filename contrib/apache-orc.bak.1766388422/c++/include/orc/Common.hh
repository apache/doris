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

#ifndef ORC_COMMON_HH
#define ORC_COMMON_HH

#include "orc/Exceptions.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"

#include <string>

namespace orc {

  class FileVersion {
   private:
    uint32_t majorVersion;
    uint32_t minorVersion;

   public:
    static const FileVersion& v_0_11();
    static const FileVersion& v_0_12();
    static const FileVersion& UNSTABLE_PRE_2_0();

    FileVersion(uint32_t major, uint32_t minor) : majorVersion(major), minorVersion(minor) {}

    /**
     * Get major version
     */
    uint32_t getMajor() const {
      return this->majorVersion;
    }

    /**
     * Get minor version
     */
    uint32_t getMinor() const {
      return this->minorVersion;
    }

    bool operator==(const FileVersion& right) const {
      return this->majorVersion == right.getMajor() && this->minorVersion == right.getMinor();
    }

    bool operator!=(const FileVersion& right) const {
      return !(*this == right);
    }

    std::string toString() const;
  };

  enum WriterId {
    ORC_JAVA_WRITER = 0,
    ORC_CPP_WRITER = 1,
    PRESTO_WRITER = 2,
    SCRITCHLEY_GO = 3,
    TRINO_WRITER = 4,
    UNKNOWN_WRITER = INT32_MAX
  };

  std::string writerIdToString(uint32_t id);

  enum CompressionKind {
    CompressionKind_NONE = 0,
    CompressionKind_ZLIB = 1,
    CompressionKind_SNAPPY = 2,
    CompressionKind_LZO = 3,
    CompressionKind_LZ4 = 4,
    CompressionKind_ZSTD = 5,
    CompressionKind_MAX = INT32_MAX
  };

  /**
   * Get the name of the CompressionKind.
   */
  std::string compressionKindToString(CompressionKind kind);

  enum WriterVersion {
    WriterVersion_ORIGINAL = 0,
    WriterVersion_HIVE_8732 = 1,
    WriterVersion_HIVE_4243 = 2,
    WriterVersion_HIVE_12055 = 3,
    WriterVersion_HIVE_13083 = 4,
    WriterVersion_ORC_101 = 5,
    WriterVersion_ORC_135 = 6,
    WriterVersion_ORC_517 = 7,
    WriterVersion_ORC_203 = 8,
    WriterVersion_ORC_14 = 9,
    WriterVersion_MAX = INT32_MAX
  };

  /**
   * Get the name of the WriterVersion.
   */
  std::string writerVersionToString(WriterVersion kind);

  enum StreamKind {
    StreamKind_PRESENT = 0,
    StreamKind_DATA = 1,
    StreamKind_LENGTH = 2,
    StreamKind_DICTIONARY_DATA = 3,
    StreamKind_DICTIONARY_COUNT = 4,
    StreamKind_SECONDARY = 5,
    StreamKind_ROW_INDEX = 6,
    StreamKind_BLOOM_FILTER = 7,
    StreamKind_BLOOM_FILTER_UTF8 = 8
  };

  /**
   * Specific read intention when selecting a certain TypeId.
   * This enum currently only being utilized by LIST, MAP, and UNION type selection.
   */
  enum ReadIntent {
    ReadIntent_ALL = 0,

    // Only read the offsets of selected type. Do not read the children types.
    ReadIntent_OFFSETS = 1
  };

  /**
   * Get the string representation of the StreamKind.
   */
  std::string streamKindToString(StreamKind kind);

  class StreamInformation {
   public:
    virtual ~StreamInformation();

    virtual StreamKind getKind() const = 0;
    virtual uint64_t getColumnId() const = 0;
    virtual uint64_t getOffset() const = 0;
    virtual uint64_t getLength() const = 0;
  };

  enum ColumnEncodingKind {
    ColumnEncodingKind_DIRECT = 0,
    ColumnEncodingKind_DICTIONARY = 1,
    ColumnEncodingKind_DIRECT_V2 = 2,
    ColumnEncodingKind_DICTIONARY_V2 = 3
  };

  std::string columnEncodingKindToString(ColumnEncodingKind kind);

  class StreamId {
   public:
    StreamId(uint64_t columnId, StreamKind streamKind)
        : _columnId(columnId), _streamKind(streamKind) {}

    size_t hash() const {
      size_t h1 = std::hash<uint64_t>{}(_columnId);
      size_t h2 = std::hash<int>{}(static_cast<int>(_streamKind));
      return h1 ^ (h2 << 1);
    }

    bool operator==(const StreamId& other) const {
      return _columnId == other._columnId && _streamKind == other._streamKind;
    }

    bool operator<(const StreamId& other) const {
      if (_columnId != other._columnId) {
        return _columnId < other._columnId;
      }
      return static_cast<int>(_streamKind) < static_cast<int>(other._streamKind);
    }

    std::string toString() const {
      std::ostringstream oss;
      oss << "[columnId=" << _columnId << ", streamKind=" << static_cast<int>(_streamKind) << "]";
      return oss.str();
    }

    uint64_t columnId() const {
      return _columnId;
    }
    StreamKind streamKind() const {
      return _streamKind;
    }

   private:
    uint64_t _columnId;
    StreamKind _streamKind;
  };

  class StripeInformation {
   public:
    virtual ~StripeInformation();

    /**
     * Get the byte offset of the start of the stripe.
     * @return the bytes from the start of the file
     */
    virtual uint64_t getOffset() const = 0;

    /**
     * Get the total length of the stripe in bytes.
     * @return the number of bytes in the stripe
     */
    virtual uint64_t getLength() const = 0;

    /**
     * Get the length of the stripe's indexes.
     * @return the number of bytes in the index
     */
    virtual uint64_t getIndexLength() const = 0;

    /**
     * Get the length of the stripe's data.
     * @return the number of bytes in the stripe
     */
    virtual uint64_t getDataLength() const = 0;

    /**
     * Get the length of the stripe's tail section, which contains its index.
     * @return the number of bytes in the tail
     */
    virtual uint64_t getFooterLength() const = 0;

    /**
     * Get the number of rows in the stripe.
     * @return a count of the number of rows
     */
    virtual uint64_t getNumberOfRows() const = 0;

    /**
     * Get the number of streams in the stripe.
     */
    virtual uint64_t getNumberOfStreams() const = 0;

    /**
     * Get the StreamInformation for the given stream.
     */
    virtual std::unique_ptr<StreamInformation> getStreamInformation(uint64_t streamId) const = 0;

    /**
     * Get the column encoding for the given column.
     * @param colId the columnId
     */
    virtual ColumnEncodingKind getColumnEncoding(uint64_t colId) const = 0;

    /**
     * Get the dictionary size.
     * @param colId the columnId
     * @return the size of the dictionary or 0 if there isn't one
     */
    virtual uint64_t getDictionarySize(uint64_t colId) const = 0;

    /**
     * Get the writer timezone.
     */
    virtual const std::string& getWriterTimezone() const = 0;
  };

  // Return true if val1 < val2; otherwise return false
  template <typename T>
  inline bool compare(T val1, T val2) {
    return (val1 < val2);
  }

  // Specialization for Decimal
  template <>
  inline bool compare(Decimal val1, Decimal val2) {
    // compare integral parts
    Int128 integral1 = scaleDownInt128ByPowerOfTen(val1.value, val1.scale);
    Int128 integral2 = scaleDownInt128ByPowerOfTen(val2.value, val2.scale);

    if (integral1 < integral2) {
      return true;
    } else if (integral1 > integral2) {
      return false;
    }

    // integral parts are equal, continue comparing fractional parts
    // unnecessary to check overflow here because the scaled number will not
    // exceed original ones
    bool overflow = false, positive = val1.value >= 0;
    val1.value -= scaleUpInt128ByPowerOfTen(integral1, val1.scale, overflow);
    val2.value -= scaleUpInt128ByPowerOfTen(integral2, val2.scale, overflow);

    int32_t diff = val1.scale - val2.scale;
    if (diff > 0) {
      val2.value = scaleUpInt128ByPowerOfTen(val2.value, diff, overflow);
      if (overflow) {
        return positive ? true : false;
      }
    } else {
      val1.value = scaleUpInt128ByPowerOfTen(val1.value, -diff, overflow);
      if (overflow) {
        return positive ? false : true;
      }
    }

    if (val1.value < val2.value) {
      return true;
    }
    return false;
  }

  enum BloomFilterVersion {
    // Include both the BLOOM_FILTER and BLOOM_FILTER_UTF8 streams to support
    // both old and new readers.
    ORIGINAL = 0,
    // Only include the BLOOM_FILTER_UTF8 streams that consistently use UTF8.
    // See ORC-101
    UTF8 = 1,
    FUTURE = INT32_MAX
  };

  inline bool operator<(const Decimal& lhs, const Decimal& rhs) {
    return compare(lhs, rhs);
  }

  inline bool operator>(const Decimal& lhs, const Decimal& rhs) {
    return rhs < lhs;
  }

  inline bool operator<=(const Decimal& lhs, const Decimal& rhs) {
    return !(lhs > rhs);
  }

  inline bool operator>=(const Decimal& lhs, const Decimal& rhs) {
    return !(lhs < rhs);
  }

  inline bool operator!=(const Decimal& lhs, const Decimal& rhs) {
    return lhs < rhs || rhs < lhs;
  }

  inline bool operator==(const Decimal& lhs, const Decimal& rhs) {
    return !(lhs != rhs);
  }

}  // namespace orc

namespace std {
  template <>
  struct hash<orc::StreamId> {
    size_t operator()(const orc::StreamId& id) const {
      return id.hash();
    }
  };
}  // namespace std

#endif
