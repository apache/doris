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

#include <sstream>

namespace orc {

  std::string compressionKindToString(CompressionKind kind) {
    switch (static_cast<int>(kind)) {
      case CompressionKind_NONE:
        return "none";
      case CompressionKind_ZLIB:
        return "zlib";
      case CompressionKind_SNAPPY:
        return "snappy";
      case CompressionKind_LZO:
        return "lzo";
      case CompressionKind_LZ4:
        return "lz4";
      case CompressionKind_ZSTD:
        return "zstd";
    }
    std::stringstream buffer;
    buffer << "unknown - " << kind;
    return buffer.str();
  }

  std::string writerVersionToString(WriterVersion version) {
    switch (static_cast<int>(version)) {
      case WriterVersion_ORIGINAL:
        return "original";
      case WriterVersion_HIVE_8732:
        return "HIVE-8732";
      case WriterVersion_HIVE_4243:
        return "HIVE-4243";
      case WriterVersion_HIVE_12055:
        return "HIVE-12055";
      case WriterVersion_HIVE_13083:
        return "HIVE-13083";
      case WriterVersion_ORC_101:
        return "ORC-101";
      case WriterVersion_ORC_135:
        return "ORC-135";
      case WriterVersion_ORC_517:
        return "ORC-517";
      case WriterVersion_ORC_203:
        return "ORC-203";
      case WriterVersion_ORC_14:
        return "ORC-14";
    }
    std::stringstream buffer;
    buffer << "future - " << version;
    return buffer.str();
  }

  std::string writerIdToString(uint32_t id) {
    switch (id) {
      case ORC_JAVA_WRITER:
        return "ORC Java";
      case ORC_CPP_WRITER:
        return "ORC C++";
      case PRESTO_WRITER:
        return "Presto";
      case SCRITCHLEY_GO:
        return "Scritchley Go";
      case TRINO_WRITER:
        return "Trino";
      default: {
        std::ostringstream buffer;
        buffer << "Unknown(" << id << ")";
        return buffer.str();
      }
    }
  }

  std::string streamKindToString(StreamKind kind) {
    switch (static_cast<int>(kind)) {
      case StreamKind_PRESENT:
        return "present";
      case StreamKind_DATA:
        return "data";
      case StreamKind_LENGTH:
        return "length";
      case StreamKind_DICTIONARY_DATA:
        return "dictionary";
      case StreamKind_DICTIONARY_COUNT:
        return "dictionary count";
      case StreamKind_SECONDARY:
        return "secondary";
      case StreamKind_ROW_INDEX:
        return "index";
      case StreamKind_BLOOM_FILTER:
        return "bloom";
    }
    std::stringstream buffer;
    buffer << "unknown - " << kind;
    return buffer.str();
  }

  std::string columnEncodingKindToString(ColumnEncodingKind kind) {
    switch (static_cast<int>(kind)) {
      case ColumnEncodingKind_DIRECT:
        return "direct";
      case ColumnEncodingKind_DICTIONARY:
        return "dictionary";
      case ColumnEncodingKind_DIRECT_V2:
        return "direct rle2";
      case ColumnEncodingKind_DICTIONARY_V2:
        return "dictionary rle2";
    }
    std::stringstream buffer;
    buffer << "unknown - " << kind;
    return buffer.str();
  }

  std::string FileVersion::toString() const {
    if (majorVersion == 1 && minorVersion == 9999) {
      return "UNSTABLE-PRE-2.0";
    }
    std::stringstream ss;
    ss << majorVersion << '.' << minorVersion;
    return ss.str();
  }

  const FileVersion& FileVersion::v_0_11() {
    static FileVersion version(0, 11);
    return version;
  }

  const FileVersion& FileVersion::v_0_12() {
    static FileVersion version(0, 12);
    return version;
  }

  /**
   * Do not use this format except for testing. It will not be compatible
   * with other versions of the software. While we iterate on the ORC 2.0
   * format, we will make incompatible format changes under this version
   * without providing any forward or backward compatibility.
   *
   * When 2.0 is released, this version identifier will be completely removed.
   */
  const FileVersion& FileVersion::UNSTABLE_PRE_2_0() {
    static FileVersion version(1, 9999);
    return version;
  }
}  // namespace orc
