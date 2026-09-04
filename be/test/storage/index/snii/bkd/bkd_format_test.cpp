// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/index/snii/bkd/bkd_format.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string_view>

#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/null_bitmap.h"

using namespace doris::snii::bkd;

// Lock down the on-disk contract values of the native BKD format. These are
// format semantics once published: changing any of them requires bumping
// kFormatVersion, so an accidental edit must break this test.
TEST(SniiBkdFormat, MagicAndVersionStable) {
    // "BKD1" read as big-endian: 'B'=0x42 'K'=0x4B 'D'=0x44 '1'=0x31.
    EXPECT_EQ(kBkdIndexMagic, 0x424B4431U);
    EXPECT_EQ(kFormatVersion, 1U);
    EXPECT_EQ(kSupportedVersion, 1U);
    // A reader must accept exactly what this binary writes.
    EXPECT_EQ(kSupportedVersion, kFormatVersion);
}

TEST(SniiBkdFormat, IndexSectionTypeDoesNotCollide) {
    // The SectionFramer type byte is a single flat namespace shared with the
    // inverted-index sections and the null-bitmap POD.
    EXPECT_NE(kBkdIndexSectionType, doris::snii::format::kNullBitmapSectionType);
    EXPECT_NE(kBkdIndexSectionType,
              static_cast<uint8_t>(doris::snii::format::SectionType::kSampledTermIndex));
    EXPECT_NE(kBkdIndexSectionType,
              static_cast<uint8_t>(doris::snii::format::SectionType::kDictBlockDirectory));
    EXPECT_NE(kBkdIndexSectionType,
              static_cast<uint8_t>(doris::snii::format::SectionType::kCoreMetadataPB));
    EXPECT_NE(kBkdIndexSectionType,
              static_cast<uint8_t>(doris::snii::format::SectionType::kSampledTermIndexZstd));
    EXPECT_NE(kBkdIndexSectionType,
              static_cast<uint8_t>(doris::snii::format::SectionType::kDictBlockDirectoryZstd));
    EXPECT_NE(kBkdIndexSectionType,
              static_cast<uint8_t>(doris::snii::format::SectionType::kNormsPod));
}

// Design doc 5.2: the leaf value_mode byte is a closed 3-value enum. The old
// CLucene encoding overloaded -1 / -2 / sorted_dim; these three are disjoint and
// exhaustive.
TEST(SniiBkdFormat, LeafValueModeEncoding) {
    EXPECT_EQ(static_cast<uint8_t>(LeafValueMode::kAllEqual), 0);
    EXPECT_EQ(static_cast<uint8_t>(LeafValueMode::kRle), 1);
    EXPECT_EQ(static_cast<uint8_t>(LeafValueMode::kRaw), 2);
    EXPECT_EQ(static_cast<uint8_t>(kMaxLeafValueMode), 2);
}

// Design doc 5.1: header `flags` bit0 = built_with_spill (diagnostic only).
TEST(SniiBkdFormat, IndexFlagBits) {
    EXPECT_EQ(index_flags::kBuiltWithSpill, 0x01U);
}

// Design doc 5: two blob sub-files, replacing the old three (bkd_meta folded
// into the bkd_index header).
TEST(SniiBkdFormat, BlobFileNames) {
    EXPECT_EQ(kBkdIndexFileName, std::string_view("bkd_index"));
    EXPECT_EQ(kBkdDataFileName, std::string_view("bkd_data"));
    EXPECT_NE(kBkdIndexFileName, kBkdDataFileName);
}

// Design doc 6.2: the build-time point record is [value][doc_id: 4 bytes BE],
// so a whole-record memcmp equals (value, doc_id) lexicographic order.
TEST(SniiBkdFormat, BuildTimeParameters) {
    EXPECT_EQ(kPointDocIdBytes, 4U);
    EXPECT_EQ(kDefaultPointsPerLeaf, 128U);
    EXPECT_EQ(kDefaultBuildBufferBytes, 256ULL << 20);
}
