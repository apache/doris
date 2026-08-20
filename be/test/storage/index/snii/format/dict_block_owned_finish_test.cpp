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

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/zstd_codec.h"
#include "storage/index/snii/format/dict_block.h"

namespace doris::snii::format {
namespace {

DictEntry MakeInlineEntry(std::string term, uint8_t value, size_t payload_size = 1024) {
    DictEntry entry;
    entry.term = std::move(term);
    entry.kind = DictEntryKind::kInline;
    entry.enc = DictEntryEnc::kSlim;
    entry.df = 32;
    entry.frq_bytes.assign(payload_size, value);
    entry.inline_dd_disk_len = entry.frq_bytes.size();
    entry.dd_meta.uncomp_len = entry.frq_bytes.size();
    entry.dd_meta.disk_len = entry.frq_bytes.size();
    entry.dd_meta.verify_crc = false;
    return entry;
}

TEST(DictBlockOwnedFinishTest, PreservesBytesWithBoundedCapacity) {
    DictBlockBuilder block(IndexTier::kT1, /*has_positions=*/false, /*frq_base=*/17,
                           /*prx_base=*/0, /*anchor_interval=*/2);
    block.add_entry(MakeInlineEntry("\x1fGcommon", 0x11));
    block.add_entry(MakeInlineEntry("alpha", 0x22));
    block.add_entry(MakeInlineEntry("alphabet", 0x33));
    block.add_entry(MakeInlineEntry("beta", 0x44));

    ByteSink legacy;
    block.finish(&legacy);
    std::vector<uint8_t> owned = block.finish_owned();

    EXPECT_EQ(owned, legacy.buffer());
    EXPECT_LT(owned.capacity(), legacy.buffer().capacity());

    std::vector<uint8_t> legacy_compressed;
    std::vector<uint8_t> owned_compressed;
    ASSERT_TRUE(zstd_compress(legacy.view(), /*level=*/3, &legacy_compressed).ok());
    ASSERT_TRUE(zstd_compress(Slice(owned), /*level=*/3, &owned_compressed).ok());
    EXPECT_EQ(owned_compressed, legacy_compressed);

    DictBlockReader reader;
    ASSERT_TRUE(
            DictBlockReader::open(Slice(owned), IndexTier::kT1, /*has_positions=*/false, &reader)
                    .ok());
    EXPECT_EQ(reader.n_entries(), 4U);
    bool found = false;
    DictEntry decoded;
    ASSERT_TRUE(reader.find_term("alphabet", &found, &decoded).ok());
    ASSERT_TRUE(found);
    EXPECT_EQ(decoded.term, "alphabet");
    EXPECT_EQ(decoded.frq_bytes, std::vector<uint8_t>(1024, 0x33));
}

TEST(DictBlockOwnedFinishTest, DoesNotReservePrefixCompressionUpperBound) {
    DictBlockBuilder block(IndexTier::kT1, /*has_positions=*/false, /*frq_base=*/0,
                           /*prx_base=*/0, /*anchor_interval=*/16);
    const std::string common_prefix(2048, 'x');
    for (char suffix = 'a'; suffix <= 'p'; ++suffix) {
        block.add_entry(MakeInlineEntry(common_prefix + suffix, 0x11, /*payload_size=*/1));
    }

    ByteSink legacy;
    block.finish(&legacy);
    std::vector<uint8_t> owned = block.finish_owned();

    EXPECT_EQ(owned, legacy.buffer());
    EXPECT_LT(owned.capacity(), legacy.buffer().capacity());
    EXPECT_LT(owned.capacity(), block.estimated_bytes());
}

} // namespace
} // namespace doris::snii::format
