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

// The SNII BKD adapter across the WHOLE numeric type spread.
//
// bkd_differential_test already proves the CORE agrees with CLucene and with
// brute force for every one of these types. What it cannot see is the ADAPTER,
// which is where per-type breakage actually lives: SniiBkdIndexColumnWriter is
// not a template, so it walks the caller's array by field_type_size(type) and
// encodes with get_key_coder(type). If those two ever disagree for some type --
// a CppType wider than the coder's output, a coder that is not length
// preserving -- values land at the wrong offsets and every row's value is
// silently shifted.
//
// The primary oracle is IDENTITY: for each distinct written value, an
// exact-match lookup must return exactly the rows that value was written to. A
// stride or width bug scrambles the value/row association and fails this
// immediately, for any type, without the test needing to know how that type
// compares.
//
// Identity alone is not enough, though, and this file used to stop there. An
// exact-match answer only needs the encoding to be INJECTIVE, and its expected
// sets are built with the same coder the writer used -- so a coder that lost its
// sign flip would produce an oracle wrong in exactly the same way and every
// assertion would still pass. The data made it worse: ranks started at 0, so no
// value ever crossed zero and the sign flip could not be observed at all. Ranks
// now straddle zero, and a second oracle taken from the NATIVE values checks a
// RANGE -- the only query whose answer depends on the encoding being order
// PRESERVING (INV-1).

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/snii/bkd/bkd_reader.h"
#include "storage/index/snii/format/metadata_directory.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/snii_bkd_index_writer.h"
#include "storage/key_coder.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/types.h"

namespace doris::segment_v2 {
namespace {

using ::doris::snii::bkd::BkdReader;
using ::doris::snii::bkd::BkdSections;
using ::doris::snii::format::LogicalIndexMetadataRef;
using ::doris::snii::format::NamedBlobFileRef;

constexpr int64_t kIndexId = 21;
constexpr const char* kTestDir = "./ut_dir/snii_bkd_adapter_types_test";

void assert_ok(const Status& status) {
    ASSERT_TRUE(status.ok()) << status.to_string();
}

template <FieldType FT>
struct FieldTag {
    static constexpr FieldType kFieldType = FT;
};

// Same deterministic rank -> value mapping bkd_differential_test uses, so the
// composite CppTypes (decimal12_t, uint24_t, Int256) are constructed the one
// correct way rather than reinvented here.
template <FieldType FT>
struct ValueOf {
    using CppType = typename CppTypeTraits<FT>::CppType;
    static CppType from_rank(int64_t rank) {
        if constexpr (std::is_same_v<CppType, decimal12_t>) {
            decimal12_t value;
            value.integer = rank;
            value.fraction = static_cast<int32_t>((rank % 1000) * 1000000);
            return value;
        } else if constexpr (std::is_same_v<CppType, uint24_t>) {
            return uint24_t(static_cast<uint32_t>(rank & 0xFFFFFF));
        } else if constexpr (std::is_same_v<CppType, wide::Int256>) {
            return wide::Int256(rank);
        } else {
            return static_cast<CppType>(rank);
        }
    }
};

void init_index_meta(TabletIndex* meta) {
    TabletIndexPB pb;
    pb.set_index_type(IndexType::INVERTED);
    pb.set_index_id(kIndexId);
    pb.set_index_name("bkd_types_idx");
    pb.add_col_unique_id(0);
    meta->init_from_pb(pb);
}

template <typename Tag>
class SniiBkdAdapterTypesTest : public testing::Test {
protected:
    void SetUp() override {
        assert_ok(io::global_local_filesystem()->delete_directory(kTestDir));
        assert_ok(io::global_local_filesystem()->create_directory(kTestDir));
        init_index_meta(&_meta);
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }
    TabletIndex _meta;
};

using FieldTypeTags = ::testing::Types<
        FieldTag<FieldType::OLAP_FIELD_TYPE_BOOL>, FieldTag<FieldType::OLAP_FIELD_TYPE_TINYINT>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_SMALLINT>, FieldTag<FieldType::OLAP_FIELD_TYPE_INT>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_BIGINT>, FieldTag<FieldType::OLAP_FIELD_TYPE_LARGEINT>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_FLOAT>, FieldTag<FieldType::OLAP_FIELD_TYPE_DOUBLE>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_DECIMAL>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_DECIMAL32>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_DECIMAL64>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_DECIMAL128I>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_DECIMAL256>, FieldTag<FieldType::OLAP_FIELD_TYPE_DATE>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_DATETIME>, FieldTag<FieldType::OLAP_FIELD_TYPE_DATEV2>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_DATETIMEV2>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ>, FieldTag<FieldType::OLAP_FIELD_TYPE_IPV4>,
        FieldTag<FieldType::OLAP_FIELD_TYPE_IPV6>>;

TYPED_TEST_SUITE(SniiBkdAdapterTypesTest, FieldTypeTags);

TYPED_TEST(SniiBkdAdapterTypesTest, AdapterPreservesValueToRowAssociation) {
    constexpr FieldType FT = TypeParam::kFieldType;
    using CppType = typename CppTypeTraits<FT>::CppType;

    // 240 rows over 60 distinct ranks, so most values own several rows and a
    // shifted-by-one stride cannot coincidentally reproduce the mapping.
    constexpr int64_t kDistinct = 60;
    constexpr uint32_t kRows = 240;
    std::vector<CppType> values;
    values.reserve(kRows);
    for (uint32_t i = 0; i < kRows; ++i) {
        // Ranks STRADDLE zero. With a non-negative range the sign flip in the
        // signed coders is unobservable -- deleting it would reorder nothing, so
        // no assertion below could see it. Unsigned and narrow types wrap here,
        // which is harmless: every oracle is built from the resulting values.
        values.push_back(
                ValueOf<FT>::from_rank(static_cast<int64_t>(i % kDistinct) - kDistinct / 2));
    }

    const std::string prefix = std::string(kTestDir) + "/t";
    io::FileWriterPtr file_writer;
    assert_ok(io::global_local_filesystem()->create_file(
            InvertedIndexDescriptor::get_index_file_path_v2(prefix), &file_writer));
    IndexFileWriter index_file_writer(io::global_local_filesystem(), prefix, "test_rowset",
                                      /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                                      std::move(file_writer), /*can_use_ram_dir=*/true,
                                      /*tablet_id=*/302);
    SniiBkdIndexColumnWriter writer(&index_file_writer, &this->_meta, FT);
    assert_ok(writer.init());
    // ONE call over a contiguous CppType array: exactly how the segment writer
    // hands a run of rows over, and the shape the stride has to walk.
    assert_ok(writer.add_values("c1", values.data(), values.size()));
    assert_ok(writer.finish());
    assert_ok(index_file_writer.begin_close());
    assert_ok(index_file_writer.finish_close());

    ::doris::snii::io::LocalFileReader file;
    assert_ok(file.open(InvertedIndexDescriptor::get_index_file_path_v2(prefix)));
    ::doris::snii::reader::SniiSegmentReader segment;
    assert_ok(::doris::snii::reader::SniiSegmentReader::open(&file, &segment));
    const LogicalIndexMetadataRef* entry = nullptr;
    assert_ok(segment.blob_entry(static_cast<uint64_t>(kIndexId), "", &entry));
    ASSERT_NE(entry, nullptr);

    BkdSections sections;
    for (const NamedBlobFileRef& blob : entry->files) {
        if (blob.name == "bkd_data") {
            sections.data_offset = blob.offset;
            sections.data_length = blob.length;
        } else if (blob.name == "bkd_index") {
            sections.index_offset = blob.offset;
            sections.index_length = blob.length;
        }
    }
    std::unique_ptr<BkdReader> reader;
    assert_ok(BkdReader::open(&file, sections, &reader));

    // The width the adapter walked the array with IS the width recorded in the
    // header; if they could differ, everything below would still be consistent
    // with itself while describing the wrong bytes.
    EXPECT_EQ(reader->header().bytes_per_dim, field_type_size(FT));
    EXPECT_EQ(reader->header().field_type, FT);
    EXPECT_EQ(reader->point_count(), values.size());

    // IDENTITY oracle: every distinct value must resolve to exactly the rows it
    // was written to. Note some narrow types (BOOL, TINYINT) wrap over the rank
    // domain, so distinct ranks can share a value -- the expected set is built
    // from the written values themselves, never from the ranks.
    const KeyCoder* coder = get_key_coder(FT);
    std::map<std::string, roaring::Roaring> expected;
    for (uint32_t rid = 0; rid < values.size(); ++rid) {
        std::string encoded;
        coder->full_encode_ascending(&values[rid], &encoded);
        ASSERT_EQ(encoded.size(), field_type_size(FT))
                << "the coder is not length preserving for this type; the adapter's stride "
                   "assumption does not hold";
        expected[encoded].add(rid);
    }
    ASSERT_GT(expected.size(), 1U) << "a single distinct value would make this oracle vacuous";

    for (const auto& [encoded, rows] : expected) {
        const ::doris::snii::Slice value(reinterpret_cast<const uint8_t*>(encoded.data()),
                                         encoded.size());
        roaring::Roaring hits;
        assert_ok(reader->range(value, true, value, true, &hits));
        EXPECT_TRUE(hits == rows) << "exact-match lookup returned the wrong row set for field type "
                                  << static_cast<int>(FT);
    }

    // ORDER oracle, taken from the NATIVE values rather than the encoded bytes.
    //
    // Everything above is order-independent: exact match only needs the encoding
    // to be injective, and its expected sets come from the same coder the writer
    // used -- so a coder that lost its sign flip would produce an oracle wrong in
    // exactly the same way and every assertion would still pass. A range is what
    // distinguishes them, because it is the only query whose answer depends on
    // the encoding being order PRESERVING (INV-1).
    {
        // The MEDIAN of the actual values, not values[n/2] -- the row order is
        // rank order, and rank 0 lands on the minimum here, which would put every
        // row above the pivot. Sorting natively also keeps this correct for the
        // unsigned and narrow types, whose ranks wrap and therefore do not run in
        // value order at all.
        std::vector<CppType> sorted = values;
        std::sort(sorted.begin(), sorted.end(),
                  [](const CppType& a, const CppType& b) { return a < b; });
        const CppType pivot = sorted[sorted.size() / 2];
        roaring::Roaring expected_ge;
        for (uint32_t rid = 0; rid < values.size(); ++rid) {
            if (!(values[rid] < pivot)) {
                expected_ge.add(rid);
            }
        }
        ASSERT_FALSE(expected_ge.isEmpty());
        ASSERT_LT(expected_ge.cardinality(), values.size())
                << "the pivot excludes nothing; this cannot distinguish an order-breaking coder";

        std::string encoded_pivot;
        coder->full_encode_ascending(&pivot, &encoded_pivot);
        const ::doris::snii::Slice lower(reinterpret_cast<const uint8_t*>(encoded_pivot.data()),
                                         encoded_pivot.size());
        roaring::Roaring hits;
        assert_ok(reader->range(lower, true, ::doris::snii::Slice(), true, &hits));
        EXPECT_TRUE(hits == expected_ge)
                << "range(>= pivot) disagrees with native ordering for field type "
                << static_cast<int>(FT) << "; the coder is not order preserving";
    }
}

} // namespace
} // namespace doris::segment_v2
