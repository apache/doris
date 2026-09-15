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

#include "format_v2/table/fluss_hybrid_reader.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "format_v2/table_reader.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_profile.h"

namespace doris::format::fluss {
namespace {

// Stands in for either child: it records what it was asked to prepare, which is all these tests
// need - the children's own behaviour has its own tests.
class RecordingChildReader : public format::TableReader {
public:
    Status prepare_split(const format::SplitReadOptions& options) override {
        prepared_ranges.push_back(options.current_range);
        return Status::OK();
    }

    Status get_block(Block* block, bool* eos) override {
        ++blocks_served;
        if (rows_per_block > 0) {
            auto column = ColumnInt32::create();
            column->insert_many_defaults(rows_per_block);
            block->insert({std::move(column), std::make_shared<DataTypeInt32>(), "row"});
        }
        *eos = true;
        return Status::OK();
    }

    Status close() override {
        closed = true;
        return Status::OK();
    }

    std::vector<TFileRangeDesc> prepared_ranges;
    int blocks_served = 0;
    // Rows this child puts into every block it serves, so the wrapper has something to count.
    size_t rows_per_block = 0;
    bool closed = false;
};

TFileRangeDesc fluss_range(const std::string& range_type) {
    TTableFormatFileDesc table_format_params;
    table_format_params.__set_table_format_type("fluss");
    table_format_params.__set_fluss_params({{"fluss.range_type", range_type}});
    TFileRangeDesc range;
    range.__set_table_format_params(std::move(table_format_params));
    range.__set_format_type(TFileFormatType::FORMAT_JNI);
    return range;
}

struct HybridFixture {
    FlussHybridReader reader;
    TFileScanRangeParams scan_params;
    RecordingChildReader* log_child = nullptr;
    RecordingChildReader* lake_child = nullptr;
    int log_children_built = 0;
    int lake_children_built = 0;

    Status open(RuntimeProfile* scanner_profile = nullptr) {
        reader._test_log_reader_factory = [this]() {
            ++log_children_built;
            auto child = std::make_unique<RecordingChildReader>();
            log_child = child.get();
            return child;
        };
        reader._test_lake_reader_factory = [this]() {
            ++lake_children_built;
            auto child = std::make_unique<RecordingChildReader>();
            lake_child = child.get();
            return child;
        };
        return reader.init({
                .projected_columns = {},
                .conjuncts = {},
                .format = format::FileFormat::JNI,
                .scan_params = &scan_params,
                .io_ctx = nullptr,
                .runtime_state = nullptr,
                .scanner_profile = scanner_profile,
        });
    }

    Status prepare(const std::string& range_type) {
        format::SplitReadOptions options;
        options.current_range = fluss_range(range_type);
        options.current_split_format = format::FileFormat::JNI;
        return reader.prepare_split(options);
    }
};

// The dispatch itself: every log-side kind reaches the JNI child, every lake-side kind reaches the
// lake child, and each child is built once however many splits it serves.
TEST(FlussHybridReaderTest, RoutesEachRangeKindToItsChild) {
    HybridFixture fixture;
    ASSERT_TRUE(fixture.open().ok());

    for (const auto* kind : {"LOG", "PK_FULL", "PK_TAIL"}) {
        ASSERT_TRUE(fixture.prepare(kind).ok()) << kind;
    }
    ASSERT_NE(fixture.log_child, nullptr);
    EXPECT_EQ(fixture.log_child->prepared_ranges.size(), 3);
    EXPECT_EQ(fixture.lake_child, nullptr);

    for (const auto* kind : {"LAKE", "LAKE_SUPPRESS"}) {
        ASSERT_TRUE(fixture.prepare(kind).ok()) << kind;
    }
    ASSERT_NE(fixture.lake_child, nullptr);
    EXPECT_EQ(fixture.lake_child->prepared_ranges.size(), 2);
    EXPECT_EQ(fixture.log_child->prepared_ranges.size(), 3);

    // Interleaving switches the active child without rebuilding either one - a rebuilt child would
    // lose the per-scan state its next split expects, e.g. the lake child's cached suppression.
    ASSERT_TRUE(fixture.prepare("LOG").ok());
    ASSERT_TRUE(fixture.prepare("LAKE_SUPPRESS").ok());
    EXPECT_EQ(fixture.log_children_built, 1);
    EXPECT_EQ(fixture.lake_children_built, 1);
}

// A block is asked of the child that prepared the CURRENT split, not of whichever child happened to
// serve an earlier one.
TEST(FlussHybridReaderTest, ReadsBlocksFromTheActiveSplitsChild) {
    HybridFixture fixture;
    ASSERT_TRUE(fixture.open().ok());
    ASSERT_TRUE(fixture.prepare("LOG").ok());
    ASSERT_TRUE(fixture.prepare("LAKE").ok());

    Block block;
    bool eos = false;
    ASSERT_TRUE(fixture.reader.get_block(&block, &eos).ok());
    EXPECT_EQ(fixture.lake_child->blocks_served, 1);
    EXPECT_EQ(fixture.log_child->blocks_served, 0);
}

// Not a default route: a range routed to the wrong side does not fail as a routing mistake, it
// fails as whatever that child makes of a foreign payload. The error names the value so the FE/BE
// mismatch it signals can actually be found.
TEST(FlussHybridReaderTest, UnknownRangeKindFailsLoudByName) {
    HybridFixture fixture;
    ASSERT_TRUE(fixture.open().ok());

    const auto unknown = fixture.prepare("UNION_PK");
    ASSERT_FALSE(unknown.ok());
    EXPECT_NE(unknown.to_string().find("UNION_PK"), std::string::npos);
    EXPECT_NE(unknown.to_string().find("fluss.range_type"), std::string::npos);

    format::SplitReadOptions no_params;
    no_params.current_range.__set_format_type(TFileFormatType::FORMAT_JNI);
    const auto missing = fixture.reader.prepare_split(no_params);
    ASSERT_FALSE(missing.ok());
    EXPECT_NE(missing.to_string().find("fluss.range_type"), std::string::npos);
}

// FileScannerV2's support check dispatches by the same key this reader routes by; the static
// answers exactly "which side would read this range".
TEST(FlussHybridReaderTest, LakeRangesAreToldApartByTheirRangeType) {
    EXPECT_TRUE(FlussHybridReader::is_lake_range(fluss_range("LAKE")));
    EXPECT_TRUE(FlussHybridReader::is_lake_range(fluss_range("LAKE_SUPPRESS")));
    EXPECT_FALSE(FlussHybridReader::is_lake_range(fluss_range("LOG")));
    EXPECT_FALSE(FlussHybridReader::is_lake_range(fluss_range("PK_FULL")));
    EXPECT_FALSE(FlussHybridReader::is_lake_range(fluss_range("PK_TAIL")));
    EXPECT_FALSE(FlussHybridReader::is_lake_range(TFileRangeDesc {}));
}

// ------------------------------------------------------------------ the profile

// Which kinds of range a scan read, and how many of each. A kind that never arrived leaves no
// counter at all: a pure log-table query's profile must not be padded with the lake and
// primary-key lines the dispatch can prove it never took.
TEST(FlussHybridReaderTest, CountsEachRangeKindItReadAndRegistersNoOthers) {
    RuntimeProfile profile("fluss_hybrid_reader_range_counts");
    HybridFixture fixture;
    ASSERT_TRUE(fixture.open(&profile).ok());

    ASSERT_TRUE(fixture.prepare("LOG").ok());
    ASSERT_TRUE(fixture.prepare("LOG").ok());
    ASSERT_TRUE(fixture.prepare("LAKE_SUPPRESS").ok());

    ASSERT_NE(profile.get_counter("FlussLogRangeNum"), nullptr);
    EXPECT_EQ(profile.get_counter("FlussLogRangeNum")->value(), 2);
    ASSERT_NE(profile.get_counter("FlussLakeSuppressRangeNum"), nullptr);
    EXPECT_EQ(profile.get_counter("FlussLakeSuppressRangeNum")->value(), 1);

    for (const auto* absent :
         {"FlussPkFullRangeNum", "FlussPkTailRangeNum", "FlussLakeRangeNum",
          "FlussPkFullRangeReadTime", "FlussPkTailRangeReadTime", "FlussLakeRangeReadTime"}) {
        EXPECT_EQ(profile.get_counter(absent), nullptr) << absent;
    }
}

// A side's metrics appear when its child is first built, so the half of the reader a scan never
// reached stays out of its profile entirely.
TEST(FlussHybridReaderTest, LeavesTheLakeSideOutOfAPureLogScansProfile) {
    RuntimeProfile profile("fluss_hybrid_reader_log_only");
    HybridFixture fixture;
    ASSERT_TRUE(fixture.open(&profile).ok());
    ASSERT_TRUE(fixture.prepare("LOG").ok());

    EXPECT_NE(profile.get_counter("FlussLogReadTime"), nullptr);
    EXPECT_NE(profile.get_counter("FlussLogRowsReturned"), nullptr);
    EXPECT_EQ(profile.get_counter("FlussLakeReadTime"), nullptr);
    EXPECT_EQ(profile.get_counter("FlussLakeRowsReturned"), nullptr);
}

// Time alone cannot tell "the lake half is slow" from "the lake half is where the rows are". The
// lake side's count is what the sibling returned minus what the log tail superseded, which is what
// the consumer downstream actually receives.
TEST(FlussHybridReaderTest, CountsTheRowsEachSideReturned) {
    RuntimeProfile profile("fluss_hybrid_reader_rows");
    HybridFixture fixture;
    ASSERT_TRUE(fixture.open(&profile).ok());

    Block block;
    bool eos = false;
    ASSERT_TRUE(fixture.prepare("LOG").ok());
    fixture.log_child->rows_per_block = 3;
    ASSERT_TRUE(fixture.reader.get_block(&block, &eos).ok());

    block.clear();
    ASSERT_TRUE(fixture.prepare("LAKE").ok());
    fixture.lake_child->rows_per_block = 5;
    ASSERT_TRUE(fixture.reader.get_block(&block, &eos).ok());

    ASSERT_NE(profile.get_counter("FlussLogRowsReturned"), nullptr);
    EXPECT_EQ(profile.get_counter("FlussLogRowsReturned")->value(), 3);
    ASSERT_NE(profile.get_counter("FlussLakeRowsReturned"), nullptr);
    EXPECT_EQ(profile.get_counter("FlussLakeRowsReturned")->value(), 5);
}

// The two layers, and the invariant that makes their difference readable: a side's timer covers
// everything the wrapper forwards to that child, standing it up and closing it included, while a
// range kind's timer covers only the per-range work. Their difference is therefore the one-off cost
// of the child itself - JNI class loading, the paimon stack's setup - which is a diagnosis of its
// own. Attributing close() to whichever range happened to be last would destroy that reading.
TEST(FlussHybridReaderTest, TimesEachSideMoreWidelyThanTheRangeKindsWithinIt) {
    RuntimeProfile profile("fluss_hybrid_reader_timers");
    HybridFixture fixture;
    ASSERT_TRUE(fixture.open(&profile).ok());
    ASSERT_TRUE(fixture.prepare("LOG").ok());
    fixture.log_child->rows_per_block = 1;
    Block block;
    bool eos = false;
    ASSERT_TRUE(fixture.reader.get_block(&block, &eos).ok());

    auto* side = profile.get_counter("FlussLogReadTime");
    auto* kind = profile.get_counter("FlussLogRangeReadTime");
    ASSERT_NE(side, nullptr);
    ASSERT_NE(kind, nullptr);
    EXPECT_GT(kind->value(), 0);
    EXPECT_GE(side->value(), kind->value());

    const auto side_before_close = side->value();
    const auto kind_before_close = kind->value();
    ASSERT_TRUE(fixture.reader.close().ok());
    EXPECT_GT(side->value(), side_before_close);
    EXPECT_EQ(kind->value(), kind_before_close);
}

TEST(FlussHybridReaderTest, ClosesEveryChildItBuilt) {
    HybridFixture fixture;
    ASSERT_TRUE(fixture.open().ok());
    ASSERT_TRUE(fixture.prepare("LOG").ok());
    ASSERT_TRUE(fixture.prepare("LAKE").ok());

    ASSERT_TRUE(fixture.reader.close().ok());
    EXPECT_TRUE(fixture.log_child->closed);
    EXPECT_TRUE(fixture.lake_child->closed);
}

} // namespace
} // namespace doris::format::fluss
