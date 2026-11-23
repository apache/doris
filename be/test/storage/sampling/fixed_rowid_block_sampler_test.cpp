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

#include "storage/sampling/fixed_rowid_block_sampler.h"

#include <gtest/gtest.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include <array>
#include <cmath>
#include <limits>
#include <set>

#include "common/exception.h"
#include "common/status.h"
#include "gen_cpp/PlanNodes_types.h"
#include "storage/sampling/dependent_rounding.h"
#include "storage/sampling/tablet_sample_plan.h"
#include "storage/sampling/tablet_sampling_algorithm_factory.h"

namespace doris {

TEST(FixedRowIdBlockSamplerTest, DomainHashUsesUnsignedBytes) {
    EXPECT_EQ(sampling_domain("\xff"), 0xaf64724c8602eb6eULL);
}

TEST(FixedRowIdBlockSamplerTest, ConstructorPreconditions) {
    const double nan = std::numeric_limits<double>::quiet_NaN();
#ifdef NDEBUG
    EXPECT_THROW(DependentRounding(nan, 0, 1), Exception);
    EXPECT_THROW(FixedRowIdBlockSampler(1, 0, 0, 1), Exception);
#else
    EXPECT_DEATH(DependentRounding(nan, 0, 1), "Check failed");
    EXPECT_DEATH(FixedRowIdBlockSampler(1, 0, 0, 1), "Check failed");
#endif
}

TEST(FixedRowIdBlockSamplerTest, CumulativeRounding) {
    const std::array<uint64_t, 10> counts {1, 0, 1, 2, 7, 1000000, 1, 5, 1, 8};
    std::array<uint64_t, counts.size()> totals {};
    constexpr double p = 0.125;
    constexpr int trials = 16384;
    for (int seed = 0; seed < trials; ++seed) {
        DependentRounding rounding(p, seed, 91);
        uint64_t population = 0;
        uint64_t selected = 0;
        for (size_t pos = 0; pos < counts.size(); ++pos) {
            population += counts[pos];
            const uint64_t next = rounding.cumulative(population);
            const uint64_t k = next - selected;
            EXPECT_GE(k, std::floor(p * counts[pos]));
            EXPECT_LE(k, std::ceil(p * counts[pos]));
            EXPECT_GE(next, std::floor(p * population));
            EXPECT_LE(next, std::ceil(p * population));
            selected = next;
            totals[pos] += k;
        }
    }
    for (size_t pos = 0; pos < counts.size(); ++pos) {
        EXPECT_NEAR(static_cast<double>(totals[pos]) / trials, p * counts[pos], 0.015);
    }
    EXPECT_EQ(DependentRounding(1, 0, 1).cumulative(UINT64_MAX), UINT64_MAX);
    EXPECT_EQ(DependentRounding(std::numeric_limits<double>::denorm_min(), 0, 1)
                      .cumulative(UINT64_MAX),
              0);
}

TEST(FixedRowIdBlockSamplerTest, FullEmptyAndCap) {
    const std::array<SampleSegment, 3> segments {{{{}, 3, 21}, {{}, 17, 0}, {{}, 41, 1}}};
    TabletSamplePlan plan;
    ASSERT_TRUE(FixedRowIdBlockSampler(1, 7, 10, 4).build(91, segments, &plan).ok());
    EXPECT_EQ(plan.population_units(), 4);
    EXPECT_EQ(plan.selected_units(), 4);
    EXPECT_EQ(plan.selected_rows(), 22);
    EXPECT_EQ(plan.segments()[0].range_size(), 1);
    EXPECT_EQ(plan.segments()[0].get_range_to(0), 21);
    EXPECT_TRUE(plan.segments()[1].is_empty());
    auto status = FixedRowIdBlockSampler(1, 7, 10, 3).build(91, segments, &plan);
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_NE(status.to_string().find(
                      "tablet_id=91 population_units=4 selected_units=4 exceed cap=3"),
              std::string::npos);
    size_t empty = 0;
    for (int seed = 0; seed < 100; ++seed) {
        ASSERT_TRUE(FixedRowIdBlockSampler(0.125, seed, 10, 1).build(91, segments, &plan).ok());
        if (plan.selected_rows() == 0) {
            ++empty;
            EXPECT_EQ(plan.selected_units(), 0);
            EXPECT_TRUE(std::ranges::all_of(plan.segments(), &segment_v2::RowRanges::is_empty));
        }
    }
    EXPECT_GT(empty, 0);
    EXPECT_LT(empty, 100);
}

TEST(FixedRowIdBlockSamplerTest, SkewAcrossOneToTenSegments) {
    std::vector<SampleSegment> segments;
    for (uint32_t pos = 0; pos < 10; ++pos) {
        segments.push_back({{}, pos, pos == 0 ? 100001U : pos + 1});
        for (int seed = 0; seed < 16; ++seed) {
            TabletSamplePlan plan;
            ASSERT_TRUE(FixedRowIdBlockSampler(0.1, seed, 32, 400).build(7, segments, &plan).ok());
            uint64_t units = 0;
            uint64_t rows = 0;
            ASSERT_EQ(plan.segments().size(), segments.size());
            for (size_t i = 0; i < segments.size(); ++i) {
                const auto& ranges = plan.segments()[i];
                for (size_t j = 0; j < ranges.range_size(); ++j) {
                    EXPECT_EQ(ranges.get_range_from(j) % 32, 0);
                    EXPECT_LE(ranges.get_range_to(j), segments[i].rows);
                    const auto n = ranges.get_range_to(j) - ranges.get_range_from(j);
                    units += (n + 31) / 32;
                    rows += n;
                }
            }
            EXPECT_EQ(units, plan.selected_units());
            EXPECT_EQ(rows, plan.selected_rows());
            EXPECT_GE(units, std::floor(0.1 * plan.population_units()));
            EXPECT_LE(units, std::ceil(0.1 * plan.population_units()));
        }
    }
}

TEST(FixedRowIdBlockSamplerTest, PartialBlocksFollowPhysicalIdentity) {
    // Each segment has eight blocks. Exactly four must be chosen independently of its position.
    std::array<SampleSegment, 3> segments {{{{}, 3, 71}, {{}, 17, 71}, {{}, 41, 71}}};
    bool saw_tail = false;
    bool saw_merged = false;
    bool saw_different = false;
    for (int seed = 0; seed < 32; ++seed) {
        TabletSamplePlan original;
        FixedRowIdBlockSampler sampler(0.5, seed, 10, 12);
        ASSERT_TRUE(sampler.build(91, segments, &original).ok());
        std::swap(segments.front(), segments.back());
        TabletSamplePlan reordered;
        ASSERT_TRUE(sampler.build(91, segments, &reordered).ok());
        for (size_t pos = 0; pos < segments.size(); ++pos) {
            const auto& ranges = original.segments()[pos];
            ASSERT_EQ(ranges.get_digest(7), reordered.segments()[2 - pos].get_digest(7));
            EXPECT_TRUE(ranges.count() == 40 || ranges.count() == 31);
            saw_tail |= ranges.count() == 31;
            saw_merged |= ranges.range_size() < 4;
        }
        saw_different |= original.segments().front().get_digest(7) !=
                         original.segments().back().get_digest(7);
    }
    EXPECT_TRUE(saw_different);
    EXPECT_TRUE(saw_tail);
    EXPECT_TRUE(saw_merged);
}

TEST(FixedRowIdBlockSamplerTest, ScannerBudgetsPreserveSparseRows) {
    std::vector<segment_v2::RowRanges> segments(4);
    segments[1].add({2, 7});
    segments[1].add({9, 12});
    segments[3].add({0, 3});
    for (int budget : {1, 3, 8, 9, 32}) {
        SampleRangeSplitter splitter(budget);
        std::set<std::pair<size_t, int64_t>> visited;
        int64_t current = 0;
        for (const auto& slice : splitter.split(segments)) {
            ASSERT_LE(slice.first_pos + slice.ranges.size(), segments.size());
            for (size_t idx = 0; idx < slice.ranges.size(); ++idx) {
                const auto pos = slice.first_pos + idx;
                const auto& ranges = slice.ranges[idx];
                EXPECT_EQ(ranges.is_empty(), segments[pos].is_empty());
                for (size_t j = 0; j < ranges.range_size(); ++j) {
                    for (auto row = ranges.get_range_from(j); row < ranges.get_range_to(j); ++row) {
                        EXPECT_TRUE(visited.emplace(pos, row).second);
                        EXPECT_TRUE(
                                (pos == 1 && ((row >= 2 && row < 7) || (row >= 9 && row < 12))) ||
                                (pos == 3 && row < 3));
                        ++current;
                    }
                }
            }
            EXPECT_LE(current, budget);
            EXPECT_EQ(slice.finishes_scanner, current == budget);
            if (slice.finishes_scanner) {
                current = 0;
            }
        }
        EXPECT_EQ(visited.size(), 11);
        // A following rowset consumes the unused budget in the same scanner.
        std::vector<segment_v2::RowRanges> next {segment_v2::RowRanges::create_single(budget)};
        auto slices = splitter.split(next);
        EXPECT_TRUE(slices.front().finishes_scanner);
        EXPECT_EQ(slices.front().ranges.front().count(), budget - current);
    }
}

TEST(FixedRowIdBlockSamplerTest, ProtocolValidation) {
    TTabletSampleOptions opts;
    std::unique_ptr<TabletSamplingAlgorithm> algo;
    auto check = [&] { return TabletSamplingAlgorithmFactory::create(opts, &algo); };
    EXPECT_TRUE(check().is<ErrorCode::INVALID_ARGUMENT>());
    opts.__set_protocol_version(2);
    EXPECT_TRUE(check().is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
    opts.__set_protocol_version(1);
    EXPECT_TRUE(check().is<ErrorCode::INVALID_ARGUMENT>());
    auto transport = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
    apache::thrift::protocol::TBinaryProtocol protocol(transport);
    protocol.writeStructBegin("TTabletSampleOptions");
    protocol.writeFieldBegin("algorithm", apache::thrift::protocol::T_I32, 2);
    protocol.writeI32(9);
    protocol.writeFieldEnd();
    protocol.writeFieldStop();
    protocol.writeStructEnd();
    opts.read(&protocol);
    EXPECT_TRUE(check().is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
    opts.__set_algorithm(TTabletSamplingAlgorithm::FIXED_ROWID_BLOCK);
    opts.__set_seed(0);
    opts.__set_tablet_inclusion_probability(1);
    TFixedRowIdBlockSampleParams params;
    params.__set_sample_unit_rows(10);
    params.__set_max_sample_units_per_tablet(0);
    opts.__set_fixed_rowid_block_params(params);
    EXPECT_TRUE(check().is<ErrorCode::INVALID_ARGUMENT>());
    for (double invalid : {0., -0.1, 1.1, std::numeric_limits<double>::quiet_NaN(),
                           std::numeric_limits<double>::infinity()}) {
        opts.__set_intra_tablet_unit_probability(invalid);
        EXPECT_TRUE(check().is<ErrorCode::INVALID_ARGUMENT>());
    }
    opts.__set_intra_tablet_unit_probability(0.5);
    ASSERT_TRUE(check().ok());
    opts.fixed_rowid_block_params.__set_max_sample_units_per_tablet(-1);
    EXPECT_TRUE(check().is<ErrorCode::INVALID_ARGUMENT>());
}

} // namespace doris
