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
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/compaction/rowid_conversion.h"

namespace {

namespace ErrorCode = doris::ErrorCode;
using doris::Status;
using doris::snii::compaction::RowIdConversionMap;
using doris::snii::compaction::ValidatedRowIdConversion;
using doris::snii::compaction::validate_rowid_conversion;

constexpr uint32_t kDeleted = std::numeric_limits<uint32_t>::max();

void expect_invalid(const Status& status, std::string_view message_fragment) {
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
    EXPECT_NE(status.to_string().find(message_fragment), std::string::npos) << status;
}

TEST(SniiRowIdConversionTest, AcceptsCompleteKWayInterleavedMapping) {
    const RowIdConversionMap conversion = {
            {{0, 0}, {kDeleted, kDeleted}, {0, 2}, {1, 1}},
            {{0, 1}, {1, 0}},
    };

    EXPECT_TRUE(validate_rowid_conversion(conversion, {4, 2}, {3, 2}).ok());
}

TEST(SniiRowIdConversionTest, CreatesValidatedTokenWithStableShapeAndMappings) {
    const RowIdConversionMap conversion = {
            {{0, 0}, {kDeleted, kDeleted}, {0, 2}, {1, 1}},
            {{0, 1}, {1, 0}},
    };
    const std::vector<uint32_t> source_rows = {4, 2};
    const std::vector<uint32_t> destination_rows = {3, 2};

    std::unique_ptr<ValidatedRowIdConversion> validated;
    EXPECT_TRUE(
            ValidatedRowIdConversion::create(&conversion, source_rows, destination_rows, &validated)
                    .ok());
    ASSERT_NE(validated, nullptr);
    EXPECT_EQ(validated->source_segment_count(), 2U);
    EXPECT_EQ(validated->source_segment_doc_counts(), source_rows);
    EXPECT_EQ(validated->destination_segment_doc_counts(), destination_rows);
    ASSERT_EQ(validated->source_mapping(1).size(), 2U);
    EXPECT_EQ(validated->source_mapping(1)[0], (std::pair<uint32_t, uint32_t> {0, 1}));
    EXPECT_TRUE(validated->source_has_deletions(0));
    EXPECT_FALSE(validated->source_has_deletions(1));
}

TEST(SniiRowIdConversionTest, RejectsInvalidMappingAndClearsOutputToken) {
    const RowIdConversionMap valid_conversion = {{{0, 0}}};
    const std::vector<uint32_t> source_rows = {1};
    const std::vector<uint32_t> destination_rows = {1};
    std::unique_ptr<ValidatedRowIdConversion> validated;
    ASSERT_TRUE(ValidatedRowIdConversion::create(&valid_conversion, source_rows, destination_rows,
                                                 &validated)
                        .ok());
    ASSERT_NE(validated, nullptr);

    const RowIdConversionMap incomplete_conversion = {{{0, 0}}};
    const std::vector<uint32_t> incomplete_destination_rows = {2};
    const Status status = ValidatedRowIdConversion::create(&incomplete_conversion, source_rows,
                                                           incomplete_destination_rows, &validated);
    expect_invalid(status, "missing destination ordinal");
    EXPECT_EQ(validated, nullptr);
}

TEST(SniiRowIdConversionTest, AcceptsEmptyDestinationAndDeletedSources) {
    const RowIdConversionMap conversion = {
            {},
            {{kDeleted, kDeleted}, {kDeleted, kDeleted}},
    };

    EXPECT_TRUE(validate_rowid_conversion(conversion, {0, 2}, {}).ok());
}

TEST(SniiRowIdConversionTest, RejectsSourceShapeMismatch) {
    expect_invalid(validate_rowid_conversion({{}}, {}, {}), "source segment count");
    expect_invalid(validate_rowid_conversion({{{kDeleted, kDeleted}}}, {2}, {}),
                   "source doc count");
}

TEST(SniiRowIdConversionTest, RejectsHalfDeletedEntries) {
    expect_invalid(validate_rowid_conversion({{{kDeleted, 0}}}, {1}, {1}), "partially deleted");
    expect_invalid(validate_rowid_conversion({{{0, kDeleted}}}, {1}, {1}), "partially deleted");
}

TEST(SniiRowIdConversionTest, RejectsLiveDestinationOutsideBounds) {
    expect_invalid(validate_rowid_conversion({{{1, 0}}}, {1}, {1}), "destination segment");
    expect_invalid(validate_rowid_conversion({{{0, 1}}}, {1}, {1}), "destination row");
}

TEST(SniiRowIdConversionTest, RejectsNonIncreasingSourceOrdinals) {
    expect_invalid(validate_rowid_conversion({{{1, 0}, {0, 1}}}, {2}, {2, 1}),
                   "not strictly increasing");
    expect_invalid(validate_rowid_conversion({{{0, 0}, {0, 0}}}, {2}, {1}),
                   "not strictly increasing");
}

TEST(SniiRowIdConversionTest, RejectsDuplicateDestinationAcrossSources) {
    const RowIdConversionMap conversion = {
            {{0, 0}},
            {{0, 0}},
    };

    expect_invalid(validate_rowid_conversion(conversion, {1, 1}, {1}),
                   "duplicate destination ordinal");
}

TEST(SniiRowIdConversionTest, RejectsIncompleteDestinationCoverage) {
    const RowIdConversionMap conversion = {
            {{0, 0}, {0, 2}},
            {{kDeleted, kDeleted}},
    };

    expect_invalid(validate_rowid_conversion(conversion, {2, 1}, {3}),
                   "missing destination ordinal");
}

TEST(SniiRowIdConversionTest, UsesWideGlobalOrdinalsAcrossUint32Boundary) {
    const RowIdConversionMap conversion = {{{1, 1}}};

    const Status status =
            validate_rowid_conversion(conversion, {1}, {std::numeric_limits<uint32_t>::max(), 2});
    expect_invalid(status, "4294967296");
}

} // namespace
