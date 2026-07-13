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

#include "format_v2/jni/jni_table_reader.h"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "io/io_common.h"

namespace doris::format {
namespace {

class FakeJniTableReader final : public JniTableReader {
public:
    int get_next_calls = 0;
    int close_calls = 0;
    int64_t close_profile_delta = 0;
    RuntimeProfile::Counter* close_profile_counter = nullptr;
    std::vector<size_t> open_batch_sizes;
    std::vector<size_t> propagated_batch_sizes;
    std::vector<Status> close_results;
    bool next_eof = false;

protected:
    std::string connector_class() const override { return "test/FakeJniScanner"; }

    Status build_scanner_params(std::map<std::string, std::string>* params) const override {
        params->clear();
        return Status::OK();
    }

    Status _get_next_jni_block(size_t* rows, bool* eof) override {
        ++get_next_calls;
        *rows = 0;
        *eof = next_eof;
        return Status::OK();
    }

    Status _close_jni_scanner() override {
        if (!TEST_scanner_opened()) {
            return Status::OK();
        }
        ++close_calls;
        if (_reserve_split_profile_publication() && close_profile_counter != nullptr) {
            COUNTER_UPDATE(close_profile_counter, close_profile_delta);
        }
        Status status = Status::OK();
        if (static_cast<size_t>(close_calls) <= close_results.size()) {
            status = close_results[close_calls - 1];
        }
        if (status.ok()) {
            TEST_set_split_state(false, TEST_eof());
        }
        return status;
    }

    Status _set_open_scanner_batch_size(size_t batch_size) override {
        propagated_batch_sizes.push_back(batch_size);
        return Status::OK();
    }

    Status _open_jni_scanner() override {
        open_batch_sizes.push_back(TEST_batch_size());
        TEST_set_split_state(true, false);
        return Status::OK();
    }
};

Status init_reader(FakeJniTableReader* reader, const std::shared_ptr<io::IOContext>& io_ctx,
                   RuntimeProfile* scanner_profile = nullptr) {
    return reader->init({
            .projected_columns = {},
            .conjuncts = {},
            .format = FileFormat::JNI,
            .scan_params = nullptr,
            .io_ctx = io_ctx,
            .runtime_state = nullptr,
            .scanner_profile = scanner_profile,
    });
}

TEST(JniTableReaderTest, CancellationStopsBeforeFetchingAnotherJavaBatch) {
    auto io_ctx = std::make_shared<io::IOContext>();
    FakeJniTableReader reader;
    ASSERT_TRUE(init_reader(&reader, io_ctx).ok());
    reader.TEST_set_split_state(true, false);
    io_ctx->should_stop = true;

    Block block;
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(reader.get_next_calls, 0);
    EXPECT_EQ(reader.close_calls, 1);
}

TEST(JniTableReaderTest, EndOfSplitRemainsIdempotentAfterScannerClose) {
    FakeJniTableReader reader;
    ASSERT_TRUE(init_reader(&reader, nullptr).ok());
    reader.TEST_set_split_state(true, false);
    reader.next_eof = true;

    Block block;
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_TRUE(reader.TEST_eof());
    EXPECT_FALSE(reader.TEST_scanner_opened());

    eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(reader.get_next_calls, 1);
    EXPECT_EQ(reader.close_calls, 1);
}

TEST(JniTableReaderTest, FailedCloseCanBeRetried) {
    RuntimeProfile profile("FailedCloseCanBeRetried");
    FakeJniTableReader reader;
    reader.close_profile_counter = profile.add_counter("PublishedCloseProfile", TUnit::UNIT);
    reader.close_profile_delta = 17;
    ASSERT_TRUE(init_reader(&reader, nullptr, &profile).ok());
    reader.TEST_set_split_state(true, false);
    reader.close_results = {Status::InternalError("injected close failure"), Status::OK()};

    EXPECT_FALSE(reader.close().ok());
    EXPECT_FALSE(reader.TEST_closed());
    EXPECT_TRUE(reader.TEST_scanner_opened());
    EXPECT_EQ(reader.close_calls, 1);
    EXPECT_EQ(reader.close_profile_counter->value(), 17);

    EXPECT_TRUE(reader.close().ok());
    EXPECT_TRUE(reader.TEST_closed());
    EXPECT_EQ(reader.close_calls, 2);
    EXPECT_EQ(reader.close_profile_counter->value(), 17);
}

TEST(JniTableReaderTest, AdaptiveBatchSizeUpdatesAnOpenJavaScanner) {
    FakeJniTableReader reader;
    ASSERT_TRUE(init_reader(&reader, nullptr).ok());

    reader.set_batch_size(17);
    EXPECT_EQ(reader.TEST_batch_size(), 17);
    EXPECT_TRUE(reader.propagated_batch_sizes.empty());

    reader.TEST_set_split_state(true, false);
    reader.set_batch_size(33);
    EXPECT_EQ(reader.TEST_batch_size(), 33);
    EXPECT_EQ(reader.propagated_batch_sizes, std::vector<size_t>({33}));
}

TEST(JniTableReaderTest, AdaptiveProbeSetBeforePrepareControlsFirstJniOpen) {
    FakeJniTableReader reader;
    ASSERT_TRUE(init_reader(&reader, nullptr).ok());

    reader.set_batch_size(32);
    ASSERT_TRUE(reader.prepare_split({
                                             .partition_values = {},
                                             .conjuncts = std::nullopt,
                                             .partition_prune_conjuncts = {},
                                             .cache = nullptr,
                                             .current_range = {},
                                             .current_split_format = FileFormat::JNI,
                                             .global_rowid_context = std::nullopt,
                                     })
                        .ok());

    EXPECT_EQ(reader.open_batch_sizes, std::vector<size_t>({32}));
    EXPECT_TRUE(reader.TEST_scanner_opened());
}

} // namespace
} // namespace doris::format
