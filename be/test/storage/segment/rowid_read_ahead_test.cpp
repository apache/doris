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

#include "storage/segment/rowid_read_ahead.h"

#include <butil/iobuf.h>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "io/fs/read_ahead_metrics.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/schema.h"
#include "storage/segment/column_read_ahead_resources.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/mock/mock_segment.h"
#include "storage/segment/page_handle.h"
#include "storage/segment/page_io.h"
#include "util/coding.h"
#include "util/threadpool.h"

namespace doris::segment_v2 {
namespace {

class TestFileReader final : public io::FileReader {
public:
    explicit TestFileReader(std::string data) : _data(std::move(data)) {}

    Status close() override {
        _closed = true;
        return Status::OK();
    }
    const io::Path& path() const override { return _path; }
    size_t size() const override { return _data.size(); }
    bool closed() const override { return _closed; }
    int64_t mtime() const override { return 0; }

    size_t read_calls() const { return _read_calls; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_context) override {
        std::lock_guard lock(_mutex);
        ++_read_calls;
        _reads.push_back({.offset = offset, .size = result.size});
        if (_fail_next_read.exchange(false)) {
            *bytes_read = 0;
            return Status::IOError("injected read failure");
        }
        DORIS_CHECK(offset <= _data.size());
        const size_t length = std::min(result.size, _data.size() - offset);
        std::memcpy(result.data, _data.data() + offset, length);
        *bytes_read = length;
        if (io_context != nullptr && io_context->file_cache_stats != nullptr) {
            ++io_context->file_cache_stats->num_remote_io_total;
            io_context->file_cache_stats->bytes_read_from_remote += length;
        }
        if (io_context != nullptr && io_context->file_reader_stats != nullptr) {
            ++io_context->file_reader_stats->read_calls;
            io_context->file_reader_stats->read_bytes += length;
        }
        return Status::OK();
    }

private:
    const io::Path _path {"test-segment.dat"};
    const std::string _data;
    mutable std::mutex _mutex;
    std::vector<io::FileRange> _reads;
    std::atomic<bool> _fail_next_read {false};
    size_t _read_calls {0};
    bool _closed {false};
};

io::FileRangePlanOptions plan_options(size_t max_gap = 16) {
    return {.coalesce_options = {.max_gap_bytes = max_gap,
                                 .max_range_bytes = 1024,
                                 .max_read_amplification_ratio = 2.0},
            .cache_block_size = 1024,
            .block_fill_min_coverage = 1.0};
}

ThreadPool* read_ahead_test_executor() {
    static std::unique_ptr<ThreadPool> executor = []() {
        std::unique_ptr<ThreadPool> result;
        Status status = ThreadPoolBuilder("RowIdReadAheadTest")
                                .set_min_threads(4)
                                .set_max_threads(16)
                                .build(&result);
        DORIS_CHECK(status.ok());
        return result;
    }();
    return executor.get();
}

std::unique_ptr<io::FileRangeReadScheduler> make_scheduler(size_t max_bytes_per_query = 4096) {
    std::unique_ptr<io::FileRangeReadScheduler> scheduler;
    EXPECT_TRUE(io::FileRangeReadScheduler::create(
                        {.max_bytes_per_query = max_bytes_per_query, .max_bytes_per_be = 64 * 1024},
                        read_ahead_test_executor(), &scheduler)
                        .ok());
    return scheduler;
}

std::shared_ptr<const ColumnReadAheadResources> make_resources(
        const io::FileReaderSPtr& source, io::FileRangeReadScheduler* scheduler,
        ColumnReadAheadIOOptions options) {
    return std::make_shared<ColumnReadAheadResources>(
            source, scheduler, scheduler->create_context(), io::FileRangeReadIOContext {},
            std::move(options));
}

TEST(RowIdReadAheadTest, ReadsSlotDefaultBeforeNewColumnReachesTabletSchema) {
    auto source = std::make_shared<TestFileReader>(std::string(256, 'x'));
    auto scheduler = make_scheduler();
    auto read_ahead = make_resources(
            source, scheduler.get(),
            {.range_plan = plan_options(), .page_cache_probe = {}, .range_consumer_factory = {}});
    auto schema = std::make_shared<TabletSchema>();
    testing::NiceMock<MockSegment> segment(schema);
    segment.set_file_reader_for_test(source);
    ON_CALL(segment, file_reader()).WillByDefault(testing::Return(source));
    auto footer = std::make_shared<SegmentFooterPB>();
    footer->set_num_rows(3);
    EXPECT_CALL(segment, _get_segment_footer(testing::_, testing::_, testing::_))
            .WillOnce([&](std::shared_ptr<SegmentFooterPB>& result, OlapReaderStatistics*,
                          const io::IOContext*) {
                result = footer;
                return Status::OK();
            });

    auto slot_data = TSlotDescriptorBuilder()
                             .type(TYPE_INT)
                             .column_name("new_column")
                             .set_nullIndicatorBit(-1)
                             .build();
    slot_data.__set_col_unique_id(123);
    slot_data.__set_col_default_value("42");
    SlotDescriptor slot(slot_data);
    MutableColumnPtr result = ColumnInt32::create();
    OlapReaderStatistics stats;
    StorageReadOptions options(stats);
    std::unique_ptr<ColumnIterator> iterator;
    RowIdColumnRead column {
            .slot = &slot, .result = &result, .read_options = &options, .iterator = &iterator};
    const std::vector<uint32_t> row_ids {0, 2};
    ASSERT_TRUE(read_columns_by_rowids_with_read_ahead(segment, *schema, row_ids,
                                                       std::span<RowIdColumnRead>(&column, 1),
                                                       read_ahead)
                        .ok());
    const auto& values = assert_cast<const ColumnInt32&>(*result).get_data();
    ASSERT_EQ(values.size(), 2);
    EXPECT_EQ(values[0], 42);
    EXPECT_EQ(values[1], 42);
    EXPECT_EQ(source->read_calls(), 0);
}

TEST(RowIdReadAheadTest, PointReadPreparationLeavesComplexIteratorsOnOriginalPath) {
    auto source = std::make_shared<TestFileReader>(std::string(256, 'x'));
    auto scheduler = make_scheduler();
    auto read_ahead = make_resources(
            source, scheduler.get(),
            {.range_plan = plan_options(), .page_cache_probe = {}, .range_consumer_factory = {}});
    const auto integer = std::make_shared<DataTypeInt32>();
    const DataTypes types {
            std::make_shared<DataTypeArray>(integer),
            std::make_shared<DataTypeMap>(integer, integer),
            std::make_shared<DataTypeStruct>(DataTypes {integer}, Strings {"child"})};
    for (const auto& type : types) {
        SCOPED_TRACE(type->get_name());
        auto slot_type = type->to_thrift();
        auto slot_data = TSlotDescriptorBuilder()
                                 .set_slotType(slot_type)
                                 .column_name("complex_column")
                                 .set_nullIndicatorBit(-1)
                                 .build();
        slot_data.__set_col_unique_id(123);
        SlotDescriptor slot(slot_data);
        auto schema = std::make_shared<TabletSchema>();
        schema->append_column(ReadSchema::tablet_column_from_slot(slot));
        testing::StrictMock<MockSegment> segment(schema);
        OlapReaderStatistics stats;
        StorageReadOptions options(stats);
        std::unique_ptr<ColumnIterator> iterator;
        RowIdColumnRead column {.slot = &slot, .read_options = &options, .iterator = &iterator};
        const std::vector<uint32_t> row_ids {0, 2};
        ASSERT_TRUE(prepare_columns_by_rowids_with_read_ahead(
                            segment, *schema, row_ids, std::span<RowIdColumnRead>(&column, 1),
                            read_ahead)
                            .ok());
        EXPECT_EQ(iterator, nullptr);
    }
    EXPECT_EQ(source->read_calls(), 0);
}

} // namespace
} // namespace doris::segment_v2
