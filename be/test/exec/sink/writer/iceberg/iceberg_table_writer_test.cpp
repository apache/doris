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

#include <atomic>
#include <future>

#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exec/operator/iceberg_sorter_reserve_memory.h"
#include "exec/sink/writer/iceberg/viceberg_table_writer.h"
#include "exec/sink/writer/iceberg/vpartition_writer_base.h"
#include "runtime/runtime_state.h"

namespace doris {

namespace {

class FakePartitionWriter final : public IPartitionWriterBase {
public:
    explicit FakePartitionWriter(std::atomic<int>* destroyed = nullptr) : _destroyed(destroyed) {}
    ~FakePartitionWriter() override {
        if (_destroyed != nullptr) {
            ++(*_destroyed);
        }
    }
    Status open(RuntimeState*, RuntimeProfile*, const RowDescriptor*) override {
        return Status::OK();
    }
    Status write(Block&) override { return Status::OK(); }
    Status close(const Status&) override { return Status::OK(); }
    const std::string& file_name() const override { return _name; }
    int file_name_index() const override { return 0; }
    size_t written_len() const override { return 0; }

private:
    std::string _name = "fake";
    std::atomic<int>* _destroyed;
};

TDataSink make_sink() {
    TDataSink sink;
    sink.__set_type(TDataSinkType::ICEBERG_TABLE_SINK);
    sink.__set_iceberg_table_sink(TIcebergTableSink());
    return sink;
}

} // namespace

class VIcebergTableWriterLifecycleTest : public testing::Test {
protected:
    static Status select_block(VIcebergTableWriter* writer, Block& input,
                               const IColumn::Permutation& rows, Block* selected) {
        return writer->_select_block(input, rows, selected);
    }

    static void add_writer(VIcebergTableWriter* writer, std::string partition) {
        writer->_partitions_to_writers.emplace(std::move(partition),
                                               std::make_shared<FakePartitionWriter>());
    }

    static void add_writer(VIcebergTableWriter* writer, std::string partition,
                           std::shared_ptr<IPartitionWriterBase> partition_writer) {
        writer->_partitions_to_writers.emplace(std::move(partition), std::move(partition_writer));
    }

    static void clear_writers(VIcebergTableWriter* writer) {
        writer->_partitions_to_writers.clear();
    }

    static void publish_active_writers(VIcebergTableWriter* writer) {
        writer->_publish_active_writers();
    }
};

TEST_F(VIcebergTableWriterLifecycleTest, RejectsCoordinatorWithoutExternalFileReportAck) {
    VIcebergTableWriter writer(make_sink(), {}, nullptr, nullptr);
    RuntimeState state;
    RuntimeProfile profile("test");

    Status status = writer.open(&state, &profile);

    EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
    EXPECT_NE(std::string::npos, status.to_string().find("acknowledges external-file reports"));
}

TEST_F(VIcebergTableWriterLifecycleTest, SelectBlockUsesRowPermutation) {
    VIcebergTableWriter writer(make_sink(), {}, nullptr, nullptr);
    auto values = ColumnInt32::create();
    values->insert_value(10);
    values->insert_value(20);
    values->insert_value(30);
    Block input;
    input.insert({std::move(values), std::make_shared<DataTypeInt32>(), "value"});
    IColumn::Permutation rows {2, 0};
    Block selected;

    ASSERT_TRUE(select_block(&writer, input, rows, &selected).ok());

    const auto& result = assert_cast<const ColumnInt32&>(*selected.get_by_position(0).column);
    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result.get_element(0), 30);
    EXPECT_EQ(result.get_element(1), 10);
}

TEST_F(VIcebergTableWriterLifecycleTest, ColdReserveCoversManyRealPartitionSelections) {
    VIcebergTableWriter writer(make_sink(), {}, nullptr, nullptr);
    auto values = ColumnString::create();
    for (size_t i = 0; i < 128; ++i) {
        const std::string value(256 + i, static_cast<char>('a' + i % 26));
        values->insert_data(value.data(), value.size());
    }
    Block input;
    input.insert({std::move(values), std::make_shared<DataTypeString>(), "value"});
    size_t selected_bytes = 0;
    for (size_t row = 0; row < input.rows(); ++row) {
        Block selected;
        ASSERT_TRUE(select_block(&writer, input, {row}, &selected).ok());
        selected_bytes += selected.allocated_bytes();
    }

    const size_t reserve = iceberg_cold_writer_reserve_size(input, 0);
    EXPECT_GE(reserve,
              input.allocated_bytes() + 2 * selected_bytes + input.rows() * sizeof(size_t));
}

TEST_F(VIcebergTableWriterLifecycleTest, ActiveWriterSnapshotContainsEveryOpenPartition) {
    VIcebergTableWriter writer(make_sink(), {}, nullptr, nullptr);
    add_writer(&writer, "p=1");
    add_writer(&writer, "p=2");

    publish_active_writers(&writer);

    ASSERT_NE(writer.active_writers(), nullptr);
    EXPECT_EQ(writer.active_writers()->size(), 2);
}

TEST_F(VIcebergTableWriterLifecycleTest, LoadedSnapshotRetainsWritersDuringConcurrentPublication) {
    VIcebergTableWriter writer(make_sink(), {}, nullptr, nullptr);
    std::atomic<int> destroyed = 0;
    add_writer(&writer, "p=1", std::make_shared<FakePartitionWriter>(&destroyed));
    publish_active_writers(&writer);
    std::promise<void> snapshot_loaded;
    std::promise<void> replacement_published;

    auto reader = std::async(std::launch::async, [&]() {
        auto snapshot = writer.active_writers();
        snapshot_loaded.set_value();
        replacement_published.get_future().wait();
        EXPECT_EQ(1, snapshot->size());
        EXPECT_EQ("fake", snapshot->front()->file_name());
    });

    snapshot_loaded.get_future().wait();
    clear_writers(&writer);
    publish_active_writers(&writer);
    EXPECT_EQ(0, destroyed.load());
    replacement_published.set_value();
    reader.get();
    EXPECT_EQ(1, destroyed.load());
}

} // namespace doris
