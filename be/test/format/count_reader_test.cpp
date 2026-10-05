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

#include "format/count_reader.h"

#include <gtest/gtest.h>

#include <memory>

#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_number.h"

namespace doris {

class CountReaderTest : public testing::Test {
protected:
    // Helper: create a Block with one nullable Int64 column (empty).
    static Block make_block() {
        Block block;
        auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
        block.insert({type->create_column(), type, "count(*)"});
        return block;
    }

    // Helper: drain a CountReader, return total rows read.
    static int64_t drain(CountReader& reader) {
        int64_t total = 0;
        bool eof = false;
        while (!eof) {
            Block block = make_block();
            size_t read_rows = 0;
            auto st = reader.get_next_block(&block, &read_rows, &eof);
            EXPECT_TRUE(st.ok()) << st.to_string();
            total += read_rows;
        }
        return total;
    }
};

TEST_F(CountReaderTest, ZeroRows) {
    CountReader reader(0, 4096);

    Block block = make_block();
    size_t read_rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader.get_next_block(&block, &read_rows, &eof).ok());
    EXPECT_EQ(read_rows, 0);
    EXPECT_TRUE(eof);
    EXPECT_EQ(block.rows(), 0);
}

TEST_F(CountReaderTest, SingleBatch) {
    const int64_t total = 100;
    const size_t batch_size = 4096;
    CountReader reader(total, batch_size);

    Block block = make_block();
    size_t read_rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader.get_next_block(&block, &read_rows, &eof).ok());
    EXPECT_EQ(read_rows, total);
    EXPECT_TRUE(eof);
    EXPECT_EQ(block.rows(), total);
}

TEST_F(CountReaderTest, MultipleBatches) {
    const int64_t total = 10000;
    const size_t batch_size = 4096;
    CountReader reader(total, batch_size);

    int64_t accumulated = 0;
    int batches = 0;
    bool eof = false;
    while (!eof) {
        Block block = make_block();
        size_t read_rows = 0;
        ASSERT_TRUE(reader.get_next_block(&block, &read_rows, &eof).ok());
        accumulated += read_rows;
        batches++;
        if (!eof) {
            EXPECT_EQ(read_rows, batch_size);
        }
    }
    EXPECT_EQ(accumulated, total);
    EXPECT_EQ(batches, 3); // 4096 + 4096 + 1808
}

TEST_F(CountReaderTest, ExactMultiple) {
    const int64_t total = 8192;
    const size_t batch_size = 4096;
    CountReader reader(total, batch_size);

    int64_t accumulated = drain(reader);
    EXPECT_EQ(accumulated, total);
}

TEST_F(CountReaderTest, NullInnerReader) {
    const int64_t total = 500;
    CountReader reader(total, 4096, nullptr);

    EXPECT_EQ(reader.get_total_rows(), total);
    EXPECT_TRUE(reader.count_read_rows());
    EXPECT_EQ(reader.get_push_down_agg_type(), TPushAggOp::type::COUNT);

    int64_t accumulated = drain(reader);
    EXPECT_EQ(accumulated, total);
}

TEST_F(CountReaderTest, GetTotalRowsAfterPartialConsume) {
    const int64_t total = 10000;
    const size_t batch_size = 4096;
    CountReader reader(total, batch_size);

    // Read one batch
    Block block = make_block();
    size_t read_rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader.get_next_block(&block, &read_rows, &eof).ok());
    EXPECT_EQ(read_rows, batch_size);
    EXPECT_FALSE(eof);

    // get_total_rows should still return the original total, not remaining
    EXPECT_EQ(reader.get_total_rows(), total);
}

TEST_F(CountReaderTest, WithInnerReader) {
    // Create a simple inner reader (another CountReader as mock)
    auto inner = std::make_unique<CountReader>(42, 4096);
    CountReader reader(100, 4096, std::move(inner));

    // get_total_rows delegates to inner reader
    EXPECT_EQ(reader.get_total_rows(), 42);
    EXPECT_NE(reader.inner_reader(), nullptr);
}

TEST_F(CountReaderTest, BatchSizeOne) {
    const int64_t total = 5;
    const size_t batch_size = 1;
    CountReader reader(total, batch_size);

    int batches = 0;
    bool eof = false;
    while (!eof) {
        Block block = make_block();
        size_t read_rows = 0;
        ASSERT_TRUE(reader.get_next_block(&block, &read_rows, &eof).ok());
        if (!eof || read_rows > 0) {
            EXPECT_EQ(read_rows, 1);
            batches++;
        }
    }
    EXPECT_EQ(batches, 5);
}

TEST_F(CountReaderTest, CloseWithNullInner) {
    CountReader reader(0, 4096, nullptr);
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(CountReaderTest, MultipleColumns) {
    const int64_t total = 50;
    const size_t batch_size = 4096;
    CountReader reader(total, batch_size);

    Block block;
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    block.insert({type->create_column(), type, "col1"});
    block.insert({type->create_column(), type, "col2"});
    block.insert({type->create_column(), type, "col3"});

    size_t read_rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader.get_next_block(&block, &read_rows, &eof).ok());
    EXPECT_EQ(read_rows, total);
    EXPECT_TRUE(eof);
    // All columns should be resized uniformly
    for (size_t i = 0; i < block.columns(); i++) {
        EXPECT_EQ(block.get_by_position(i).column->size(), total);
    }
}

} // namespace doris
