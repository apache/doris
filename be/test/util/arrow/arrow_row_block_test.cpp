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

#include <sstream>

#include "common/logging.h"
#include "util/arrow/row_block.h"

#define ARROW_UTIL_LOGGING_H
#include <arrow/buffer.h>
#include <arrow/json/api.h>
#include <arrow/json/test_common.h>
#include <arrow/memory_pool.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>

#include "olap/row_block2.h"
#include "olap/schema.h"
#include "olap/tablet_schema_helper.h"

namespace doris {

class ArrowRowBlockTest : public testing::Test {
public:
    ArrowRowBlockTest() {}
    virtual ~ArrowRowBlockTest() {}
    std::string test_str() {
        return R"(
    { "c1": 1, "c2": 1.1 }
    { "c1": 2, "c2": 2.2 }
    { "c1": 3, "c2": 3.3 }
        )";
    }
    void MakeBuffer(const std::string& data, std::shared_ptr<arrow::Buffer>* out) {
        auto buffer_res = arrow::AllocateBuffer(data.size(), arrow::default_memory_pool());
        *out = std::move(buffer_res.ValueOrDie());
        std::copy(std::begin(data), std::end(data), (*out)->mutable_data());
    }
};

TEST_F(ArrowRowBlockTest, Normal) {
    auto json = test_str();
    std::shared_ptr<arrow::Buffer> buffer;
    MakeBuffer(test_str(), &buffer);
    arrow::json::ParseOptions parse_opts = arrow::json::ParseOptions::Defaults();
    parse_opts.explicit_schema = arrow::schema({
            arrow::field("c1", arrow::int64()),
    });

    auto arrow_st = arrow::json::ParseOne(parse_opts, buffer);
    EXPECT_TRUE(arrow_st.ok());
    std::shared_ptr<arrow::RecordBatch> record_batch = arrow_st.ValueOrDie();

    std::shared_ptr<Schema> schema;
    auto doris_st = convert_to_doris_schema(*record_batch->schema(), &schema);
    EXPECT_TRUE(doris_st.ok());

    std::shared_ptr<RowBlockV2> row_block;
    doris_st = convert_to_row_block(*record_batch, *schema, &row_block);
    EXPECT_TRUE(doris_st.ok());

    {
        std::shared_ptr<arrow::Schema> check_schema;
        doris_st = convert_to_arrow_schema(*schema, &check_schema);
        EXPECT_TRUE(doris_st.ok());
        arrow::MemoryPool* pool = arrow::default_memory_pool();
        std::shared_ptr<arrow::RecordBatch> check_batch;
        doris_st = convert_to_arrow_batch(*row_block, check_schema, pool, &check_batch);
        EXPECT_TRUE(doris_st.ok());
        EXPECT_EQ(3, check_batch->num_rows());
        EXPECT_TRUE(record_batch->Equals(*check_batch));
    }
}

} // namespace doris
