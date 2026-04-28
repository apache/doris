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

#include "udf/python/python_udaf_client.h"

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>

namespace doris {

std::shared_ptr<arrow::RecordBatch> make_udaf_response(const std::optional<std::string>& error) {
    arrow::BooleanBuilder success_builder;
    std::shared_ptr<arrow::Array> success_array;
    EXPECT_TRUE(success_builder.Append(false).ok());
    EXPECT_TRUE(success_builder.Finish(&success_array).ok());

    arrow::Int64Builder rows_processed_builder;
    std::shared_ptr<arrow::Array> rows_processed_array;
    EXPECT_TRUE(rows_processed_builder.Append(0).ok());
    EXPECT_TRUE(rows_processed_builder.Finish(&rows_processed_array).ok());

    arrow::BinaryBuilder data_builder;
    std::shared_ptr<arrow::Array> data_array;
    if (error.has_value()) {
        EXPECT_TRUE(data_builder.Append(error->data(), static_cast<int32_t>(error->size())).ok());
    } else {
        EXPECT_TRUE(data_builder.AppendNull().ok());
    }
    EXPECT_TRUE(data_builder.Finish(&data_array).ok());

    auto schema = arrow::schema({
            arrow::field("success", arrow::boolean()),
            arrow::field("rows_processed", arrow::int64()),
            arrow::field("serialized_data", arrow::binary()),
    });
    return arrow::RecordBatch::Make(schema, 1, {success_array, rows_processed_array, data_array});
}

TEST(PythonUDAFClientTest, FailureStatusIncludesPythonErrorMessage) {
    auto response = make_udaf_response("finish failed");
    Status status = PythonUDAFClient::make_udaf_failure_status_for_test(response, "FINALIZE", 7);

    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("FINALIZE operation failed for place_id=7: finish failed"),
              std::string::npos);
}

TEST(PythonUDAFClientTest, FailureStatusFallsBackWhenErrorMessageIsNullOrEmpty) {
    Status null_status = PythonUDAFClient::make_udaf_failure_status_for_test(
            make_udaf_response(std::nullopt), "RESET", 8);
    EXPECT_FALSE(null_status.ok());
    EXPECT_NE(null_status.to_string().find("RESET operation failed for place_id=8"),
              std::string::npos);

    Status empty_status =
            PythonUDAFClient::make_udaf_failure_status_for_test(make_udaf_response(""), "MERGE", 9);
    EXPECT_FALSE(empty_status.ok());
    EXPECT_NE(empty_status.to_string().find("MERGE operation failed for place_id=9"),
              std::string::npos);
}

TEST(PythonUDAFClientTest, FailureStatusRejectsInvalidResponseShape) {
    Status null_status =
            PythonUDAFClient::make_udaf_failure_status_for_test(nullptr, "ACCUMULATE", 10);
    EXPECT_FALSE(null_status.ok());
    EXPECT_NE(null_status.to_string().find("Invalid ACCUMULATE failure response for place_id=10"),
              std::string::npos);

    auto zero_row_response = make_udaf_response("accumulate failed")->Slice(0, 0);
    Status zero_row_status = PythonUDAFClient::make_udaf_failure_status_for_test(zero_row_response,
                                                                                 "ACCUMULATE", 11);
    EXPECT_FALSE(zero_row_status.ok());
    EXPECT_NE(
            zero_row_status.to_string().find("Invalid ACCUMULATE failure response for place_id=11"),
            std::string::npos);

    auto response = make_udaf_response("reset failed");
    auto two_column_response = arrow::RecordBatch::Make(
            arrow::schema({response->schema()->field(0), response->schema()->field(1)}), 1,
            {response->column(0), response->column(1)});
    Status two_column_status =
            PythonUDAFClient::make_udaf_failure_status_for_test(two_column_response, "RESET", 12);
    EXPECT_FALSE(two_column_status.ok());
    EXPECT_NE(two_column_status.to_string().find("Invalid RESET failure response for place_id=12"),
              std::string::npos);
}

} // namespace doris
