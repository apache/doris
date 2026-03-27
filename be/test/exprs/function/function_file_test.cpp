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

#include <memory>
#include <string>
#include <vector>

#include "core/binary_cast.hpp"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_file.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/primitive_type.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/file_schema_descriptor.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/simple_function_factory.h"
#include "service/http/ev_http_server.h"
#include "service/http/http_channel.h"
#include "service/http/http_handler.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "testutil/column_helper.h"

namespace doris {

namespace {

class ToFileMetadataHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        req->add_output_header(HttpHeaders::ETAG, "\"etag-123\"");
        req->add_output_header(HttpHeaders::LAST_MODIFIED, "Fri, 01 Mar 2024 10:00:00 GMT");
        if (req->method() == HttpMethod::HEAD) {
            HttpChannel::send_reply(req, HttpStatus::FORBIDDEN, "signature mismatch");
            return;
        }

        EXPECT_EQ(req->header(HttpHeaders::RANGE), "bytes=0-0");
        req->add_output_header(HttpHeaders::CONTENT_RANGE, "bytes 0-0/123");
        req->add_output_header(HttpHeaders::CONTENT_LENGTH, "1");
        HttpChannel::send_reply(req, HttpStatus::PARTIAL_CONTENT, "x");
    }
};

} // namespace

TEST(FunctionFileTest, toFileBuildsExpectedPhysicalFields) {
    EvHttpServer server(0);
    ToFileMetadataHandler handler;
    server.register_handler(HEAD, "/bucket/path/image.JPG", &handler);
    server.register_handler(GET, "/bucket/path/image.JPG", &handler);
    server.start();
    std::string object_url = "http://127.0.0.1:" + std::to_string(server.get_real_port()) +
                             "/bucket/path/image.JPG?X-Amz-Signature=test-signature";

    auto string_type = std::make_shared<DataTypeString>();
    ColumnsWithTypeAndName arguments {
            ColumnWithTypeAndName {ColumnHelper::create_column<DataTypeString>(
                                           {object_url}),
                                   string_type, "object_url"}};

    auto result_type = std::make_shared<DataTypeFile>();
    auto function =
            SimpleFunctionFactory::instance().get_function("to_file", arguments, result_type);
    ASSERT_TRUE(function);

    Block block(arguments);
    block.insert(ColumnWithTypeAndName {nullptr, result_type, "result"});

    Status status = function->execute(nullptr, block, {0}, 1, 1);
    ASSERT_TRUE(status.ok()) << status;

    const auto& result_column = assert_cast<const ColumnFile&>(*block.get_by_position(1).column);
    const auto& object_uri_column = assert_cast<const ColumnString&>(result_column.get_subcolumn(0));
    const auto& file_name_column = assert_cast<const ColumnString&>(result_column.get_subcolumn(1));
    const auto& file_extension_column = assert_cast<const ColumnString&>(result_column.get_subcolumn(2));
    const auto& size_column = assert_cast<const ColumnInt64&>(result_column.get_subcolumn(3));
    const auto& etag_column = assert_cast<const ColumnNullable&>(result_column.get_subcolumn(4));
    const auto& etag_nested_column =
            assert_cast<const ColumnString&>(etag_column.get_nested_column());
    const auto& mtime_column = assert_cast<const ColumnDateTimeV2&>(result_column.get_subcolumn(5));

    DateV2Value<DateTimeV2ValueType> expected_time;
    expected_time.from_unixtime(1709287200, cctz::utc_time_zone());

    ASSERT_EQ(result_column.size(), 1);
    EXPECT_EQ(object_uri_column.get_data_at(0).to_string(), object_url);
    EXPECT_EQ(file_name_column.get_data_at(0).to_string(), "image.JPG");
    EXPECT_EQ(file_extension_column.get_data_at(0).to_string(), ".jpg");
    EXPECT_EQ(size_column.get_data()[0], 123);
    EXPECT_EQ(etag_column.get_null_map_data()[0], 0);
    EXPECT_EQ(etag_nested_column.get_data_at(0).to_string(), "\"etag-123\"");
    EXPECT_EQ(mtime_column.get_data()[0],
              binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(expected_time));
    EXPECT_TRUE(result_column.check_schema(FileSchemaDescriptor::instance()).ok());
}

} // namespace doris
