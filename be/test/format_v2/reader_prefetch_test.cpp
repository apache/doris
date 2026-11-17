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

#include "common/object_pool.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "format_v2/delimited_text/csv_reader.h"
#include "format_v2/delimited_text/text_reader.h"
#include "format_v2/json/json_reader.h"
#include "io/fs/buffered_reader.h"
#include "io/io_common.h"
#include "runtime/runtime_state.h"
#include "testutil/desc_tbl_builder.h"

namespace doris::format {
namespace {

class SequentialTextPrefetchTest : public testing::TestWithParam<TFileFormatType::type> {};

TEST_P(SequentialTextPrefetchTest, RetainsSharedContextAndAlignedSplitRange) {
    ObjectPool pool;
    DescriptorTblBuilder descriptors(&pool);
    descriptors.declare_tuple() << TupleDescBuilder::SlotType {
            make_nullable(std::make_shared<DataTypeString>()), "value"};
    const auto slots = descriptors.build()->get_tuple_descriptor(0)->slots();
    auto properties = std::make_shared<io::FileSystemProperties>();
    properties->system_type = TFileType::FILE_S3;
    properties->properties = {{"AWS_ENDPOINT", "http://s3.example.invalid"},
                              {"AWS_REGION", "us-east-1"},
                              {"AWS_ACCESS_KEY", "test-access-key"},
                              {"AWS_SECRET_KEY", "test-secret-key"},
                              {"use_path_style", "true"}};
    // A known length permits constructing the real S3 reader without issuing network requests.
    auto description = std::make_unique<io::FileDescription>();
    description->path = "s3://reader-test/data";
    description->file_size = 8 * 1024 * 1024;
    description->range_start_offset = 64;
    description->range_size = 2 * 1024 * 1024;
    TFileRangeDesc range;
    range.__set_start_offset(description->range_start_offset);
    range.__set_size(description->range_size);
    range.__set_file_size(description->file_size);
    range.__set_path(description->path);
    TFileScanRangeParams params;
    params.__set_file_type(TFileType::FILE_S3);
    params.__set_format_type(GetParam());
    params.__set_column_idxs({0});
    TFileTextScanRangeParams text;
    text.__set_column_separator(",");
    text.__set_line_delimiter("\n");
    TFileAttributes attributes;
    attributes.__set_text_params(text);
    attributes.__set_read_json_by_line(true);
    params.__set_file_attributes(attributes);
    TQueryOptions options;
    options.__set_enable_file_cache(false);
    RuntimeState state {options, TQueryGlobals()};
    RuntimeProfile profile("sequential-reader");
    auto context = std::make_shared<io::IOContext>();
    std::unique_ptr<FileReader> reader;
    if (GetParam() == TFileFormatType::FORMAT_JSON) {
        reader = std::make_unique<json::JsonReader>(properties, description, context, &profile,
                                                    &params, range, slots);
    } else if (GetParam() == TFileFormatType::FORMAT_TEXT) {
        reader = std::make_unique<text::TextReader>(properties, description, context, &profile,
                                                    &params, slots);
    } else {
        reader = std::make_unique<csv::CsvReader>(properties, description, context, &profile,
                                                  &params, slots);
    }
    ASSERT_TRUE(reader->init(&state).ok());
    auto request = std::make_shared<FileScanRequest>();
    request->non_predicate_columns = {LocalColumnIndex::top_level(LocalColumnId(0))};
    ASSERT_TRUE(reader->open(request).ok());
    auto* prefetch = dynamic_cast<io::PrefetchBufferedReader*>(reader->_file_reader.get());
    ASSERT_NE(prefetch, nullptr);
    EXPECT_EQ(prefetch->_io_ctx_holder.get(), context.get());
    EXPECT_EQ(prefetch->_file_range.start_offset, 63);
    EXPECT_EQ(prefetch->_file_range.end_offset, 64 + 2 * 1024 * 1024);
    EXPECT_NE(profile.get_counter("PreRequestIO"), nullptr);
    EXPECT_TRUE(reader->close().ok());
}

INSTANTIATE_TEST_SUITE_P(Formats, SequentialTextPrefetchTest,
                         testing::Values(TFileFormatType::FORMAT_CSV_PLAIN,
                                         TFileFormatType::FORMAT_TEXT,
                                         TFileFormatType::FORMAT_JSON));

} // namespace
} // namespace doris::format
