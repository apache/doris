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

#include "format/json/new_json_reader.h"

namespace doris {

// Test that set_batch_size stores the value correctly.
TEST(NewJsonReaderSetBatchSizeTest, SetBatchSizeStoresValue) {
    TFileScanRangeParams params;
    params.format_type = TFileFormatType::FORMAT_JSON;
    params.__isset.file_attributes = true;
    params.file_attributes.__isset.text_params = true;
    params.file_attributes.text_params.line_delimiter = "\n";

    TFileRangeDesc range;
    range.path = "/nonexistent/test.json";
    range.start_offset = 0;
    range.size = 0;

    std::vector<SlotDescriptor*> file_slot_descs;
    // Use the second constructor (profile, params, range, file_slot_descs, io_ctx)
    // to avoid the first constructor's ADD_TIMER(_profile, ...) which crashes on nullptr.
    auto reader = NewJsonReader::create_unique(nullptr, params, range, file_slot_descs, nullptr);

    // Default: _batch_size should be 0 (not set)
    EXPECT_EQ(reader->get_batch_size(), 4064U);

    // After set_batch_size, it should store the value
    reader->set_batch_size(128);
    EXPECT_EQ(reader->get_batch_size(), 128U);

    // Calling set_batch_size multiple times should update the value.
    reader->set_batch_size(256);
    EXPECT_EQ(reader->get_batch_size(), 256U);

    // Setting to 0 should revert to default behavior.
    reader->set_batch_size(0);
    EXPECT_EQ(reader->get_batch_size(), 0U);
}

// Test that set_batch_size is callable via the GenericReader interface.
TEST(NewJsonReaderSetBatchSizeTest, SetBatchSizeViaGenericInterface) {
    TFileScanRangeParams params;
    params.format_type = TFileFormatType::FORMAT_JSON;
    params.__isset.file_attributes = true;
    params.file_attributes.__isset.text_params = true;
    params.file_attributes.text_params.line_delimiter = "\n";

    TFileRangeDesc range;
    range.path = "/nonexistent/test.json";
    range.start_offset = 0;
    range.size = 0;

    std::vector<SlotDescriptor*> file_slot_descs;
    // Use the second constructor to avoid nullptr profile crash in ADD_TIMER.
    auto reader = NewJsonReader::create_unique(nullptr, params, range, file_slot_descs, nullptr);

    // Access through base class pointer — this is how FileScanner calls it.
    GenericReader* base_reader = reader.get();
    base_reader->set_batch_size(128);
    EXPECT_EQ(base_reader->get_batch_size(), 128U);
    base_reader->set_batch_size(4096);
    EXPECT_EQ(base_reader->get_batch_size(), 4096U);
}

} // namespace doris
