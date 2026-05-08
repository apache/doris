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

#include "format/table/table_format_reader.h"

#include <gtest/gtest.h>

namespace doris {

class MockTableFormatReader : public TableFormatReader {
public:
    Status _do_get_next_block(Block*, size_t*, bool*) override { return Status::OK(); }
};

TEST(TableFormatReaderTest, FillSynthesizedColumnsInvokesRegisteredHandlers) {
    MockTableFormatReader reader;
    size_t handled_rows = 0;
    int handler_calls = 0;

    reader.register_synthesized_column_handler("synthetic",
                                               [&](Block* block, size_t rows) -> Status {
                                                   EXPECT_EQ(block, nullptr);
                                                   handled_rows = rows;
                                                   ++handler_calls;
                                                   return Status::OK();
                                               });

    EXPECT_TRUE(reader.has_synthesized_column_handlers());

    auto status = reader.fill_synthesized_columns(nullptr, 128);

    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(handler_calls, 1);
    EXPECT_EQ(handled_rows, 128u);
}

} // namespace doris
