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

#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "format/csv/csv_reader.h"
#include "format/transformer/vcsv_transformer.h"
#include "format_v2/delimited_text/csv_reader.h"
#include "io/fs/file_writer.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris {
namespace {

class CsvMemoryWriter final : public io::FileWriter {
public:
    Status appendv(const Slice* slices, size_t count) override {
        for (size_t i = 0; i < count; ++i) data.append(slices[i].data, slices[i].size);
        return Status::OK();
    }
    Status close(bool non_block = false) override {
        _state = State::CLOSED;
        return Status::OK();
    }
    const io::Path& path() const override { return _path; }
    size_t bytes_appended() const override { return data.size(); }
    State state() const override { return _state; }

    std::string data;

private:
    io::Path _path = "csv-memory";
    State _state = State::OPENED;
};

std::vector<DataTypePtr> file_types() {
    const auto file = make_nullable(std::make_shared<DataTypeFile>());
    return {file, std::make_shared<DataTypeArray>(file),
            std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), file),
            make_nullable(std::make_shared<DataTypeStruct>(DataTypes {file}, Strings {"f"}))};
}
} // namespace

TEST(VCsvFileTransformerTest, RejectsFileSchemaBeforeWritingEmptyOrNullRows) {
    RuntimeState state;
    state.set_timezone("UTC");
    for (const auto& type : file_types()) {
        SCOPED_TRACE(type->get_name());
        for (bool empty : {false, true}) {
            CsvMemoryWriter writer;
            const auto expressions = MockSlotRef::create_mock_contexts(DataTypes {type});
            VCSVTransformer transformer(&state, &writer, expressions, false, {}, {}, ",", "\n",
                                        false, TFileCompressType::PLAIN, nullptr);
            auto column = type->create_column();
            if (!empty) {
                column->insert_default();
            }
            Block block {{std::move(column), type, "file"}};
            const auto opened = transformer.open();
            const auto status = opened.ok() ? transformer.write(block) : opened;
            EXPECT_FALSE(status.ok());
            EXPECT_TRUE(writer.data.empty());
        }
    }
}

TEST(VCsvFileTransformerTest, BothReadersRejectFileSchemaDuringInitialization) {
    RuntimeState state;
    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_CSV_PLAIN);
    params.__set_compress_type(TFileCompressType::PLAIN);
    params.__isset.file_attributes = true;
    params.file_attributes.__isset.text_params = true;
    params.file_attributes.text_params.__set_column_separator(",");
    params.file_attributes.text_params.__set_line_delimiter("\n");
    TFileRangeDesc range;
    for (const auto& type : file_types()) {
        SCOPED_TRACE(type->get_name());
        SlotDescriptor slot;
        slot._type = type;
        std::vector<SlotDescriptor*> slots {&slot};
        auto reader = CsvReader::create_unique(&state, nullptr, nullptr, params, range, slots, 1024,
                                               nullptr);
        EXPECT_FALSE(reader->_init_options().ok());
        auto properties = std::make_shared<io::FileSystemProperties>();
        auto description = std::make_unique<io::FileDescription>();
        format::csv::CsvReader reader_v2(properties, description, nullptr, nullptr, &params, slots);
        EXPECT_FALSE(reader_v2._init_format_state().ok());
    }
}

TEST(VCsvFileTransformerTest, OrdinaryStringsRetainCsvOutput) {
    RuntimeState state;
    state.set_timezone("UTC");
    const auto type = std::make_shared<DataTypeString>();
    auto column = type->create_column();
    column->insert(Field::create_field<TYPE_STRING>("plain"));
    CsvMemoryWriter writer;
    const auto expressions = MockSlotRef::create_mock_contexts(DataTypes {type});
    VCSVTransformer transformer(&state, &writer, expressions, false, {}, {}, ",", "\n", false,
                                TFileCompressType::PLAIN, nullptr);
    ASSERT_TRUE(transformer.open().ok());
    ASSERT_TRUE(transformer.write(Block {{std::move(column), type, "text"}}).ok());
    EXPECT_EQ(writer.data, "plain\n");
}
} // namespace doris
