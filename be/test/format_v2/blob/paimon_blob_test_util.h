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

#pragma once

#include <gtest/gtest.h>

#include <fstream>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "core/block/block.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_nullable.h"
#include "format_v2/blob/blob_test_util.h"
#include "format_v2/blob/paimon_blob_column_reader.h"
#include "io/file_factory.h"
#include "io/io_common.h"
#include "testutil/scoped_temp_dir.h"

namespace doris::format::blob_test {

// Use real local FileFactory reads without a JVM, object store, or a Parquet scan.
class PaimonBlobTest : public ::testing::Test {
protected:
    PaimonBlobTest() { _system_properties.system_type = TFileType::FILE_LOCAL; }

    TPaimonBlobFileDesc write_blob(const std::string& name, int32_t field_id, int64_t first_row_id,
                                   const std::vector<std::optional<std::string>>& payloads) {
        return write_blob_bytes(name, field_id, first_row_id, static_cast<int64_t>(payloads.size()),
                                make_blob_file(payloads));
    }

    TPaimonBlobFileDesc write_blob_bytes(const std::string& name, int32_t field_id,
                                         int64_t first_row_id, int64_t row_count,
                                         const std::string& bytes) {
        const auto path = (_temp_directory.path() / name).string();
        std::ofstream out(path, std::ios::binary);
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        out.close();
        EXPECT_TRUE(out.good()) << path;
        TPaimonBlobFileDesc file;
        file.__set_field_id(field_id);
        file.__set_path(path);
        file.__set_file_size(static_cast<int64_t>(bytes.size()));
        file.__set_first_row_id(first_row_id);
        file.__set_row_count(row_count);
        return file;
    }

    static ColumnDefinition blob_column(std::string name, int32_t field_id, bool nullable = true) {
        ColumnDefinition column;
        column.name = std::move(name);
        column.identifier = Field::create_field<TYPE_INT>(field_id);
        column.type = std::make_shared<DataTypeFile>();
        if (nullable) {
            column.type = make_nullable(column.type);
        }
        return column;
    }

    static auto row_positions(std::initializer_list<int64_t> positions) {
        auto column = ColumnInt64::create();
        for (const auto position : positions) {
            column->insert_value(position);
        }
        return column;
    }

    paimon::PaimonBlobColumnReader::ReadContext read_context(int64_t max_size = 10485760) {
        return {.system_properties = _system_properties,
                .runtime_state = nullptr,
                .io_ctx = &_io_context,
                .profile = nullptr,
                .fs_name = _fs_name,
                .file_cache_admission = false,
                .blob_max_size = max_size};
    }

    static TPaimonFileDesc blob_split(int64_t anchor_first_row_id, int64_t anchor_row_count,
                                      std::vector<TPaimonBlobFileDesc> files) {
        TPaimonFileDesc split;
        split.__set_anchor_first_row_id(anchor_first_row_id);
        split.__set_anchor_row_count(anchor_row_count);
        split.__set_blob_files(std::move(files));
        return split;
    }

    static Block output_block(const std::vector<ColumnDefinition>& columns, size_t rows) {
        Block block;
        for (const auto& column : columns) {
            auto data = column.type->create_column();
            data->insert_many_defaults(rows);
            block.insert({std::move(data), column.type, column.name});
        }
        return block;
    }

    static void expect_inline(const IColumn& column, size_t row,
                              const std::optional<std::string>& expected) {
        const auto value = column[row];
        if (!expected) {
            EXPECT_TRUE(value.is_null()) << "row " << row;
            return;
        }
        ASSERT_FALSE(value.is_null()) << "row " << row;
        const auto& fields = value.get<TYPE_FILE>();
        ASSERT_EQ(fields.size(), DataTypeFile::FIELD_COUNT);
        for (size_t child = 0; child < DataTypeFile::INLINE_FIELD_INDEX; ++child) {
            EXPECT_TRUE(fields[child].is_null()) << "row " << row << " child " << child;
        }
        ASSERT_FALSE(fields[DataTypeFile::INLINE_FIELD_INDEX].is_null());
        const auto& bytes = fields[DataTypeFile::INLINE_FIELD_INDEX].get<TYPE_VARBINARY>();
        EXPECT_EQ(std::string(bytes.data(), bytes.size()), *expected) << "row " << row;
    }

    static void expect_descriptor(const IColumn& column, size_t row, const std::string& path,
                                  int64_t offset, int64_t size) {
        const auto value = column[row];
        ASSERT_FALSE(value.is_null()) << "row " << row;
        const auto& fields = value.get<TYPE_FILE>();
        ASSERT_EQ(fields.size(), DataTypeFile::FIELD_COUNT);
        ASSERT_FALSE(fields[DataTypeFile::URI_FIELD_INDEX].is_null());
        ASSERT_FALSE(fields[DataTypeFile::OFFSET_FIELD_INDEX].is_null());
        ASSERT_FALSE(fields[DataTypeFile::SIZE_FIELD_INDEX].is_null());
        EXPECT_EQ(fields[DataTypeFile::URI_FIELD_INDEX].get<TYPE_STRING>(), path);
        EXPECT_EQ(fields[DataTypeFile::OFFSET_FIELD_INDEX].get<TYPE_BIGINT>(), offset);
        EXPECT_EQ(fields[DataTypeFile::SIZE_FIELD_INDEX].get<TYPE_BIGINT>(), size);
        EXPECT_TRUE(fields[DataTypeFile::INLINE_FIELD_INDEX].is_null());
        EXPECT_TRUE(fields[DataTypeFile::CONTENT_TYPE_FIELD_INDEX].is_null());
        EXPECT_TRUE(fields[DataTypeFile::CHECKSUM_FIELD_INDEX].is_null());
    }

    doris::test::ScopedTempDirectory _temp_directory {"paimon_blob"};
    io::FileSystemProperties _system_properties;
    io::IOContext _io_context;
    std::string _fs_name;
};

} // namespace doris::format::blob_test
