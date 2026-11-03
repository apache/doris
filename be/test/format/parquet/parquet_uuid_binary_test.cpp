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

#include <cstdlib>

#include "core/column/column_array.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_varbinary.h"
#include "format/parquet/vparquet_reader.h"
#include "io/fs/file_meta_cache.h"
#include "io/fs/local_file_system.h"
#include "util/timezone_utils.h"

namespace doris {

TEST(ParquetUuidBinaryTest, LegacyReaderPreservesNestedBytesAcrossBatches) {
    // Timestamp-semantics negotiation can force V2 even with its session flag disabled.
    // Exercise the legacy reader directly to keep its nested BINARY mapping covered.
    const char* source_root = std::getenv("ROOT");
    ASSERT_NE(source_root, nullptr);
    const std::vector<std::string> bytes = {std::string(16, '\0'),
                                            std::string(16, '\0'),
                                            std::string(15, '\0') + std::string(1, '\1'),
                                            std::string(1, '\x7f') + std::string(15, '\xff'),
                                            std::string(1, '\x80') + std::string(15, '\0'),
                                            std::string(16, '\xff')};
    auto check_value = [&](const IColumn& column, size_t row, size_t id) {
        EXPECT_EQ(column.is_null_at(row), id % 6 == 0);
        if (id % 6 != 0) {
            EXPECT_EQ(assert_cast<const ColumnNullable&>(column)
                              .get_nested_column()
                              .get_data_at(row)
                              .to_string(),
                      bytes[id % 6]);
        }
    };
    const auto binary = make_nullable(std::make_shared<DataTypeVarbinary>());
    const DataTypes types = {
            make_nullable(std::make_shared<DataTypeInt32>()), binary,
            make_nullable(std::make_shared<DataTypeArray>(binary)),
            make_nullable(std::make_shared<DataTypeStruct>(DataTypes {binary}, Strings {"k"}))};
    const std::vector<std::string> names = {"id", "u", "a", "s"};
    std::unordered_map<std::string, uint32_t> indexes = {{"id", 0}, {"u", 1}, {"a", 2}, {"s", 3}};
    cctz::time_zone zone;
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("UTC", zone));
    FileMetaCache cache(1024);
    for (const std::string encoding : {"plain", "dictionary"}) {
        io::FileReaderSPtr file;
        ASSERT_TRUE(
                io::global_local_filesystem()
                        ->open_file(
                                std::string(source_root) +
                                        "/regression-test/data/datatype_p0/uuid/test_data/uuid_" +
                                        encoding + ".parquet",
                                &file)
                        .ok());
        TFileScanRangeParams params;
        params.__set_enable_mapping_varbinary(true);
        params.__set_file_type(TFileType::FILE_LOCAL);
        TFileRangeDesc range;
        range.__set_start_offset(0);
        range.__set_size(file->size());
        range.__set_file_size(file->size());
        range.__set_path(file->path().native());
        ParquetReader reader(nullptr, params, range, 37, &zone, nullptr, nullptr, &cache);
        phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>> predicates;
        auto status = reader.init_reader(names, &indexes, {}, predicates, nullptr, nullptr, nullptr,
                                         nullptr, nullptr);
        ASSERT_TRUE(status.ok()) << status;
        std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>> partitions;
        std::unordered_map<std::string, VExprContextSPtr> missing;
        ASSERT_TRUE(reader.set_fill_columns(partitions, missing).ok());
        bool eof = false;
        size_t total = 0;
        while (!eof) {
            Block block;
            for (size_t i = 0; i < names.size(); ++i) {
                block.insert({types[i]->create_column(), types[i], names[i]});
            }
            size_t rows = 0;
            status = reader.get_next_block(&block, &rows, &eof);
            ASSERT_TRUE(status.ok()) << status;
            for (size_t row = 0; row < rows; ++row) {
                const size_t id = total + row;
                check_value(*block.get_by_position(1).column, row, id);
                const auto& array_nullable =
                        assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
                EXPECT_EQ(array_nullable.is_null_at(row), id % 7 == 0);
                const auto& array =
                        assert_cast<const ColumnArray&>(array_nullable.get_nested_column());
                const auto begin = array.get_offsets()[static_cast<ssize_t>(row) - 1];
                const auto end = array.get_offsets()[row];
                if (id % 7 > 1) {
                    ASSERT_EQ(end - begin, 2);
                    check_value(array.get_data(), begin, id);
                    EXPECT_TRUE(array.get_data().is_null_at(begin + 1));
                } else {
                    EXPECT_EQ(end, begin);
                }
                const auto& structure =
                        assert_cast<const ColumnNullable&>(*block.get_by_position(3).column);
                EXPECT_EQ(structure.is_null_at(row), id % 5 == 0);
                if (id % 5 != 0) {
                    const auto& fields =
                            assert_cast<const ColumnStruct&>(structure.get_nested_column());
                    check_value(fields.get_column(0), row, id);
                }
            }
            total += rows;
        }
        EXPECT_EQ(total, 4097);
    }
}

} // namespace doris
