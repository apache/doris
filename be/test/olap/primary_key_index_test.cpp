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

#include "olap/primary_key_index.h"

#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <algorithm>
#include <string>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "gutil/stringprintf.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "olap/types.h"
#include "vec/columns/column.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {
using namespace ErrorCode;

class PrimaryKeyIndexTest : public testing::Test {
public:
    void SetUp() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_and_create_directory(kTestDir).ok());
    }
    void TearDown() override {
        //EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

private:
    const std::string kTestDir = "./ut_dir/primary_key_index_test";
};

TEST_F(PrimaryKeyIndexTest, builder) {
    std::string filename = kTestDir + "/builder";
    io::FileWriterPtr file_writer;
    auto fs = io::global_local_filesystem();
    EXPECT_TRUE(fs->create_file(filename, &file_writer).ok());

    PrimaryKeyIndexBuilder builder(file_writer.get(), 0);
    static_cast<void>(builder.init());
    size_t num_rows = 0;
    std::vector<std::string> keys;
    for (int i = 1000; i < 10000; i += 2) {
        keys.push_back(std::to_string(i));
        static_cast<void>(builder.add_item(std::to_string(i)));
        num_rows++;
    }
    EXPECT_EQ("1000", builder.min_key().to_string());
    EXPECT_EQ("9998", builder.max_key().to_string());
    segment_v2::PrimaryKeyIndexMetaPB index_meta;
    EXPECT_TRUE(builder.finalize(&index_meta));
    EXPECT_EQ(builder.disk_size(), file_writer->bytes_appended());
    EXPECT_TRUE(file_writer->close().ok());
    EXPECT_EQ(num_rows, builder.num_rows());

    PrimaryKeyIndexReader index_reader;
    io::FileReaderSPtr file_reader;
    EXPECT_TRUE(fs->open_file(filename, &file_reader).ok());
    EXPECT_TRUE(index_reader.parse_index(file_reader, index_meta).ok());
    EXPECT_TRUE(index_reader.parse_bf(file_reader, index_meta).ok());
    EXPECT_EQ(num_rows, index_reader.num_rows());

    std::unique_ptr<segment_v2::IndexedColumnIterator> index_iterator;
    EXPECT_TRUE(index_reader.new_iterator(&index_iterator).ok());
    bool exact_match = false;
    uint32_t row_id;
    for (size_t i = 0; i < keys.size(); i++) {
        bool exists = index_reader.check_present(keys[i]);
        EXPECT_TRUE(exists);
        auto status = index_iterator->seek_at_or_after(&keys[i], &exact_match);
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(exact_match);
        row_id = index_iterator->get_current_ordinal();
        EXPECT_EQ(i, row_id);
    }
    // find a non-existing key "8701"
    {
        string key("8701");
        Slice slice(key);
        bool exists = index_reader.check_present(slice);
        EXPECT_FALSE(exists);
        auto status = index_iterator->seek_at_or_after(&slice, &exact_match);
        EXPECT_TRUE(status.ok());
        EXPECT_FALSE(exact_match);
        row_id = index_iterator->get_current_ordinal();
        EXPECT_EQ(3851, row_id);
    }

    // find prefix "87"
    {
        string key("87");
        Slice slice(key);
        bool exists = index_reader.check_present(slice);
        EXPECT_FALSE(exists);
        auto status = index_iterator->seek_at_or_after(&slice, &exact_match);
        EXPECT_TRUE(status.ok());
        EXPECT_FALSE(exact_match);
        row_id = index_iterator->get_current_ordinal();
        EXPECT_EQ(3850, row_id);
    }

    // find prefix "9999"
    {
        string key("9999");
        Slice slice(key);
        bool exists = index_reader.check_present(slice);
        EXPECT_FALSE(exists);
        auto status = index_iterator->seek_at_or_after(&slice, &exact_match);
        EXPECT_FALSE(exact_match);
        EXPECT_TRUE(status.is<ErrorCode::ENTRY_NOT_FOUND>());
    }

    // read all key
    {
        int32_t remaining = num_rows;
        std::string last_key;
        int num_batch = 0;
        int batch_size = 1024;
        while (remaining > 0) {
            std::unique_ptr<segment_v2::IndexedColumnIterator> iter;
            EXPECT_TRUE(index_reader.new_iterator(&iter).ok());

            size_t num_to_read = std::min(batch_size, remaining);
            auto index_type = vectorized::DataTypeFactory::instance().create_data_type(
                    index_reader.type_info()->type(), 1, 0);
            auto index_column = index_type->create_column();
            Slice last_key_slice(last_key);
            EXPECT_TRUE(iter->seek_at_or_after(&last_key_slice, &exact_match).ok());

            size_t num_read = num_to_read;
            EXPECT_TRUE(iter->next_batch(&num_read, index_column).ok());
            EXPECT_EQ(num_to_read, num_read);
            last_key = index_column->get_data_at(num_read - 1).to_string();
            // exclude last_key, last_key will be read in next batch.
            if (num_read == batch_size && num_read != remaining) {
                num_read -= 1;
            }
            for (size_t i = 0; i < num_read; i++) {
                Slice key =
                        Slice(index_column->get_data_at(i).data, index_column->get_data_at(i).size);
                DCHECK_EQ(keys[i + (batch_size - 1) * num_batch], key.to_string());
            }
            num_batch++;
            remaining -= num_read;
        }
    }
}

TEST_F(PrimaryKeyIndexTest, single_page) {
    std::string filename = kTestDir + "/builder";
    io::FileWriterPtr file_writer;
    auto fs = io::global_local_filesystem();
    EXPECT_TRUE(fs->create_file(filename, &file_writer).ok());

    PrimaryKeyIndexBuilder builder(file_writer.get(), 0);
    static_cast<void>(builder.init());
    size_t num_rows = 0;
    std::vector<std::string> keys;
    for (int i = 1000; i < 10000; i += 2) {
        keys.push_back(std::to_string(i));
        static_cast<void>(builder.add_item(std::to_string(i)));
        num_rows++;
    }
    EXPECT_EQ("1000", builder.min_key().to_string());
    EXPECT_EQ("9998", builder.max_key().to_string());
    segment_v2::PrimaryKeyIndexMetaPB index_meta;
    EXPECT_TRUE(builder.finalize(&index_meta));
    EXPECT_EQ(builder.disk_size(), file_writer->bytes_appended());
    EXPECT_TRUE(file_writer->close().ok());
    EXPECT_EQ(num_rows, builder.num_rows());

    PrimaryKeyIndexReader index_reader;
    io::FileReaderSPtr file_reader;
    EXPECT_TRUE(fs->open_file(filename, &file_reader).ok());
    EXPECT_TRUE(index_reader.parse_index(file_reader, index_meta).ok());
    EXPECT_TRUE(index_reader.parse_bf(file_reader, index_meta).ok());
    EXPECT_EQ(num_rows, index_reader.num_rows());

    std::unique_ptr<segment_v2::IndexedColumnIterator> index_iterator;
    EXPECT_TRUE(index_reader.new_iterator(&index_iterator).ok());
    bool exact_match = false;
    uint32_t row_id;
    for (size_t i = 0; i < keys.size(); i++) {
        bool exists = index_reader.check_present(keys[i]);
        EXPECT_TRUE(exists);
        auto status = index_iterator->seek_at_or_after(&keys[i], &exact_match);
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(exact_match);
        row_id = index_iterator->get_current_ordinal();
        EXPECT_EQ(i, row_id);
    }
    // find a non-existing key "8701"
    {
        string key("8701");
        Slice slice(key);
        bool exists = index_reader.check_present(slice);
        EXPECT_FALSE(exists);
        auto status = index_iterator->seek_at_or_after(&slice, &exact_match);
        EXPECT_TRUE(status.ok());
        EXPECT_FALSE(exact_match);
        row_id = index_iterator->get_current_ordinal();
        EXPECT_EQ(3851, row_id);
    }

    // find prefix "87"
    {
        string key("87");
        Slice slice(key);
        bool exists = index_reader.check_present(slice);
        EXPECT_FALSE(exists);
        auto status = index_iterator->seek_at_or_after(&slice, &exact_match);
        EXPECT_TRUE(status.ok());
        EXPECT_FALSE(exact_match);
        row_id = index_iterator->get_current_ordinal();
        EXPECT_EQ(3850, row_id);
    }

    // find prefix "9999"
    {
        string key("9999");
        Slice slice(key);
        bool exists = index_reader.check_present(slice);
        EXPECT_FALSE(exists);
        auto status = index_iterator->seek_at_or_after(&slice, &exact_match);
        EXPECT_FALSE(exact_match);
        EXPECT_TRUE(status.is<ErrorCode::ENTRY_NOT_FOUND>());
    }

    // read all key
    {
        int32_t remaining = num_rows;
        std::string last_key;
        int num_batch = 0;
        int batch_size = 1024;
        while (remaining > 0) {
            std::unique_ptr<segment_v2::IndexedColumnIterator> iter;
            EXPECT_TRUE(index_reader.new_iterator(&iter).ok());

            size_t num_to_read = std::min(batch_size, remaining);
            auto index_type = vectorized::DataTypeFactory::instance().create_data_type(
                    index_reader.type_info()->type(), 1, 0);
            auto index_column = index_type->create_column();
            Slice last_key_slice(last_key);
            EXPECT_TRUE(iter->seek_at_or_after(&last_key_slice, &exact_match).ok());

            size_t num_read = num_to_read;
            EXPECT_TRUE(iter->next_batch(&num_read, index_column).ok());
            EXPECT_EQ(num_to_read, num_read);
            last_key = index_column->get_data_at(num_read - 1).to_string();
            // exclude last_key, last_key will be read in next batch.
            if (num_read == batch_size && num_read != remaining) {
                num_read -= 1;
            }
            for (size_t i = 0; i < num_read; i++) {
                Slice key =
                        Slice(index_column->get_data_at(i).data, index_column->get_data_at(i).size);
                DCHECK_EQ(keys[i + (batch_size - 1) * num_batch], key.to_string());
            }
            num_batch++;
            remaining -= num_read;
        }
    }
}

TEST_F(PrimaryKeyIndexTest, multiple_pages) {
    std::string filename = kTestDir + "/builder";
    io::FileWriterPtr file_writer;
    auto fs = io::global_local_filesystem();
    EXPECT_TRUE(fs->create_file(filename, &file_writer).ok());

    PrimaryKeyIndexBuilder builder(file_writer.get(), 0);
    static_cast<void>(builder.init());
    size_t num_rows = 0;
    std::vector<std::string> keys;
    for (int i = 1000; i < 10000; i += 2) {
        keys.push_back(std::to_string(i));
        static_cast<void>(builder.add_item(std::to_string(i)));
        num_rows++;
    }
    EXPECT_EQ("1000", builder.min_key().to_string());
    EXPECT_EQ("9998", builder.max_key().to_string());
    segment_v2::PrimaryKeyIndexMetaPB index_meta;
    EXPECT_TRUE(builder.finalize(&index_meta));
    EXPECT_EQ(builder.disk_size(), file_writer->bytes_appended());
    EXPECT_TRUE(file_writer->close().ok());
    EXPECT_EQ(num_rows, builder.num_rows());

    PrimaryKeyIndexReader index_reader;
    io::FileReaderSPtr file_reader;
    EXPECT_TRUE(fs->open_file(filename, &file_reader).ok());
    EXPECT_TRUE(index_reader.parse_index(file_reader, index_meta).ok());
    EXPECT_TRUE(index_reader.parse_bf(file_reader, index_meta).ok());
    EXPECT_EQ(num_rows, index_reader.num_rows());

    std::unique_ptr<segment_v2::IndexedColumnIterator> index_iterator;
    EXPECT_TRUE(index_reader.new_iterator(&index_iterator).ok());
    bool exact_match = false;
    uint32_t row_id;
    for (size_t i = 0; i < keys.size(); i++) {
        bool exists = index_reader.check_present(keys[i]);
        EXPECT_TRUE(exists);
        auto status = index_iterator->seek_at_or_after(&keys[i], &exact_match);
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(exact_match);
        row_id = index_iterator->get_current_ordinal();
        EXPECT_EQ(i, row_id);
    }
    // find a non-existing key "8701"
    {
        string key("8701");
        Slice slice(key);
        bool exists = index_reader.check_present(slice);
        EXPECT_FALSE(exists);
        auto status = index_iterator->seek_at_or_after(&slice, &exact_match);
        EXPECT_TRUE(status.ok());
        EXPECT_FALSE(exact_match);
        row_id = index_iterator->get_current_ordinal();
        EXPECT_EQ(3851, row_id);
    }

    // find prefix "87"
    {
        string key("87");
        Slice slice(key);
        bool exists = index_reader.check_present(slice);
        EXPECT_FALSE(exists);
        auto status = index_iterator->seek_at_or_after(&slice, &exact_match);
        EXPECT_TRUE(status.ok());
        EXPECT_FALSE(exact_match);
        row_id = index_iterator->get_current_ordinal();
        EXPECT_EQ(3850, row_id);
    }

    // find prefix "9999"
    {
        string key("9999");
        Slice slice(key);
        bool exists = index_reader.check_present(slice);
        EXPECT_FALSE(exists);
        auto status = index_iterator->seek_at_or_after(&slice, &exact_match);
        EXPECT_FALSE(exact_match);
        EXPECT_TRUE(status.is<ErrorCode::ENTRY_NOT_FOUND>());
    }

    // read all key
    {
        int32_t remaining = num_rows;
        std::string last_key;
        int num_batch = 0;
        int batch_size = 1024;
        while (remaining > 0) {
            std::unique_ptr<segment_v2::IndexedColumnIterator> iter;
            EXPECT_TRUE(index_reader.new_iterator(&iter).ok());

            size_t num_to_read = std::min(batch_size, remaining);
            auto index_type = vectorized::DataTypeFactory::instance().create_data_type(
                    index_reader.type_info()->type(), 1, 0);
            auto index_column = index_type->create_column();
            Slice last_key_slice(last_key);
            EXPECT_TRUE(iter->seek_at_or_after(&last_key_slice, &exact_match).ok());

            size_t num_read = num_to_read;
            EXPECT_TRUE(iter->next_batch(&num_read, index_column).ok());
            EXPECT_EQ(num_to_read, num_read);
            last_key = index_column->get_data_at(num_read - 1).to_string();
            // exclude last_key, last_key will be read in next batch.
            if (num_read == batch_size && num_read != remaining) {
                num_read -= 1;
            }
            for (size_t i = 0; i < num_read; i++) {
                Slice key =
                        Slice(index_column->get_data_at(i).data, index_column->get_data_at(i).size);
                DCHECK_EQ(keys[i + (batch_size - 1) * num_batch], key.to_string());
            }
            num_batch++;
            remaining -= num_read;
        }
    }
}
} // namespace doris
