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

#include <gen_cpp/PaloInternalService_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stdint.h>
#include <unistd.h>

#include <cmath>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "olap/options.h"
#include "runtime/block_spill_manager.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/bitmap_value.h"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/block_spill_reader.h"
#include "vec/core/block_spill_writer.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris {
class RuntimeProfile;

static const uint32_t MAX_PATH_LEN = 1024;

static const std::string TMP_DATA_DIR = "block_spill_test";

std::string test_data_dir;
std::shared_ptr<BlockSpillManager> block_spill_manager;

class TestBlockSpill : public testing::Test {
public:
    TestBlockSpill() : runtime_state_(TQueryGlobals()) {
        profile_ = runtime_state_.runtime_profile();
    }
    static void SetUpTestSuite() {
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        test_data_dir = std::string(buffer) + "/" + TMP_DATA_DIR;
        std::cout << "test data dir: " << test_data_dir << "\n";
        static_cast<void>(
                io::global_local_filesystem()->delete_and_create_directory(test_data_dir));

        std::vector<StorePath> paths;
        paths.emplace_back(test_data_dir, -1);
        block_spill_manager = std::make_shared<BlockSpillManager>(paths);
        static_cast<void>(block_spill_manager->init());
    }

    static void TearDownTestSuite() {
        static_cast<void>(io::global_local_filesystem()->delete_directory(test_data_dir));
    }

protected:
    void SetUp() {
        env_ = ExecEnv::GetInstance();
        env_->_block_spill_mgr = block_spill_manager.get();
    }

    void TearDown() {}

private:
    ExecEnv* env_ = nullptr;
    RuntimeState runtime_state_;
    RuntimeProfile* profile_;
};

TEST_F(TestBlockSpill, TestInt) {
    int batch_size = 3; // rows in a block
    int batch_num = 3;
    int total_rows = batch_size * batch_num;
    auto col1 = vectorized::ColumnVector<int>::create();
    auto col2 = vectorized::ColumnVector<int>::create();
    auto& data1 = col1->get_data();
    for (int i = 0; i < total_rows; ++i) {
        data1.push_back(i);
    }
    auto& data2 = col2->get_data();
    data2.push_back(0);

    vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt32>());
    vectorized::ColumnWithTypeAndName type_and_name1(col1->get_ptr(), data_type,
                                                     "spill_block_test_int");
    vectorized::Block block1({type_and_name1});

    vectorized::ColumnWithTypeAndName type_and_name2(col2->get_ptr(), data_type,
                                                     "spill_block_test_int");
    vectorized::Block block2({type_and_name2});

    vectorized::BlockSpillWriterUPtr spill_block_writer;
    static_cast<void>(block_spill_manager->get_writer(batch_size, spill_block_writer, profile_));
    static_cast<void>(spill_block_writer->write(block1));
    static_cast<void>(spill_block_writer->write(block2));
    static_cast<void>(spill_block_writer->close());

    vectorized::BlockSpillReaderUPtr spill_block_reader;
    static_cast<void>(block_spill_manager->get_reader(spill_block_writer->get_id(),
                                                      spill_block_reader, profile_));

    vectorized::Block block_read;
    bool eos = false;

    for (int i = 0; i < batch_num; ++i) {
        static_cast<void>(spill_block_reader->read(&block_read, &eos));
        EXPECT_EQ(block_read.rows(), batch_size);
        auto column = block_read.get_by_position(0).column;
        auto* real_column = (vectorized::ColumnVector<int>*)column.get();
        for (size_t j = 0; j < batch_size; ++j) {
            EXPECT_EQ(real_column->get_int(j), j + i * batch_size);
        }
    }

    static_cast<void>(spill_block_reader->read(&block_read, &eos));
    static_cast<void>(spill_block_reader->close());

    EXPECT_EQ(block_read.rows(), 1);
    auto column = block_read.get_by_position(0).column;
    auto* real_column = (vectorized::ColumnVector<int>*)column.get();
    EXPECT_EQ(real_column->get_int(0), 0);
}

TEST_F(TestBlockSpill, TestIntNullable) {
    int batch_size = 3; // rows in a block
    int batch_num = 3;
    int total_rows = batch_size * batch_num + 1;
    auto vec = vectorized::ColumnVector<int>::create();
    auto nullable_vec = vectorized::make_nullable(std::move(vec));
    auto* raw_nullable_vec = (vectorized::ColumnNullable*)nullable_vec.get();
    for (int i = 0; i < total_rows; ++i) {
        if ((i + 1) % batch_size == 0) {
            raw_nullable_vec->insert_data(nullptr, 0);
        } else {
            raw_nullable_vec->insert_data((const char*)&i, 4);
        }
    }
    auto data_type = vectorized::make_nullable(std::make_shared<vectorized::DataTypeInt32>());
    vectorized::ColumnWithTypeAndName type_and_name(nullable_vec->get_ptr(), data_type,
                                                    "spill_block_test_int_nullable");
    vectorized::Block block({type_and_name});

    vectorized::BlockSpillWriterUPtr spill_block_writer;
    static_cast<void>(block_spill_manager->get_writer(batch_size, spill_block_writer, profile_));
    static_cast<void>(spill_block_writer->write(block));
    static_cast<void>(spill_block_writer->close());

    vectorized::BlockSpillReaderUPtr spill_block_reader;
    static_cast<void>(block_spill_manager->get_reader(spill_block_writer->get_id(),
                                                      spill_block_reader, profile_));

    vectorized::Block block_read;
    bool eos = false;

    for (int i = 0; i < batch_num; ++i) {
        static_cast<void>(spill_block_reader->read(&block_read, &eos));

        EXPECT_EQ(block_read.rows(), batch_size);
        auto column = block_read.get_by_position(0).column;
        auto* real_column = (vectorized::ColumnNullable*)column.get();
        const auto& int_column =
                (const vectorized::ColumnVector<int>&)(real_column->get_nested_column());
        for (size_t j = 0; j < batch_size; ++j) {
            if ((j + 1) % batch_size == 0) {
                ASSERT_TRUE(real_column->is_null_at(j));
            } else {
                EXPECT_EQ(int_column.get_int(j), j + i * batch_size);
            }
        }
    }

    static_cast<void>(spill_block_reader->read(&block_read, &eos));
    static_cast<void>(spill_block_reader->close());

    EXPECT_EQ(block_read.rows(), 1);
    auto column = block_read.get_by_position(0).column;
    auto* real_column = (vectorized::ColumnNullable*)column.get();
    const auto& int_column =
            (const vectorized::ColumnVector<int>&)(real_column->get_nested_column());
    EXPECT_EQ(int_column.get_int(0), batch_size * 3);
}
TEST_F(TestBlockSpill, TestString) {
    int batch_size = 3; // rows in a block
    int batch_num = 3;
    int total_rows = batch_size * batch_num + 1;
    auto strcol = vectorized::ColumnString::create();
    for (int i = 0; i < total_rows; ++i) {
        std::string is = std::to_string(i);
        strcol->insert_data(is.c_str(), is.size());
    }
    vectorized::DataTypePtr string_type(std::make_shared<vectorized::DataTypeString>());
    vectorized::ColumnWithTypeAndName test_string(strcol->get_ptr(), string_type,
                                                  "spill_block_test_string");
    vectorized::Block block({test_string});

    vectorized::BlockSpillWriterUPtr spill_block_writer;
    static_cast<void>(block_spill_manager->get_writer(batch_size, spill_block_writer, profile_));
    Status st = spill_block_writer->write(block);
    static_cast<void>(spill_block_writer->close());
    EXPECT_TRUE(st.ok());

    vectorized::BlockSpillReaderUPtr spill_block_reader;
    static_cast<void>(block_spill_manager->get_reader(spill_block_writer->get_id(),
                                                      spill_block_reader, profile_));

    vectorized::Block block_read;
    bool eos = false;

    for (int i = 0; i < batch_num; ++i) {
        st = spill_block_reader->read(&block_read, &eos);
        EXPECT_TRUE(st.ok());

        EXPECT_EQ(block_read.rows(), batch_size);
        auto column = block_read.get_by_position(0).column;
        auto* real_column = (vectorized::ColumnString*)column.get();
        for (size_t j = 0; j < batch_size; ++j) {
            EXPECT_EQ(real_column->get_data_at(j), StringRef(std::to_string(j + i * batch_size)));
        }
    }

    static_cast<void>(spill_block_reader->read(&block_read, &eos));
    static_cast<void>(spill_block_reader->close());

    EXPECT_EQ(block_read.rows(), 1);
    auto column = block_read.get_by_position(0).column;
    auto* real_column = (vectorized::ColumnString*)column.get();
    EXPECT_EQ(real_column->get_data_at(0), StringRef(std::to_string(batch_size * 3)));
}
TEST_F(TestBlockSpill, TestStringNullable) {
    int batch_size = 3; // rows in a block
    int batch_num = 3;
    int total_rows = batch_size * batch_num + 1;
    auto strcol = vectorized::ColumnString::create();
    auto nullable_vec = vectorized::make_nullable(std::move(strcol));
    auto* raw_nullable_vec = (vectorized::ColumnNullable*)nullable_vec.get();
    for (int i = 0; i < total_rows; ++i) {
        if ((i + 1) % batch_size == 0) {
            raw_nullable_vec->insert_data(nullptr, 0);
        } else {
            std::string is = std::to_string(i);
            raw_nullable_vec->insert_data(is.c_str(), is.size());
        }
    }
    auto data_type = vectorized::make_nullable(std::make_shared<vectorized::DataTypeString>());
    vectorized::ColumnWithTypeAndName type_and_name(nullable_vec->get_ptr(), data_type,
                                                    "spill_block_test_string_nullable");
    vectorized::Block block({type_and_name});

    vectorized::BlockSpillWriterUPtr spill_block_writer;
    static_cast<void>(block_spill_manager->get_writer(batch_size, spill_block_writer, profile_));
    Status st = spill_block_writer->write(block);
    static_cast<void>(spill_block_writer->close());
    EXPECT_TRUE(st.ok());

    vectorized::BlockSpillReaderUPtr spill_block_reader;
    static_cast<void>(block_spill_manager->get_reader(spill_block_writer->get_id(),
                                                      spill_block_reader, profile_));

    vectorized::Block block_read;
    bool eos = false;

    for (int i = 0; i < batch_num; ++i) {
        st = spill_block_reader->read(&block_read, &eos);
        EXPECT_TRUE(st.ok());

        EXPECT_EQ(block_read.rows(), batch_size);
        auto column = block_read.get_by_position(0).column;
        auto* real_column = (vectorized::ColumnNullable*)column.get();
        const auto& string_column =
                (const vectorized::ColumnString&)(real_column->get_nested_column());
        for (size_t j = 0; j < batch_size; ++j) {
            if ((j + 1) % batch_size == 0) {
                ASSERT_TRUE(real_column->is_null_at(j));
            } else {
                EXPECT_EQ(string_column.get_data_at(j),
                          StringRef(std::to_string(j + i * batch_size)));
            }
        }
    }

    st = spill_block_reader->read(&block_read, &eos);
    static_cast<void>(spill_block_reader->close());
    EXPECT_TRUE(st.ok());

    EXPECT_EQ(block_read.rows(), 1);
    auto column = block_read.get_by_position(0).column;
    auto* real_column = (vectorized::ColumnNullable*)column.get();
    const auto& string_column = (const vectorized::ColumnString&)(real_column->get_nested_column());
    EXPECT_EQ(string_column.get_data_at(0), StringRef(std::to_string(batch_size * 3)));
}
TEST_F(TestBlockSpill, TestDecimal) {
    int batch_size = 3; // rows in a block
    int batch_num = 3;
    int total_rows = batch_size * batch_num + 1;

    vectorized::DataTypePtr decimal_data_type(doris::vectorized::create_decimal(27, 9, true));
    auto decimal_column = decimal_data_type->create_column();
    auto& decimal_data = ((vectorized::ColumnDecimal<vectorized::Decimal<vectorized::Int128>>*)
                                  decimal_column.get())
                                 ->get_data();
    for (int i = 0; i < total_rows; ++i) {
        __int128_t value = i * pow(10, 9) + i * pow(10, 8);
        decimal_data.push_back(value);
    }
    vectorized::ColumnWithTypeAndName test_decimal(decimal_column->get_ptr(), decimal_data_type,
                                                   "spill_block_test_decimal");
    vectorized::Block block({test_decimal});

    vectorized::BlockSpillWriterUPtr spill_block_writer;
    static_cast<void>(block_spill_manager->get_writer(batch_size, spill_block_writer, profile_));
    auto st = spill_block_writer->write(block);
    static_cast<void>(spill_block_writer->close());
    EXPECT_TRUE(st.ok());

    vectorized::BlockSpillReaderUPtr spill_block_reader;
    static_cast<void>(block_spill_manager->get_reader(spill_block_writer->get_id(),
                                                      spill_block_reader, profile_));

    vectorized::Block block_read;
    bool eos = false;

    for (int i = 0; i < batch_num; ++i) {
        st = spill_block_reader->read(&block_read, &eos);
        EXPECT_TRUE(st.ok());

        EXPECT_EQ(block_read.rows(), batch_size);
        auto column = block_read.get_by_position(0).column;
        auto* real_column =
                (vectorized::ColumnDecimal<vectorized::Decimal<vectorized::Int128>>*)column.get();
        for (size_t j = 0; j < batch_size; ++j) {
            __int128_t value = (j + i * batch_size) * (pow(10, 9) + pow(10, 8));
            EXPECT_EQ(real_column->get_element(j).value, value);
        }
    }

    st = spill_block_reader->read(&block_read, &eos);
    static_cast<void>(spill_block_reader->close());
    EXPECT_TRUE(st.ok());

    EXPECT_EQ(block_read.rows(), 1);
    auto column = block_read.get_by_position(0).column;
    auto* real_column =
            (vectorized::ColumnDecimal<vectorized::Decimal<vectorized::Int128>>*)column.get();
    EXPECT_EQ(real_column->get_element(0).value, batch_size * 3 * (pow(10, 9) + pow(10, 8)));
}
TEST_F(TestBlockSpill, TestDecimalNullable) {
    int batch_size = 3; // rows in a block
    int batch_num = 3;
    int total_rows = batch_size * batch_num + 1;

    vectorized::DataTypePtr decimal_data_type(doris::vectorized::create_decimal(27, 9, true));
    auto base_col = vectorized::make_nullable(decimal_data_type->create_column());
    auto* nullable_col = (vectorized::ColumnNullable*)base_col.get();
    for (int i = 0; i < total_rows; ++i) {
        if ((i + 1) % batch_size == 0) {
            nullable_col->insert_data(nullptr, 0);
        } else {
            __int128_t value = i * pow(10, 9) + i * pow(10, 8);
            nullable_col->insert_data((const char*)&value, sizeof(value));
        }
    }
    auto data_type = vectorized::make_nullable(decimal_data_type);
    vectorized::ColumnWithTypeAndName type_and_name(nullable_col->get_ptr(), data_type,
                                                    "spill_block_test_decimal_nullable");
    vectorized::Block block({type_and_name});

    vectorized::BlockSpillWriterUPtr spill_block_writer;
    static_cast<void>(block_spill_manager->get_writer(batch_size, spill_block_writer, profile_));
    auto st = spill_block_writer->write(block);
    static_cast<void>(spill_block_writer->close());
    EXPECT_TRUE(st.ok());

    vectorized::BlockSpillReaderUPtr spill_block_reader;
    static_cast<void>(block_spill_manager->get_reader(spill_block_writer->get_id(),
                                                      spill_block_reader, profile_));

    vectorized::Block block_read;
    bool eos = false;

    for (int i = 0; i < batch_num; ++i) {
        st = spill_block_reader->read(&block_read, &eos);
        EXPECT_TRUE(st.ok());

        EXPECT_EQ(block_read.rows(), batch_size);
        auto column = block_read.get_by_position(0).column;
        auto* real_column = (vectorized::ColumnNullable*)column.get();
        const auto& decimal_col = (vectorized::ColumnDecimal<vectorized::Decimal<
                                           vectorized::Int128>>&)(real_column->get_nested_column());
        for (size_t j = 0; j < batch_size; ++j) {
            if ((j + 1) % batch_size == 0) {
                ASSERT_TRUE(real_column->is_null_at(j));
            } else {
                __int128_t value = (j + i * batch_size) * (pow(10, 9) + pow(10, 8));
                EXPECT_EQ(decimal_col.get_element(j).value, value);
            }
        }
    }

    st = spill_block_reader->read(&block_read, &eos);
    static_cast<void>(spill_block_reader->close());
    EXPECT_TRUE(st.ok());

    EXPECT_EQ(block_read.rows(), 1);
    auto column = block_read.get_by_position(0).column;
    auto* real_column = (vectorized::ColumnNullable*)column.get();
    const auto& decimal_col =
            (vectorized::ColumnDecimal<
                    vectorized::Decimal<vectorized::Int128>>&)(real_column->get_nested_column());
    EXPECT_EQ(decimal_col.get_element(0).value, batch_size * 3 * (pow(10, 9) + pow(10, 8)));
}
std::string convert_bitmap_to_string(BitmapValue& bitmap);
TEST_F(TestBlockSpill, TestBitmap) {
    int batch_size = 3; // rows in a block
    int batch_num = 3;
    int total_rows = batch_size * batch_num + 1;

    vectorized::DataTypePtr bitmap_data_type(std::make_shared<vectorized::DataTypeBitMap>());
    auto bitmap_column = bitmap_data_type->create_column();
    std::vector<BitmapValue>& container =
            ((vectorized::ColumnComplexType<BitmapValue>*)bitmap_column.get())->get_data();
    std::vector<std::string> expected_bitmap_str;
    for (int i = 0; i < total_rows; ++i) {
        BitmapValue bv;
        for (int j = 0; j <= i; ++j) {
            bv.add(j);
        }
        expected_bitmap_str.emplace_back(convert_bitmap_to_string(bv));
        container.push_back(bv);
    }
    vectorized::ColumnWithTypeAndName type_and_name(bitmap_column->get_ptr(), bitmap_data_type,
                                                    "spill_block_test_bitmap");
    vectorized::Block block({type_and_name});

    vectorized::BlockSpillWriterUPtr spill_block_writer;
    static_cast<void>(block_spill_manager->get_writer(batch_size, spill_block_writer, profile_));
    auto st = spill_block_writer->write(block);
    static_cast<void>(spill_block_writer->close());
    EXPECT_TRUE(st.ok());

    vectorized::BlockSpillReaderUPtr spill_block_reader;
    static_cast<void>(block_spill_manager->get_reader(spill_block_writer->get_id(),
                                                      spill_block_reader, profile_));

    vectorized::Block block_read;
    bool eos = false;

    for (int i = 0; i < batch_num; ++i) {
        st = spill_block_reader->read(&block_read, &eos);
        EXPECT_TRUE(st.ok());

        EXPECT_EQ(block_read.rows(), batch_size);
        auto column = block_read.get_by_position(0).column;
        auto* real_column = (vectorized::ColumnComplexType<BitmapValue>*)column.get();
        for (size_t j = 0; j < batch_size; ++j) {
            auto bitmap_str = convert_bitmap_to_string(real_column->get_element(j));
            EXPECT_EQ(bitmap_str, expected_bitmap_str[j + i * batch_size]);
        }
    }

    st = spill_block_reader->read(&block_read, &eos);
    static_cast<void>(spill_block_reader->close());
    EXPECT_TRUE(st.ok());

    EXPECT_EQ(block_read.rows(), 1);
    auto column = block_read.get_by_position(0).column;
    auto* real_column = (vectorized::ColumnComplexType<BitmapValue>*)column.get();
    auto bitmap_str = convert_bitmap_to_string(real_column->get_element(0));
    EXPECT_EQ(bitmap_str, expected_bitmap_str[3 * batch_size]);
}
} // namespace doris
