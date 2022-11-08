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

#include "vec/columns/column_complex.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "agent/be_exec_version_manager.h"
#include "agent/heartbeat_server.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_bitmap.h"
namespace doris::vectorized {
TEST(ColumnComplexTest, BasicTest) {
    using ColumnSTLString = ColumnComplexType<std::string>;
    auto column = ColumnSTLString::create();
    EXPECT_EQ(column->size(), 0);
    std::string val0 = "";
    std::string val1 = "str-1";

    column->insert_data(reinterpret_cast<const char*>(&val0), sizeof(val0));
    column->insert_data(reinterpret_cast<const char*>(&val1), sizeof(val1));

    StringRef ref = column->get_data_at(0);
    EXPECT_EQ((*reinterpret_cast<const std::string*>(ref.data)), "");
    ref = column->get_data_at(1);
    EXPECT_EQ((*reinterpret_cast<const std::string*>(ref.data)), val1);
}

// Test the compile failed
TEST(ColumnComplexType, DataTypeBitmapTest) {
    std::make_shared<DataTypeBitMap>();
}

class ColumnBitmapTest : public testing::Test {
public:
    virtual void SetUp() override {}
    virtual void TearDown() override {}

    void check_bitmap_column(const IColumn& l, const IColumn& r) {
        ASSERT_EQ(l.size(), r.size());
        const auto& l_col = assert_cast<const ColumnBitmap&>(l);
        const auto& r_col = assert_cast<const ColumnBitmap&>(r);
        for (size_t i = 0; i < l_col.size(); ++i) {
            auto& l_bitmap = const_cast<BitmapValue&>(l_col.get_element(i));
            auto& r_bitmap = const_cast<BitmapValue&>(r_col.get_element(i));
            ASSERT_EQ(l_bitmap.xor_cardinality(r_bitmap), 0);
        }
    }

    void check_serialize_and_deserialize(MutableColumnPtr& col) {
        auto column = assert_cast<ColumnBitmap*>(col.get());
        auto size = _bitmap_type.get_uncompressed_serialized_bytes(
                *column, BeExecVersionManager::get_newest_version());
        std::unique_ptr<char[]> buf = std::make_unique<char[]>(size);
        auto result = _bitmap_type.serialize(*column, buf.get(),
                                             BeExecVersionManager::get_newest_version());
        ASSERT_EQ(result, buf.get() + size);

        auto column2 = _bitmap_type.create_column();
        _bitmap_type.deserialize(buf.get(), column2.get(),
                                 BeExecVersionManager::get_newest_version());
        check_bitmap_column(*column, *column2.get());
    }

private:
    DataTypeBitMap _bitmap_type;
};

TEST_F(ColumnBitmapTest, SerializeAndDeserialize) {
    auto column = _bitmap_type.create_column();

    // empty column
    check_serialize_and_deserialize(column);

    // bitmap with lots of rows
    const size_t row_size = 20000;
    auto& data = assert_cast<ColumnBitmap&>(*column.get()).get_data();
    data.resize(row_size);
    check_serialize_and_deserialize(column);

    // bitmap with values case 1
    data[0].add(10);
    data[0].add(1000000);
    check_serialize_and_deserialize(column);

    // bitmap with values case 2
    data[row_size - 1].add(33333);
    data[row_size - 1].add(0);
    check_serialize_and_deserialize(column);
}

} // namespace doris::vectorized
