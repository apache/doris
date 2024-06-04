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

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <boost/iterator/iterator_facade.hpp>
#include <memory>

#include "gtest/gtest_pred_impl.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "olap/bitmap_filter_predicate.h"
#include "olap/block_column_predicate.h"
#include "runtime/define_primitive_type.h"
#include "vec/columns/predicate_column.h"

namespace doris {

class BitmapFilterColumnPredicateTest : public testing::Test {
public:
    BitmapFilterColumnPredicateTest() = default;

    ~BitmapFilterColumnPredicateTest() = default;

    template <PrimitiveType type>
    BitmapFilterColumnPredicate<type> create_predicate(
            const std::shared_ptr<BitmapFilterFunc<type>>& filter) {
        return BitmapFilterColumnPredicate<type>(0, filter, 0);
    }

    const std::string kTestDir = "./ut_dir/bitmap_filter_column_predicate_test";
    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }
};

using FileSystemSPtr = std::shared_ptr<io::FileSystem>;

template <FieldType type>
void write_index_file(const std::string& filename, FileSystemSPtr fs, const void* values,
                      size_t value_count, size_t null_count, ColumnIndexMetaPB* meta);

template <FieldType type>
void get_bitmap_reader_iter(const std::string& file_name, const ColumnIndexMetaPB& meta,
                            BitmapIndexReader** reader, BitmapIndexIterator** iter);

TEST_F(BitmapFilterColumnPredicateTest, evaluate_and) {
    auto filter = std::make_shared<BitmapFilterFunc<PrimitiveType::TYPE_INT>>();

    auto predicate = create_predicate(filter);
    EXPECT_EQ(predicate.type(), PredicateType::BITMAP_FILTER);

    auto* min = WrapperField::create_by_type(FieldType::OLAP_FIELD_TYPE_INT);
    auto* max = WrapperField::create_by_type(FieldType::OLAP_FIELD_TYPE_INT);

    min->set_null();
    max->set_null();

    filter->set_not_in(true);
    EXPECT_TRUE(predicate.evaluate_and({min, max}));

    filter->set_not_in(false);
    EXPECT_FALSE(predicate.evaluate_and({min, max}));

    min->set_not_null();
    max->set_null();
    EXPECT_EQ(min->from_string("1"), Status::OK());
    EXPECT_EQ(max->from_string("101"), Status::OK());

    EXPECT_FALSE(predicate.evaluate_and({min, max}));

    max->set_not_null();

    BitmapValue bitmap_value;
    bitmap_value.add(0);
    bitmap_value.add(102);
    filter->insert(&bitmap_value);
    EXPECT_FALSE(predicate.evaluate_and({min, max}));

    bitmap_value.add(2);
    filter->insert(&bitmap_value);
    EXPECT_TRUE(predicate.evaluate_and({min, max}));

    delete min;
    delete max;
}

TEST_F(BitmapFilterColumnPredicateTest, evaluate) {
    auto filter = std::make_shared<BitmapFilterFunc<PrimitiveType::TYPE_INT>>();
    auto predicate = create_predicate(filter);
    EXPECT_EQ(predicate.type(), PredicateType::BITMAP_FILTER);

    EXPECT_EQ(predicate.evaluate(nullptr, 0, nullptr), Status::OK());
}

TEST_F(BitmapFilterColumnPredicateTest, evaluate_column) {
    auto filter = std::make_shared<BitmapFilterFunc<PrimitiveType::TYPE_INT>>();
    auto predicate = create_predicate(filter);
    EXPECT_EQ(predicate.type(), PredicateType::BITMAP_FILTER);

    BitmapValue bitmap_value;
    const std::vector<int32_t> filter_values(
            {1, 2, 3, 5, 8, 16, 32, 64, 65, 127, 128, 129, 256, 512});
    const std::vector<int32_t> values({512, 256, 999, 3, 32, 44, 32, 127, 127, 63, 63, 64, 100, 7});
    bitmap_value.add_many(filter_values.data(), filter_values.size());
    filter->insert(&bitmap_value);

    auto column = vectorized::ColumnInt32::create();
    column->insert_many_fix_len_data(reinterpret_cast<const char*>(values.data()), values.size());

    uint16_t* sel = new uint16_t[column->size()];
    for (size_t i = 0; i < column->size(); ++i) {
        sel[i] = i;
    }
    uint16_t size = column->size();
    size = predicate.ColumnPredicate::evaluate(*column, sel, size);
    EXPECT_EQ(size, 8);
    EXPECT_EQ(sel[0], 0);
    EXPECT_EQ(sel[1], 1);
    EXPECT_EQ(sel[2], 3);
    EXPECT_EQ(sel[3], 4);
    EXPECT_EQ(sel[4], 6);
    EXPECT_EQ(sel[5], 7);
    EXPECT_EQ(sel[6], 8);
    EXPECT_EQ(sel[7], 11);

    delete[] sel;
}

TEST_F(BitmapFilterColumnPredicateTest, evaluate_column_nullable) {
    auto filter = std::make_shared<BitmapFilterFunc<PrimitiveType::TYPE_INT>>();
    auto predicate = create_predicate(filter);
    EXPECT_EQ(predicate.type(), PredicateType::BITMAP_FILTER);

    BitmapValue bitmap_value;
    const std::vector<int32_t> filter_values(
            {1, 2, 3, 5, 8, 16, 32, 64, 65, 127, 128, 129, 256, 512});
    const std::vector<int32_t> values({512, 256, 999, 3, 32, 44, 32, 127, 127, 63, 63, 64, 100, 7});
    bitmap_value.add_many(filter_values.data(), filter_values.size());
    filter->insert(&bitmap_value);

    auto column = vectorized::ColumnInt32::create();
    column->insert_many_fix_len_data(reinterpret_cast<const char*>(values.data()), values.size());
    auto flag = vectorized::ColumnUInt8::create();
    flag->insert_many_defaults(column->size());
    flag->get_data()[0] = 1;
    flag->get_data()[2] = 1;
    flag->get_data()[3] = 1;
    flag->get_data()[5] = 1;
    auto column_nullable = vectorized::ColumnNullable::create(std::move(column), std::move(flag));

    uint16_t* sel = new uint16_t[column_nullable->size()];
    for (size_t i = 0; i < column_nullable->size(); ++i) {
        sel[i] = i;
    }

    uint16_t size = column_nullable->size();
    size = predicate.ColumnPredicate::evaluate(*column_nullable, sel, size);

    EXPECT_EQ(size, 6);
    EXPECT_EQ(sel[0], 1);
    EXPECT_EQ(sel[1], 4);
    EXPECT_EQ(sel[2], 6);
    EXPECT_EQ(sel[3], 7);
    EXPECT_EQ(sel[4], 8);
    EXPECT_EQ(sel[5], 11);

    delete[] sel;
}

} // namespace doris