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

#include "olap/rowset/segment_v2/variant/hierarchical_data_iterator.h"

#include <gtest/gtest.h>

#include <cstring>

#include "vec/columns/column_map.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_variant.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/json/path_in_data.h"

using doris::Status;
using doris::segment_v2::ColumnIterator;
using doris::segment_v2::ColumnIteratorOptions;
using doris::segment_v2::HierarchicalDataIterator;
using doris::vectorized::ColumnMap;
using doris::vectorized::ColumnString;
using doris::vectorized::ColumnVariant;
using doris::vectorized::MutableColumnPtr;
using doris::vectorized::PathInData;

class DummySparseIterator final : public ColumnIterator {
public:
    Status init(const ColumnIteratorOptions&) override { return Status::OK(); }
    Status seek_to_ordinal(ordinal_t) override { return Status::OK(); }
    ordinal_t get_current_ordinal() const override { return 0; }
    Status next_batch(size_t*, MutableColumnPtr&, bool*) override { return Status::OK(); }
    Status read_by_rowids(const doris::segment_v2::rowid_t*, const size_t,
                          MutableColumnPtr&) override {
        return Status::OK();
    }
};

TEST(HierarchicalDataIteratorTest, ProcessSparseExtractSubpaths) {
    std::unique_ptr<ColumnIterator> sparse_reader = std::make_unique<DummySparseIterator>();
    HierarchicalDataIterator::ReadType read_type = HierarchicalDataIterator::ReadType::READ_DIRECT;
    doris::segment_v2::ColumnIteratorUPtr iter;
    ASSERT_TRUE(HierarchicalDataIterator::create(&iter, PathInData("a.b"), /*node*/ nullptr,
                                                 /*root*/ nullptr, read_type,
                                                 std::move(sparse_reader))
                        .ok());

    ColumnIteratorOptions opts;
    ASSERT_TRUE(iter->init(opts).ok());
    ASSERT_TRUE(iter->seek_to_ordinal(0).ok());

    auto* hiter = static_cast<HierarchicalDataIterator*>(iter.get());
    auto& map = assert_cast<ColumnMap&>(*hiter->_sparse_column_reader->column);
    auto& keys = assert_cast<ColumnString&>(map.get_keys());
    auto& vals = assert_cast<ColumnString&>(map.get_values());
    auto& offs = map.get_offsets();

    doris::vectorized::DataTypePtr str_type = std::make_shared<doris::vectorized::DataTypeString>();
    auto str_col = str_type->create_column();
    auto serde = str_type->get_serde();
    str_col->insert_data("abcvalues", strlen("abcvalues"));
    str_col->insert_data("abdvalues", strlen("abdvalues"));
    str_col->insert_data("abcvalues", strlen("abcvalues"));
    str_col->insert_data("abevalues", strlen("abevalues"));
    str_col->insert_data("axvalues", strlen("axvalues"));
    ColumnString::Chars& chars = vals.get_chars();
    for (size_t i = 0; i < 5; ++i) {
        serde->write_one_cell_to_binary(*str_col, chars, i);
        vals.get_offsets().push_back(chars.size());
    }

    // row0: {"a.b.c": "abcvalues", "a.b.d": "abdvalues"}
    keys.insert_data("a.b.c", strlen("a.b.c"));
    keys.insert_data("a.b.d", strlen("a.b.d"));
    offs.push_back(keys.size());

    // row1: {"a.b.c": "abcvalues", "a.b.e": "abevalues", "a.x": "axvalues"}
    keys.insert_data("a.b.c", strlen("a.b.c"));
    keys.insert_data("a.b.e", strlen("a.b.e"));
    keys.insert_data("a.x", strlen("a.x"));
    offs.push_back(keys.size());

    const size_t nrows = 2;
    MutableColumnPtr dst = ColumnVariant::create(/*max_subcolumns_count*/ 2, nrows);

    auto& variant = assert_cast<ColumnVariant&>(*dst);
    ASSERT_TRUE(hiter->_process_sparse_column(variant, nrows).ok());

    // root column + 2 subcolumns
    EXPECT_EQ(variant.get_subcolumns().size(), 3);

    auto* abc_subcolumn = variant.get_subcolumn(PathInData("c"));
    auto* abd_subcolumn = variant.get_subcolumn(PathInData("d"));

    EXPECT_TRUE(abc_subcolumn);
    EXPECT_TRUE(abd_subcolumn);

    EXPECT_EQ(abc_subcolumn->get_non_null_value_size(), 2);
    EXPECT_EQ(abd_subcolumn->get_non_null_value_size(), 1);

    const auto& abc_subcolumn_data = assert_cast<const doris::vectorized::ColumnNullable&>(
            *abc_subcolumn->get_finalized_column_ptr());
    const auto& abd_subcolumn_data = assert_cast<const doris::vectorized::ColumnNullable&>(
            *abd_subcolumn->get_finalized_column_ptr());
    EXPECT_EQ(abc_subcolumn_data.get_nested_column_ptr()->get_data_at(0).to_string(), "abcvalues");
    EXPECT_EQ(abc_subcolumn_data.get_nested_column_ptr()->get_data_at(1).to_string(), "abcvalues");
    EXPECT_EQ(abd_subcolumn_data.get_nested_column_ptr()->get_data_at(0).to_string(), "abdvalues");

    const auto& read_map = assert_cast<const ColumnMap&>(*variant.get_sparse_column());
    const auto& read_keys = assert_cast<const ColumnString&>(read_map.get_keys());
    const auto& read_vals = assert_cast<const ColumnString&>(read_map.get_values());
    const auto& read_offs = read_map.get_offsets();

    EXPECT_EQ(read_offs.size(), 2);

    EXPECT_EQ(read_keys.get_element(0), "e");
    auto val = read_vals.get_data_at(0).to_string();
    EXPECT_EQ(val.substr(val.size() - 9, 9), "abevalues");

    EXPECT_EQ(read_offs[0], 0);
    EXPECT_EQ(read_offs[1], 1);
}
