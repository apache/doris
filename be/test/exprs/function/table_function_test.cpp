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

#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gmock/gmock-spec-builders.h>
#include <gtest/gtest-matchers.h>

#include <memory>
#include <string>
#include <vector>

#include "core/types.h"
#include "exprs/function/function_test_util.h"
#include "exprs/mock_vexpr.h"
#include "exprs/table_function/vexplode.h"
#include "exprs/table_function/vexplode_numbers.h"
#include "exprs/table_function/vexplode_v2.h"
#include "testutil/any_type.h"

namespace doris {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::SetArgPointee;

class TableFunctionTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}

    void clear() {
        _ctx = nullptr;
        _root = nullptr;
        _children.clear();
        _column_ids.clear();
    }

    void init_expr_context(int child_num) {
        clear();

        _root = std::make_shared<MockVExpr>();
        for (int i = 0; i < child_num; ++i) {
            _column_ids.push_back(i);
            _children.push_back(std::make_shared<MockVExpr>());
            EXPECT_CALL(*_children[i], execute(_, _, _))
                    .WillRepeatedly(DoAll(SetArgPointee<2>(_column_ids[i]), Return(Status::OK())));
            _root->add_child(_children[i]);
        }
        _ctx = std::make_shared<VExprContext>(_root);
    }

private:
    VExprContextSPtr _ctx;
    std::shared_ptr<MockVExpr> _root;
    std::vector<std::shared_ptr<MockVExpr>> _children;
    std::vector<int> _column_ids;
};

TEST_F(TableFunctionTest, vexplode_outer) {
    init_expr_context(1);
    VExplodeTableFunction explode_outer;
    explode_outer.set_outer();
    explode_outer.set_expr_context(_ctx);

    // explode_outer(Array<Int32>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_INT};
        TestArray vec = {Int32(1), Null(), Int32(2), Int32(3)};
        InputDataSet input_set = {{AnyType {vec}}, {Null()}, {AnyType {TestArray {}}}};

        InputTypeSet output_types = {PrimitiveType::TYPE_INT};
        InputDataSet output_set = {{Int32(1)}, {Null()}, {Int32(2)},
                                   {Int32(3)}, {Null()}, {Null()}};

        check_vec_table_function(&explode_outer, input_types, input_set, output_types, output_set);
    }

    // explode_outer(Array<String>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_VARCHAR};
        TestArray vec = {std::string("abc"), std::string(""), std::string("def")};
        InputDataSet input_set = {{Null()}, {AnyType {TestArray {}}}, {AnyType {vec}}};

        InputTypeSet output_types = {PrimitiveType::TYPE_VARCHAR};
        InputDataSet output_set = {
                {Null()}, {Null()}, {std::string("abc")}, {std::string("")}, {std::string("def")}};

        check_vec_table_function(&explode_outer, input_types, input_set, output_types, output_set);
    }

    // explode_outer(Array<Decimal>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DECIMALV2};
        TestArray vec = {ut_type::DECIMALV2VALUEFROMDOUBLE(17014116.67),
                         ut_type::DECIMALV2VALUEFROMDOUBLE(-17014116.67)};
        InputDataSet input_set = {{Null()}, {AnyType {TestArray {}}}, {AnyType {vec}}};

        InputTypeSet output_types = {PrimitiveType::TYPE_DECIMALV2};
        InputDataSet output_set = {{Null()},
                                   {Null()},
                                   {ut_type::DECIMALV2VALUEFROMDOUBLE(17014116.67)},
                                   {ut_type::DECIMALV2VALUEFROMDOUBLE(-17014116.67)}};

        check_vec_table_function(&explode_outer, input_types, input_set, output_types, output_set);
    }
}

TEST_F(TableFunctionTest, vexplode_outer_v2) {
    init_expr_context(1);
    VExplodeV2TableFunction explode_outer;
    explode_outer.set_outer();
    explode_outer.set_expr_context(_ctx);

    // explode_outer(Array<Int32>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_INT};
        TestArray vec = {Int32(1), Null(), Int32(2), Int32(3)};
        InputDataSet input_set = {{AnyType {vec}}, {Null()}, {AnyType {TestArray {}}}};

        InputTypeSet output_types = {PrimitiveType::TYPE_INT};

        InputDataSet output_set = {{Int32(1)}, {Null()}, {Int32(2)},
                                   {Int32(3)}, {Null()}, {Null()}};

        check_vec_table_function(&explode_outer, input_types, input_set, output_types, output_set);
    }

    // explode_outer(Array<String>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_VARCHAR};
        TestArray vec = {std::string("abc"), std::string(""), std::string("def")};
        InputDataSet input_set = {{Null()}, {AnyType {TestArray {}}}, {AnyType {vec}}};

        InputTypeSet output_types = {PrimitiveType::TYPE_VARCHAR};

        InputDataSet output_set = {
                {Null()}, {Null()}, {std::string("abc")}, {std::string("")}, {std::string("def")}};

        check_vec_table_function(&explode_outer, input_types, input_set, output_types, output_set);
    }

    // // explode_outer(Array<Decimal>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DECIMALV2};
        TestArray vec = {ut_type::DECIMALV2VALUEFROMDOUBLE(17014116.67),
                         ut_type::DECIMALV2VALUEFROMDOUBLE(-17014116.67)};
        InputDataSet input_set = {{Null()}, {AnyType {TestArray {}}}, {AnyType {vec}}};

        InputTypeSet output_types = {PrimitiveType::TYPE_DECIMALV2};

        InputDataSet output_set = {{Null()},
                                   {Null()},
                                   {ut_type::DECIMALV2VALUEFROMDOUBLE(17014116.67)},
                                   {ut_type::DECIMALV2VALUEFROMDOUBLE(-17014116.67)}};

        check_vec_table_function(&explode_outer, input_types, input_set, output_types, output_set);
    }
}

TEST_F(TableFunctionTest, vexplode) {
    init_expr_context(1);
    VExplodeTableFunction explode;
    explode.set_expr_context(_ctx);

    // explode(Array<Int32>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_INT};

        TestArray vec = {Int32(1), Null(), Int32(2), Int32(3)};
        InputDataSet input_set = {{AnyType {vec}}, {Null()}, {AnyType {TestArray {}}}};

        InputTypeSet output_types = {PrimitiveType::TYPE_INT};
        InputDataSet output_set = {{Int32(1)}, {Null()}, {Int32(2)}, {Int32(3)}};

        check_vec_table_function(&explode, input_types, input_set, output_types, output_set);
    }

    // explode(Array<String>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_VARCHAR};
        TestArray vec = {std::string("abc"), std::string(""), std::string("def")};
        InputDataSet input_set = {{Null()}, {AnyType {TestArray {}}}, {AnyType {vec}}};

        InputTypeSet output_types = {PrimitiveType::TYPE_VARCHAR};
        InputDataSet output_set = {{std::string("abc")}, {std::string("")}, {std::string("def")}};

        check_vec_table_function(&explode, input_types, input_set, output_types, output_set);
    }
}

TEST_F(TableFunctionTest, vexplode_v2) {
    init_expr_context(1);
    VExplodeV2TableFunction explode;
    explode.set_expr_context(_ctx);

    // explode(Array<Int32>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_INT};

        TestArray vec = {Int32(1), Null(), Int32(2), Int32(3)};
        InputDataSet input_set = {{AnyType {vec}}, {Null()}, {AnyType {TestArray {}}}};

        InputTypeSet output_types = {PrimitiveType::TYPE_INT};
        InputDataSet output_set = {{Int32(1)}, {Null()}, {Int32(2)}, {Int32(3)}};

        check_vec_table_function(&explode, input_types, input_set, output_types, output_set);
    }

    // explode(Array<String>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_VARCHAR};
        TestArray vec = {std::string("abc"), std::string(""), std::string("def")};
        InputDataSet input_set = {{Null()}, {AnyType {TestArray {}}}, {AnyType {vec}}};

        InputTypeSet output_types = {PrimitiveType::TYPE_VARCHAR};

        InputDataSet output_set = {{std::string("abc")}, {std::string("")}, {std::string("def")}};
        check_vec_table_function(&explode, input_types, input_set, output_types, output_set);
    }
}

TEST_F(TableFunctionTest, vexplode_v2_two_param) {
    init_expr_context(2);
    VExplodeV2TableFunction explode;
    explode.set_expr_context(_ctx);
    // explode(Array<String>, Array<String>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_VARCHAR};
        TestArray vec = {std::string("one"), std::string("two"), std::string("three")};
        TestArray vec1 = {std::string("1"), std::string("2"), std::string("3")};
        InputDataSet input_set = {{vec, vec1}};

        InputTypeSet output_types = {PrimitiveType::TYPE_STRUCT, PrimitiveType::TYPE_VARCHAR,
                                     PrimitiveType::TYPE_VARCHAR};

        InputDataSet output_set = {{{TestArray {std::string("one"), std::string("1")}}},
                                   {{TestArray {std::string("two"), std::string("2")}}},
                                   {{TestArray {std::string("three"), std::string("3")}}}};
        check_vec_table_function(&explode, input_types, input_set, output_types, output_set, false);
        check_vec_table_function(&explode, input_types, input_set, output_types, output_set, true);
    }

    // explode(null, Array<String>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_VARCHAR};
        TestArray vec = {std::string("one"), std::string("two"), std::string("three")};
        InputDataSet input_set = {{Null(), vec}};

        InputTypeSet output_types = {PrimitiveType::TYPE_STRUCT, PrimitiveType::TYPE_VARCHAR,
                                     PrimitiveType::TYPE_VARCHAR};

        InputDataSet output_set = {{{TestArray {Null(), std::string("one")}}},
                                   {{TestArray {Null(), std::string("two")}}},
                                   {{TestArray {Null(), std::string("three")}}}};

        check_vec_table_function(&explode, input_types, input_set, output_types, output_set, false);
        check_vec_table_function(&explode, input_types, input_set, output_types, output_set, true);
    }

    // explode(Array<Null>, Array<String>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_VARCHAR};
        TestArray vec = {std::string("one"), std::string("two"), std::string("three")};
        TestArray vec1 = {std::string("1"), Null(), std::string("3")};
        InputDataSet input_set = {{vec, vec1}};

        InputTypeSet output_types = {PrimitiveType::TYPE_STRUCT, PrimitiveType::TYPE_VARCHAR,
                                     PrimitiveType::TYPE_VARCHAR};

        InputDataSet output_set = {{{TestArray {std::string("one"), std::string("1")}}},
                                   {{TestArray {std::string("two"), Null()}}},
                                   {{TestArray {std::string("three"), std::string("3")}}}};

        check_vec_table_function(&explode, input_types, input_set, output_types, output_set, false);
        check_vec_table_function(&explode, input_types, input_set, output_types, output_set, true);
    }
}

TEST_F(TableFunctionTest, vexplode_numbers) {
    init_expr_context(1);
    VExplodeNumbersTableFunction tfn;
    tfn.set_expr_context(_ctx);

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_INT};
        InputDataSet input_set = {{Int32(2)}, {Int32(3)}, {Null()}, {Int32(0)}, {Int32(-2)}};

        InputTypeSet output_types = {PrimitiveType::TYPE_INT};
        InputDataSet output_set = {{Int32(0)}, {Int32(1)}, {Int32(0)}, {Int32(1)}, {Int32(2)}};

        check_vec_table_function(&tfn, input_types, input_set, output_types, output_set);
    }
}

// ---------------------------------------------------------------------------
// Direct-API helpers for json_each / json_each_text tests.
// The test framework's check_vec_table_function does not properly support
// TYPE_STRUCT output (insert_cell always expects ColumnNullable wrapping the
// struct column), so we drive the table function API directly.
// ---------------------------------------------------------------------------

// Build a one-column JSONB input block.  An empty string means SQL NULL.
static std::unique_ptr<Block> build_jsonb_input_block(const std::vector<std::string>& json_rows) {
    auto str_col = ColumnString::create();
    auto null_col = ColumnUInt8::create();
    for (const auto& json : json_rows) {
        if (json.empty()) {
            str_col->insert_default();
            null_col->insert_value(1);
        } else {
            JsonBinaryValue jbv;
            if (jbv.from_json_string(json.c_str(), json.size()).ok()) {
                str_col->insert_data(jbv.value(), jbv.size());
                null_col->insert_value(0);
            } else {
                str_col->insert_default();
                null_col->insert_value(1);
            }
        }
    }
    auto col = ColumnNullable::create(std::move(str_col), std::move(null_col));
    auto block = Block::create_unique();
    block->insert({std::move(col),
                   make_nullable(DataTypeFactory::instance().create_data_type(
                           doris::PrimitiveType::TYPE_JSONB, false)),
                   "jval"});
    return block;
}

// Run the given table function over all rows in block.
// Returns list of (key, value) pairs where value == "__NULL__" means SQL NULL.
// val_is_jsonb controls whether the value column is decoded as JSONB→JSON text or plain text.
static std::vector<std::pair<std::string, std::string>> run_json_each_fn(TableFunction* fn,
                                                                         Block* block,
                                                                         bool val_is_jsonb) {
    // Output type: Nullable(Struct(Nullable(VARCHAR key), Nullable(VARCHAR/JSONB value)))
    DataTypePtr key_dt = make_nullable(DataTypeFactory::instance().create_data_type(
            doris::PrimitiveType::TYPE_VARCHAR, false));
    DataTypePtr val_dt = make_nullable(DataTypeFactory::instance().create_data_type(
            val_is_jsonb ? doris::PrimitiveType::TYPE_JSONB : doris::PrimitiveType::TYPE_VARCHAR,
            false));
    DataTypePtr struct_dt =
            make_nullable(std::make_shared<DataTypeStruct>(DataTypes {key_dt, val_dt}));

    auto out_col = struct_dt->create_column();
    fn->set_nullable();

    TQueryOptions q_opts;
    TQueryGlobals q_globals;
    RuntimeState rs(q_opts, q_globals);
    EXPECT_TRUE(fn->process_init(block, &rs).ok());

    for (size_t row = 0; row < block->rows(); ++row) {
        fn->process_row(row);
        if (!fn->current_empty()) {
            do {
                fn->get_value(out_col, 1);
            } while (!fn->eos());
        }
    }
    fn->process_close();

    std::vector<std::pair<std::string, std::string>> result;
    const auto& nullable_out = assert_cast<const ColumnNullable&>(*out_col);
    const auto& struct_col = assert_cast<const ColumnStruct&>(nullable_out.get_nested_column());
    const auto& key_col = assert_cast<const ColumnNullable&>(struct_col.get_column(0));
    const auto& val_col = assert_cast<const ColumnNullable&>(struct_col.get_column(1));

    for (size_t i = 0; i < struct_col.size(); ++i) {
        if (nullable_out.is_null_at(i)) {
            result.emplace_back("__NULL_ROW__", "__NULL_ROW__");
            continue;
        }
        std::string key;
        if (!key_col.is_null_at(i)) {
            StringRef sr = key_col.get_nested_column().get_data_at(i);
            key.assign(sr.data, sr.size);
        }
        std::string val;
        if (val_col.is_null_at(i)) {
            val = "__NULL__";
        } else {
            StringRef sr = val_col.get_nested_column().get_data_at(i);
            if (val_is_jsonb) {
                // JSONB binary → JSON text for comparison
                const JsonbDocument* doc = nullptr;
                if (JsonbDocument::checkAndCreateDocument(sr.data, sr.size, &doc).ok() && doc &&
                    doc->getValue()) {
                    val = JsonbToJson().to_json_string(doc->getValue());
                } else {
                    val = "__BAD_JSONB__";
                }
            } else {
                val.assign(sr.data, sr.size);
            }
        }
        result.emplace_back(std::move(key), std::move(val));
    }
    return result;
}

TEST_F(TableFunctionTest, vjson_each) {
    init_expr_context(1);
    VJsonEachTableFn fn;
    fn.set_expr_context(_ctx);

    // basic: string and numeric values; JSONB value column shows JSON text with quotes
    {
        auto block = build_jsonb_input_block({{R"({"a":"foo","b":123})"}});
        auto rows = run_json_each_fn(&fn, block.get(), true);
        ASSERT_EQ(2u, rows.size());
        EXPECT_EQ("a", rows[0].first);
        EXPECT_EQ("\"foo\"", rows[0].second); // JSONB string → JSON text includes quotes
        EXPECT_EQ("b", rows[1].first);
        EXPECT_EQ("123", rows[1].second);
    }

    // JSON null value → SQL NULL
    {
        auto block = build_jsonb_input_block({{R"({"x":null})"}});
        auto rows = run_json_each_fn(&fn, block.get(), true);
        ASSERT_EQ(1u, rows.size());
        EXPECT_EQ("x", rows[0].first);
        EXPECT_EQ("__NULL__", rows[0].second);
    }

    // boolean and negative int
    {
        auto block = build_jsonb_input_block({{R"({"flag":true,"neg":-1})"}});
        auto rows = run_json_each_fn(&fn, block.get(), true);
        ASSERT_EQ(2u, rows.size());
        bool ok_flag = false, ok_neg = false;
        for (auto& kv : rows) {
            if (kv.first == "flag") {
                EXPECT_EQ("true", kv.second);
                ok_flag = true;
            }
            if (kv.first == "neg") {
                EXPECT_EQ("-1", kv.second);
                ok_neg = true;
            }
        }
        EXPECT_TRUE(ok_flag) << "key 'flag' not found";
        EXPECT_TRUE(ok_neg) << "key 'neg' not found";
    }

    // SQL NULL input → 0 rows
    {
        auto block = build_jsonb_input_block({{""}}); // empty string → SQL NULL
        auto rows = run_json_each_fn(&fn, block.get(), true);
        EXPECT_EQ(0u, rows.size());
    }

    // empty object → 0 rows
    {
        auto block = build_jsonb_input_block({{"{}"}});
        auto rows = run_json_each_fn(&fn, block.get(), true);
        EXPECT_EQ(0u, rows.size());
    }

    // non-object input → 0 rows
    {
        auto block = build_jsonb_input_block({{"[1,2,3]"}});
        auto rows = run_json_each_fn(&fn, block.get(), true);
        EXPECT_EQ(0u, rows.size());
    }
}

TEST_F(TableFunctionTest, vjson_each_text) {
    init_expr_context(1);
    VJsonEachTextTableFn fn;
    fn.set_expr_context(_ctx);

    // basic: strings unquoted (text mode), numbers as plain text
    {
        auto block = build_jsonb_input_block({{R"({"a":"foo","b":123})"}});
        auto rows = run_json_each_fn(&fn, block.get(), false);
        ASSERT_EQ(2u, rows.size());
        EXPECT_EQ("a", rows[0].first);
        EXPECT_EQ("foo", rows[0].second); // string unquoted in text mode
        EXPECT_EQ("b", rows[1].first);
        EXPECT_EQ("123", rows[1].second);
    }

    // booleans
    {
        auto block = build_jsonb_input_block({{R"({"t":true,"f":false})"}});
        auto rows = run_json_each_fn(&fn, block.get(), false);
        ASSERT_EQ(2u, rows.size());
        bool ok_t = false, ok_f = false;
        for (auto& kv : rows) {
            if (kv.first == "t") {
                EXPECT_EQ("true", kv.second);
                ok_t = true;
            }
            if (kv.first == "f") {
                EXPECT_EQ("false", kv.second);
                ok_f = true;
            }
        }
        EXPECT_TRUE(ok_t) << "key 't' not found";
        EXPECT_TRUE(ok_f) << "key 'f' not found";
    }

    // JSON null → SQL NULL
    {
        auto block = build_jsonb_input_block({{R"({"x":null})"}});
        auto rows = run_json_each_fn(&fn, block.get(), false);
        ASSERT_EQ(1u, rows.size());
        EXPECT_EQ("x", rows[0].first);
        EXPECT_EQ("__NULL__", rows[0].second);
    }

    // SQL NULL input → 0 rows
    {
        auto block = build_jsonb_input_block({{""}});
        auto rows = run_json_each_fn(&fn, block.get(), false);
        EXPECT_EQ(0u, rows.size());
    }

    // empty object → 0 rows
    {
        auto block = build_jsonb_input_block({{"{}"}});
        auto rows = run_json_each_fn(&fn, block.get(), false);
        EXPECT_EQ(0u, rows.size());
    }
}
TEST_F(TableFunctionTest, vjson_each_get_same_many_values) {
    init_expr_context(1);
    VJsonEachTableFn fn;
    fn.set_expr_context(_ctx);
    fn.set_nullable();

    DataTypePtr key_dt = make_nullable(DataTypeFactory::instance().create_data_type(
            doris::PrimitiveType::TYPE_VARCHAR, false));
    DataTypePtr val_dt = make_nullable(
            DataTypeFactory::instance().create_data_type(doris::PrimitiveType::TYPE_JSONB, false));
    DataTypePtr struct_dt =
            make_nullable(std::make_shared<DataTypeStruct>(DataTypes {key_dt, val_dt}));

    TQueryOptions q_opts;
    TQueryGlobals q_globals;
    RuntimeState rs(q_opts, q_globals);

    // Case 1: normal object — get_same_many_values replicates the entry at _cur_offset.
    // Simulates a non-last table function being asked to copy its current value 3 times
    // to match 3 rows emitted by the driving (last) function in the same pass.
    {
        auto block = build_jsonb_input_block({{R"({"k0":"v0","k1":"v1"})"}});
        ASSERT_TRUE(fn.process_init(block.get(), &rs).ok());
        fn.process_row(0);
        ASSERT_FALSE(fn.current_empty());

        auto out_col = struct_dt->create_column();
        fn.get_same_many_values(out_col, 3);

        const auto& nullable_out = assert_cast<const ColumnNullable&>(*out_col);
        ASSERT_EQ(3u, nullable_out.size());
        const auto& struct_col = assert_cast<const ColumnStruct&>(nullable_out.get_nested_column());
        const auto& key_col = assert_cast<const ColumnNullable&>(struct_col.get_column(0));
        // All 3 output rows should carry the entry at _cur_offset=0 ("k0")
        for (size_t i = 0; i < 3; ++i) {
            EXPECT_FALSE(nullable_out.is_null_at(i));
            ASSERT_FALSE(key_col.is_null_at(i));
            StringRef k = key_col.get_nested_column().get_data_at(i);
            EXPECT_EQ("k0", std::string(k.data, k.size));
        }
        fn.process_close();
    }

    // Case 2: SQL NULL input — current_empty() is true → insert_many_defaults.
    {
        auto block = build_jsonb_input_block({{""}}); // empty string → SQL NULL
        ASSERT_TRUE(fn.process_init(block.get(), &rs).ok());
        fn.process_row(0);
        ASSERT_TRUE(fn.current_empty());

        auto out_col = struct_dt->create_column();
        fn.get_same_many_values(out_col, 2);

        ASSERT_EQ(2u, out_col->size());
        const auto& nullable_out = assert_cast<const ColumnNullable&>(*out_col);
        EXPECT_TRUE(nullable_out.is_null_at(0));
        EXPECT_TRUE(nullable_out.is_null_at(1));
        fn.process_close();
    }
}

TEST_F(TableFunctionTest, vjson_each_outer) {
    init_expr_context(1);
    VJsonEachTableFn fn;
    fn.set_expr_context(_ctx);

    // set_outer() correctly sets the is_outer flag
    EXPECT_FALSE(fn.is_outer());
    fn.set_outer();
    EXPECT_TRUE(fn.is_outer());

    // Normal object: outer flag does not affect KV expansion
    {
        auto block = build_jsonb_input_block({{R"({"a":"foo","b":123})"}});
        auto rows = run_json_each_fn(&fn, block.get(), true);
        ASSERT_EQ(2u, rows.size());
        EXPECT_EQ("a", rows[0].first);
        EXPECT_EQ("\"foo\"", rows[0].second);
        EXPECT_EQ("b", rows[1].first);
        EXPECT_EQ("123", rows[1].second);
    }

    // For NULL / empty-object / non-object inputs: current_empty() is true.
    // The operator calls get_value() unconditionally when is_outer() — verify that
    // get_value() inserts exactly one default (NULL) struct row in each case.
    DataTypePtr key_dt = make_nullable(DataTypeFactory::instance().create_data_type(
            doris::PrimitiveType::TYPE_VARCHAR, false));
    DataTypePtr val_dt = make_nullable(
            DataTypeFactory::instance().create_data_type(doris::PrimitiveType::TYPE_JSONB, false));
    DataTypePtr struct_dt =
            make_nullable(std::make_shared<DataTypeStruct>(DataTypes {key_dt, val_dt}));

    TQueryOptions q_opts;
    TQueryGlobals q_globals;
    RuntimeState rs(q_opts, q_globals);

    for (const char* input : {"", "{}", "[1,2,3]"}) {
        auto block = build_jsonb_input_block({{input}});
        ASSERT_TRUE(fn.process_init(block.get(), &rs).ok()) << "input: " << input;
        fn.process_row(0);
        EXPECT_TRUE(fn.current_empty()) << "input: " << input;

        auto out_col = struct_dt->create_column();
        fn.get_value(out_col, 1);
        ASSERT_EQ(1u, out_col->size()) << "input: " << input;
        EXPECT_TRUE(out_col->is_null_at(0)) << "input: " << input;
        fn.process_close();
    }
}

TEST_F(TableFunctionTest, vjson_each_text_outer) {
    init_expr_context(1);
    VJsonEachTextTableFn fn;
    fn.set_expr_context(_ctx);

    EXPECT_FALSE(fn.is_outer());
    fn.set_outer();
    EXPECT_TRUE(fn.is_outer());

    // Normal object: text mode (strings unquoted), outer flag does not affect expansion
    {
        auto block = build_jsonb_input_block({{R"({"a":"foo","b":123})"}});
        auto rows = run_json_each_fn(&fn, block.get(), false);
        ASSERT_EQ(2u, rows.size());
        EXPECT_EQ("a", rows[0].first);
        EXPECT_EQ("foo", rows[0].second);
        EXPECT_EQ("b", rows[1].first);
        EXPECT_EQ("123", rows[1].second);
    }

    // NULL / empty-object / non-object → current_empty(), get_value() inserts one default row
    DataTypePtr key_dt = make_nullable(DataTypeFactory::instance().create_data_type(
            doris::PrimitiveType::TYPE_VARCHAR, false));
    DataTypePtr val_dt = make_nullable(DataTypeFactory::instance().create_data_type(
            doris::PrimitiveType::TYPE_VARCHAR, false));
    DataTypePtr struct_dt =
            make_nullable(std::make_shared<DataTypeStruct>(DataTypes {key_dt, val_dt}));

    TQueryOptions q_opts;
    TQueryGlobals q_globals;
    RuntimeState rs(q_opts, q_globals);

    for (const char* input : {"", "{}", "[1,2,3]"}) {
        auto block = build_jsonb_input_block({{input}});
        ASSERT_TRUE(fn.process_init(block.get(), &rs).ok()) << "input: " << input;
        fn.process_row(0);
        EXPECT_TRUE(fn.current_empty()) << "input: " << input;

        auto out_col = struct_dt->create_column();
        fn.get_value(out_col, 1);
        ASSERT_EQ(1u, out_col->size()) << "input: " << input;
        EXPECT_TRUE(out_col->is_null_at(0)) << "input: " << input;
        fn.process_close();
    }
}
} // namespace doris
