
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

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>
#include <cctz/time_zone.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/parquet_types.h>
#include <glog/logging.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/schema.h>
#include <stddef.h>
#include <sys/types.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <memory>
#include <new>
#include <ostream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "common/status.h"
#include "exec/schema_scanner.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "testutil/mock/mock_fn_call.h"
#include "testutil/mock/mock_literal_expr.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/timezone_utils.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/exec/format/parquet/parquet_thrift_util.h"
#include "vec/exec/format/parquet/schema_desc.h"
#include "vec/exec/format/parquet/vparquet_column_chunk_reader.h"
#include "vec/exec/format/parquet/vparquet_file_metadata.h"
#include "vec/exec/format/parquet/vparquet_reader.h"

namespace doris {
namespace vectorized {
class VExprContext;
//using namespace iceberg;
using namespace parquet;

class ParquetExprTest : public testing::Test {
public:
    ParquetExprTest() {}

    void SetUp() override {
        std::string test_dir = "ut_dir/test_parquet_expr";
        Status st;
        st = io::global_local_filesystem()->delete_directory(test_dir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(test_dir);
        ASSERT_TRUE(st.ok()) << st;

        // 1. 构造各种类型的 Arrow Array
        const int num_rows = 6;

        // int32 部分为 null
        arrow::Int32Builder int32_partial_null_builder;
        for (int i = 0; i < num_rows; ++i) {
            if (i % 3 == 0) {
                ASSERT_TRUE(int32_partial_null_builder.AppendNull().ok());
            } else {
                ASSERT_TRUE(int32_partial_null_builder.Append(i * 10).ok());
            }
        }
        std::shared_ptr<arrow::Array> int32_partial_null_array;
        ASSERT_TRUE(int32_partial_null_builder.Finish(&int32_partial_null_array).ok());

        // int32 全为 null
        arrow::Int32Builder int32_all_null_builder;
        for (int i = 0; i < num_rows; ++i) {
            ASSERT_TRUE(int32_all_null_builder.AppendNull().ok());
        }
        std::shared_ptr<arrow::Array> int32_all_null_array;
        ASSERT_TRUE(int32_all_null_builder.Finish(&int32_all_null_array).ok());

        // int64
        arrow::Int64Builder int64_builder;
        for (int i = 0; i < num_rows; ++i) {
            ASSERT_TRUE(int64_builder.Append(10000000000 + i).ok());
        }
        std::shared_ptr<arrow::Array> int64_array;
        ASSERT_TRUE(int64_builder.Finish(&int64_array).ok());

        // float
        arrow::FloatBuilder float_builder;
        for (int i = 0; i < num_rows; ++i) {
            ASSERT_TRUE(float_builder.Append(1.1f + i).ok());
        }
        std::shared_ptr<arrow::Array> float_array;
        ASSERT_TRUE(float_builder.Finish(&float_array).ok());

        // double
        arrow::DoubleBuilder double_builder;
        for (int i = 0; i < num_rows; ++i) {
            ASSERT_TRUE(double_builder.Append(2.22 + i).ok());
        }
        std::shared_ptr<arrow::Array> double_array;
        ASSERT_TRUE(double_builder.Finish(&double_array).ok());

        // string
        arrow::StringBuilder string_builder;
        for (int i = 0; i < num_rows; ++i) {
            ASSERT_TRUE(string_builder.Append("name_" + std::to_string(i)).ok());
        }
        std::shared_ptr<arrow::Array> string_array;
        ASSERT_TRUE(string_builder.Finish(&string_array).ok());

        // bool
        arrow::BooleanBuilder bool_builder;
        for (int i = 0; i < num_rows; ++i) {
            ASSERT_TRUE(bool_builder.Append(i % 2 == 0).ok());
        }
        std::shared_ptr<arrow::Array> bool_array;
        ASSERT_TRUE(bool_builder.Finish(&bool_array).ok());

        // date32
        arrow::Date32Builder date_builder;
        for (int i = 0; i < num_rows; ++i) {
            // 以 2020-01-01 为基准，每行递增一天
            ASSERT_TRUE(
                    date_builder.Append(18262 + i).ok()); // 18262 是 2020-01-01 的 days since epoch
        }
        std::shared_ptr<arrow::Array> date_array;
        ASSERT_TRUE(date_builder.Finish(&date_array).ok());

        // timestamp
        arrow::TimestampBuilder ts_builder(arrow::timestamp(arrow::TimeUnit::SECOND),
                                           arrow::default_memory_pool());
        for (int i = 0; i < num_rows; ++i) {
            ASSERT_TRUE(ts_builder.Append(1609459200 + i * 3600).ok()); // 每小时递增
        }
        std::shared_ptr<arrow::Array> timestamp_array;
        ASSERT_TRUE(ts_builder.Finish(&timestamp_array).ok());

        // decimal(10,2)
        std::shared_ptr<arrow::DataType> decimal_type_10_2 = arrow::decimal128(10, 2);
        arrow::Decimal128Builder decimal_builder_10_2(decimal_type_10_2,
                                                      arrow::default_memory_pool());
        for (int i = 0; i < num_rows; ++i) {
            ASSERT_TRUE(decimal_builder_10_2.Append(arrow::Decimal128(10000 + i * 100)).ok());
        }
        std::shared_ptr<arrow::Array> decimal_array_10_2;
        ASSERT_TRUE(decimal_builder_10_2.Finish(&decimal_array_10_2).ok());

        // decimal(18,6)
        std::shared_ptr<arrow::DataType> decimal_type_18_6 = arrow::decimal128(18, 6);
        arrow::Decimal128Builder decimal_builder_18_6(decimal_type_18_6,
                                                      arrow::default_memory_pool());
        for (int i = 0; i < num_rows; ++i) {
            ASSERT_TRUE(decimal_builder_18_6.Append(arrow::Decimal128(1000000 + i * 10000)).ok());
        }
        std::shared_ptr<arrow::Array> decimal_array_18_6;
        ASSERT_TRUE(decimal_builder_18_6.Finish(&decimal_array_18_6).ok());

        // 2. 构造 Arrow Schema
        std::vector<std::shared_ptr<arrow::Field>> fields = {
                arrow::field("int32_partial_null_col", arrow::int32()),
                arrow::field("int32_all_null_col", arrow::int32()),
                arrow::field("int64_col", arrow::int64()),
                arrow::field("float_col", arrow::float32()),
                arrow::field("double_col", arrow::float64()),
                arrow::field("string_col", arrow::utf8()),
                arrow::field("bool_col", arrow::boolean()),
                arrow::field("date_col", arrow::date32()),
                arrow::field("timestamp_col", arrow::timestamp(arrow::TimeUnit::SECOND)),
                arrow::field("decimal_col_10_2", decimal_type_10_2),
                arrow::field("decimal_col_18_6", decimal_type_18_6)};
        auto arrow_schema = arrow::schema(fields);

        // 3. 构造 Arrow Table
        auto table = arrow::Table::Make(
                arrow_schema, {int32_partial_null_array, int32_all_null_array, int64_array,
                               float_array, double_array, string_array, bool_array, date_array,
                               timestamp_array, decimal_array_10_2, decimal_array_18_6});

        file_path = test_dir + "/f1.parquet";
        std::shared_ptr<arrow::io::FileOutputStream> outfile;
        auto result_file = arrow::io::FileOutputStream::Open(file_path);
        ASSERT_TRUE(result_file.ok());
        outfile = std::move(result_file).ValueUnsafe();

        PARQUET_THROW_NOT_OK(
                ::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 3));

        std::vector<std::string> table_column_names = {"int32_partial_null_col",
                                                       "int32_all_null_col",
                                                       "int64_col",
                                                       "float_col",
                                                       "double_col",
                                                       "string_col",
                                                       "bool_col",
                                                       "date_col",
                                                       "timestamp_col",
                                                       "decimal_col_10_2",
                                                       "decimal_col_18_6"};
        std::vector<TPrimitiveType::type> table_column_types = {
                TPrimitiveType::INT,       TPrimitiveType::INT,        TPrimitiveType::BIGINT,
                TPrimitiveType::FLOAT,     TPrimitiveType::DOUBLE,     TPrimitiveType::STRING,
                TPrimitiveType::BOOLEAN,   TPrimitiveType::DATEV2,     TPrimitiveType::DATETIMEV2,
                TPrimitiveType::DECIMAL64, TPrimitiveType::DECIMAL128I};
        create_table_desc(t_desc_table, t_table_desc, table_column_names, table_column_types);

        static_cast<void>(DescriptorTbl::create(&obj_pool, t_desc_table, &desc_tbl));
        auto tuple_desc = desc_tbl->get_tuple_descriptor(0);
        slot_descs = desc_tbl->get_tuple_descriptor(0)->slots();
        auto local_fs = io::global_local_filesystem();
        io::FileReaderSPtr local_file_reader;
        static_cast<void>(local_fs->open_file(file_path, &local_file_reader));

        cctz::time_zone ctz;
        TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
        //        auto tuple_desc = desc_tbl->get_tuple_descriptor(0);
        std::vector<std::string> column_names;
        for (int i = 0; i < slot_descs.size(); i++) {
            column_names.push_back(slot_descs[i]->col_name());
        }
        TFileScanRangeParams scan_params;
        TFileRangeDesc scan_range;
        {
            scan_range.start_offset = 0;
            scan_range.size = local_file_reader->size();
        }

        p_reader = ParquetReader::create_unique(nullptr, scan_params, scan_range, scan_range.size,
                                                &ctz, nullptr, nullptr);
        p_reader->set_file_reader(local_file_reader);

        static_cast<void>(p_reader->init_reader(column_names, nullptr, {}, tuple_desc, nullptr,
                                                nullptr, nullptr, nullptr));

        size_t meta_size;
        static_cast<void>(parse_thrift_footer(p_reader->_file_reader, &doris_file_metadata,
                                              &meta_size, nullptr));
        doris_metadata = doris_file_metadata->to_thrift();
    }

    static void create_table_desc(TDescriptorTable& t_desc_table, TTableDescriptor& t_table_desc,
                                  std::vector<std::string> table_column_names,
                                  std::vector<TPrimitiveType::type> types) {
        t_table_desc.id = 0;
        t_table_desc.tableType = TTableType::OLAP_TABLE;
        t_table_desc.numCols = 0;
        t_table_desc.numClusteringCols = 0;
        t_desc_table.tableDescriptors.push_back(t_table_desc);
        t_desc_table.__isset.tableDescriptors = true;

        // init boolean and numeric slot
        for (int i = 0; i < table_column_names.size(); i++) {
            TSlotDescriptor tslot_desc;
            {
                tslot_desc.id = i;
                tslot_desc.parent = 0;
                TTypeDesc type;
                {
                    TTypeNode node;
                    node.__set_type(TTypeNodeType::SCALAR);
                    TScalarType scalar_type;
                    scalar_type.__set_type(types[i]);
                    if (types[i] == TPrimitiveType::DECIMAL64) {
                        scalar_type.__set_precision(10);
                        scalar_type.__set_scale(2);
                    } else if (types[i] == TPrimitiveType::DECIMAL128I) {
                        scalar_type.__set_precision(18);
                        scalar_type.__set_scale(6);
                    }

                    node.__set_scalar_type(scalar_type);
                    type.types.push_back(node);
                }
                tslot_desc.slotType = type;
                tslot_desc.columnPos = 0;
                tslot_desc.byteOffset = 0;
                tslot_desc.nullIndicatorByte = 0;
                tslot_desc.nullIndicatorBit = -1;
                tslot_desc.colName = table_column_names[i];
                tslot_desc.slotIdx = 0;
                tslot_desc.isMaterialized = true;
                t_desc_table.slotDescriptors.push_back(tslot_desc);
            }
        }

        t_desc_table.__isset.slotDescriptors = true;
        {
            // TTupleDescriptor dest
            TTupleDescriptor t_tuple_desc;
            t_tuple_desc.id = 0;
            t_tuple_desc.byteSize = 16;
            t_tuple_desc.numNullBytes = 0;
            t_tuple_desc.tableId = 0;
            t_tuple_desc.__isset.tableId = true;
            t_desc_table.tupleDescriptors.push_back(t_tuple_desc);
        }
    };

    //        st = io::global_local_filesystem()->delete_directory(test_dir);
    //        EXPECT_TRUE(st.ok()) << st;

    std::string file_path;
    std::unique_ptr<ParquetReader> p_reader;
    // create doirs parquet reader.
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    std::vector<SlotDescriptor*> slot_descs;
    FileMetaData* doris_file_metadata;
    tparquet::FileMetaData doris_metadata;
};

TEST_F(ParquetExprTest, test_min_max) {
    // open parquet with parquet's API
    std::unique_ptr<::parquet::ParquetFileReader> arrow_reader =
            ::parquet::ParquetFileReader::OpenFile(file_path, false);
    std::shared_ptr<::parquet::FileMetaData> arrow_metadata = arrow_reader->metadata();

    Status st;

    EXPECT_EQ(arrow_reader->metadata()->num_row_groups(), doris_metadata.row_groups.size());

    for (auto row_group_idx = 0; row_group_idx < arrow_reader->metadata()->num_row_groups();
         row_group_idx++) {
        for (auto column_idx = 0; column_idx < arrow_reader->metadata()->num_columns();
             column_idx++) {
            const auto& column_meta_data =
                    doris_metadata.row_groups[row_group_idx].columns[column_idx].meta_data;
            auto col_schema = doris_file_metadata->schema().get_column(column_idx);
            ParquetPredicate::ColumnStat stat;
            ASSERT_TRUE(ParquetPredicate::read_column_stats(col_schema, column_meta_data, nullptr,
                                                            doris_metadata.created_by, &stat)
                                .ok());
            ASSERT_EQ(stat.has_null, arrow_reader->RowGroup(row_group_idx)
                                                     ->metadata()
                                                     ->ColumnChunk(column_idx)
                                                     ->statistics()
                                                     ->null_count() > 0);
            ASSERT_EQ(stat.is_all_null,
                      arrow_reader->RowGroup(row_group_idx)
                                      ->metadata()
                                      ->ColumnChunk(column_idx)
                                      ->statistics()
                                      ->null_count() ==
                              arrow_reader->RowGroup(row_group_idx)->metadata()->num_rows());
        }
    }
}

TEST_F(ParquetExprTest, test_ne) {
    auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt64>());
    auto fn_eq = MockFnCall::create("ne");
    auto const_val = std::make_shared<MockLiteral>(
            ColumnHelper::create_column_with_name<DataTypeInt64>({100}));

    fn_eq->add_child(slot_ref);
    fn_eq->add_child(const_val);
    fn_eq->_node_type = TExprNodeType::BINARY_PRED;
    fn_eq->_opcode = TExprOpcode::NE;
    slot_ref->_slot_id = 1;
    EXPECT_FALSE(fn_eq->is_constant());

    auto ctx = VExprContext::create_shared(fn_eq);
    ctx->_prepared = true;
    ctx->_opened = true;
    ASSERT_FALSE(p_reader->_check_expr_can_push_down(ctx->root()));
}

TEST_F(ParquetExprTest, test_eq) {
    auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt32>());
    auto fn_eq = MockFnCall::create("eq");
    auto const_val = std::make_shared<MockLiteral>(
            ColumnHelper::create_column_with_name<DataTypeInt32>({100}));

    fn_eq->add_child(slot_ref);
    fn_eq->add_child(const_val);
    fn_eq->_node_type = TExprNodeType::BINARY_PRED;
    fn_eq->_opcode = TExprOpcode::EQ;
    slot_ref->_slot_id = 1;
    slot_ref->_column_id = 1;
    EXPECT_FALSE(fn_eq->is_constant());

    auto ctx = VExprContext::create_shared(fn_eq);
    ctx->_prepared = true;
    ctx->_opened = true;
    ASSERT_TRUE(p_reader->_check_expr_can_push_down(ctx->root()));
}

TEST_F(ParquetExprTest, test_le) {
    auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt32>());
    auto fn_eq = MockFnCall::create("le");
    auto const_val = std::make_shared<MockLiteral>(
            ColumnHelper::create_column_with_name<DataTypeInt32>({100}));

    fn_eq->add_child(slot_ref);
    fn_eq->add_child(const_val);
    fn_eq->_node_type = TExprNodeType::BINARY_PRED;
    fn_eq->_opcode = TExprOpcode::LE;
    slot_ref->_slot_id = 1;
    slot_ref->_column_id = 1;
    EXPECT_FALSE(fn_eq->is_constant());

    auto ctx = VExprContext::create_shared(fn_eq);
    ctx->_prepared = true;
    ctx->_opened = true;
    ASSERT_TRUE(p_reader->_check_expr_can_push_down(ctx->root()));
}

TEST_F(ParquetExprTest, test_ge) {
    auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt32>());
    auto fn_eq = MockFnCall::create("ge");
    auto const_val = std::make_shared<MockLiteral>(
            ColumnHelper::create_column_with_name<DataTypeInt32>({100}));

    fn_eq->add_child(slot_ref);
    fn_eq->add_child(const_val);
    fn_eq->_node_type = TExprNodeType::BINARY_PRED;
    fn_eq->_opcode = TExprOpcode::GE;
    slot_ref->_slot_id = 1;
    slot_ref->_column_id = 1;
    EXPECT_FALSE(fn_eq->is_constant());

    auto ctx = VExprContext::create_shared(fn_eq);
    ctx->_prepared = true;
    ctx->_opened = true;
    ASSERT_TRUE(p_reader->_check_expr_can_push_down(ctx->root()));
}

TEST_F(ParquetExprTest, test_gt) {
    auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt32>());
    auto fn_eq = MockFnCall::create("gt");
    auto const_val = std::make_shared<MockLiteral>(
            ColumnHelper::create_column_with_name<DataTypeInt32>({100}));

    fn_eq->add_child(slot_ref);
    fn_eq->add_child(const_val);
    fn_eq->_node_type = TExprNodeType::BINARY_PRED;
    fn_eq->_opcode = TExprOpcode::GT;
    slot_ref->_slot_id = 1;
    slot_ref->_column_id = 1;
    EXPECT_FALSE(fn_eq->is_constant());

    auto ctx = VExprContext::create_shared(fn_eq);
    ctx->_prepared = true;
    ctx->_opened = true;
    ASSERT_TRUE(p_reader->_check_expr_can_push_down(ctx->root()));
}

TEST_F(ParquetExprTest, test_lt) {
    auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt32>());
    auto fn_eq = MockFnCall::create("lt");
    auto const_val = std::make_shared<MockLiteral>(
            ColumnHelper::create_column_with_name<DataTypeInt32>({100}));

    fn_eq->add_child(slot_ref);
    fn_eq->add_child(const_val);
    fn_eq->_node_type = TExprNodeType::BINARY_PRED;
    fn_eq->_opcode = TExprOpcode::LT;
    slot_ref->_slot_id = 1;
    slot_ref->_column_id = 1;
    EXPECT_FALSE(fn_eq->is_constant());

    auto ctx = VExprContext::create_shared(fn_eq);
    ctx->_prepared = true;
    ctx->_opened = true;
    ASSERT_TRUE(p_reader->_check_expr_can_push_down(ctx->root()));
}

TEST_F(ParquetExprTest, test_ge_2) { // int64_col = 10000000001   [10000000000 , 10000000000+3)
    int loc = 2;
    auto slot_ref = std::make_shared<MockSlotRef>(loc, std::make_shared<DataTypeInt64>());
    auto fn_eq = MockFnCall::create("eq");
    auto const_val = std::make_shared<MockLiteral>(
            ColumnHelper::create_column_with_name<DataTypeInt64>({10000000001}));
    slot_ref->set_expr_name("int64_col");
    fn_eq->add_child(slot_ref);
    fn_eq->add_child(const_val);
    fn_eq->_node_type = TExprNodeType::BINARY_PRED;
    fn_eq->_opcode = TExprOpcode::EQ;
    slot_ref->_slot_id = loc;
    slot_ref->_column_id = loc;
    EXPECT_FALSE(fn_eq->is_constant());

    auto ctx = VExprContext::create_shared(fn_eq);
    ctx->_prepared = true;
    ctx->_opened = true;
    ASSERT_TRUE(p_reader->_check_expr_can_push_down(ctx->root()));

    {
        const std::function<bool(const FieldSchema*, ParquetPredicate::ColumnStat*)>&
                get_stat_func =
                        [&](const FieldSchema*, ParquetPredicate::ColumnStat* stat) -> bool {
            const auto& column_meta_data = doris_metadata.row_groups[0].columns[loc].meta_data;
            auto col_schema = doris_file_metadata->schema().get_column(loc);
            if (!ParquetPredicate::read_column_stats(col_schema, column_meta_data, nullptr,
                                                     doris_metadata.created_by, stat)
                         .ok()) {
                return false;
            }
            return true;
        };
        ASSERT_FALSE(p_reader->_expr_push_down(ctx->root(), get_stat_func));
    }

    {
        const std::function<bool(const FieldSchema*, ParquetPredicate::ColumnStat*)>&
                get_stat_func =
                        [&](const FieldSchema*, ParquetPredicate::ColumnStat* stat) -> bool {
            const auto& column_meta_data = doris_metadata.row_groups[1].columns[loc].meta_data;
            auto col_schema = doris_file_metadata->schema().get_column(loc);
            if (!ParquetPredicate::read_column_stats(col_schema, column_meta_data, nullptr,
                                                     doris_metadata.created_by, stat)
                         .ok()) {
                return false;
            }
            return true;
        };
        ASSERT_TRUE(p_reader->_expr_push_down(ctx->root(), get_stat_func));
    }
}

TEST_F(ParquetExprTest, test_lt_2) { // string_col < name_1
    int loc = 5;
    auto slot_ref = std::make_shared<MockSlotRef>(loc, std::make_shared<DataTypeString>());
    auto fn_eq = MockFnCall::create("lt");
    auto const_val = std::make_shared<MockLiteral>(
            ColumnHelper::create_column_with_name<DataTypeString>({"name_1"}));
    slot_ref->set_expr_name("string_col");
    fn_eq->add_child(slot_ref);
    fn_eq->add_child(const_val);
    fn_eq->_node_type = TExprNodeType::BINARY_PRED;
    fn_eq->_opcode = TExprOpcode::LT;
    slot_ref->_slot_id = loc;
    slot_ref->_column_id = loc;
    EXPECT_FALSE(fn_eq->is_constant());

    auto ctx = VExprContext::create_shared(fn_eq);
    ctx->_prepared = true;
    ctx->_opened = true;
    ASSERT_TRUE(p_reader->_check_expr_can_push_down(ctx->root()));

    {
        const std::function<bool(const FieldSchema*, ParquetPredicate::ColumnStat*)>&
                get_stat_func = [](const FieldSchema*, ParquetPredicate::ColumnStat* stat) -> bool {
            stat->encoded_max_value = "name_5";
            stat->encoded_min_value = "name_4";
            stat->is_all_null = false;
            stat->has_null = false;
            return true;
        };
        ASSERT_TRUE(p_reader->_expr_push_down(ctx->root(), get_stat_func));
    }

    {
        const std::function<bool(const FieldSchema*, ParquetPredicate::ColumnStat*)>&
                get_stat_func = [](const FieldSchema*, ParquetPredicate::ColumnStat* stat) -> bool {
            stat->encoded_max_value = "name_5";
            stat->encoded_min_value = "name_0";
            stat->is_all_null = false;
            stat->has_null = false;
            return true;
        };
        ASSERT_FALSE(p_reader->_expr_push_down(ctx->root(), get_stat_func));
    }
}

TEST_F(ParquetExprTest, test_is_null) { // int32_all_null_col is null

    auto slot_ref = std::make_shared<MockSlotRef>(1, std::make_shared<DataTypeInt64>());
    auto fn_eq = MockFnCall::create("is_null_pred");
    auto const_val = std::make_shared<MockLiteral>(
            ColumnHelper::create_column_with_name<DataTypeInt64>({100}));

    fn_eq->add_child(slot_ref);
    fn_eq->add_child(const_val);
    fn_eq->_node_type = TExprNodeType::FUNCTION_CALL;
    slot_ref->_slot_id = 1;
    slot_ref->set_expr_name("int32_all_null_col");
    EXPECT_FALSE(fn_eq->is_constant());

    auto ctx = VExprContext::create_shared(fn_eq);
    ctx->_prepared = true;
    ctx->_opened = true;

    ASSERT_TRUE(p_reader->_check_expr_can_push_down(ctx->root()));

    {
        const std::function<bool(const FieldSchema*, ParquetPredicate::ColumnStat*)>&
                get_stat_func =
                        [&](const FieldSchema*, ParquetPredicate::ColumnStat* stat) -> bool {
            const auto& column_meta_data = doris_metadata.row_groups[0].columns[1].meta_data;
            auto col_schema = doris_file_metadata->schema().get_column(1);
            if (!ParquetPredicate::read_column_stats(col_schema, column_meta_data, nullptr,
                                                     doris_metadata.created_by, stat)
                         .ok()) {
                return false;
            }
            return true;
        };
        ASSERT_FALSE(p_reader->_expr_push_down(ctx->root(), get_stat_func));
    }

    {
        const std::function<bool(const FieldSchema*, ParquetPredicate::ColumnStat*)>&
                get_stat_func =
                        [&](const FieldSchema*, ParquetPredicate::ColumnStat* stat) -> bool {
            const auto& column_meta_data = doris_metadata.row_groups[1].columns[1].meta_data;
            auto col_schema = doris_file_metadata->schema().get_column(1);
            if (!ParquetPredicate::read_column_stats(col_schema, column_meta_data, nullptr,
                                                     doris_metadata.created_by, stat)
                         .ok()) {
                return false;
            }
            return true;
        };
        ASSERT_FALSE(p_reader->_expr_push_down(ctx->root(), get_stat_func));
    }
}

TEST_F(ParquetExprTest, test_is_not_null) { // int32_all_null_col is not null
    auto slot_ref = std::make_shared<MockSlotRef>(1, std::make_shared<DataTypeInt64>());
    auto fn_eq = MockFnCall::create("is_not_null_pred");
    auto const_val = std::make_shared<MockLiteral>(
            ColumnHelper::create_column_with_name<DataTypeInt64>({100}));

    fn_eq->add_child(slot_ref);
    fn_eq->add_child(const_val);
    fn_eq->_node_type = TExprNodeType::FUNCTION_CALL;
    slot_ref->_slot_id = 1;
    slot_ref->set_expr_name("int32_all_null_col");
    EXPECT_FALSE(fn_eq->is_constant());

    auto ctx = VExprContext::create_shared(fn_eq);
    ctx->_prepared = true;
    ctx->_opened = true;

    ASSERT_TRUE(p_reader->_check_expr_can_push_down(ctx->root()));

    {
        const std::function<bool(const FieldSchema*, ParquetPredicate::ColumnStat*)>&
                get_stat_func =
                        [&](const FieldSchema*, ParquetPredicate::ColumnStat* stat) -> bool {
            const auto& column_meta_data = doris_metadata.row_groups[0].columns[1].meta_data;
            auto col_schema = doris_file_metadata->schema().get_column(1);
            if (!ParquetPredicate::read_column_stats(col_schema, column_meta_data, nullptr,
                                                     doris_metadata.created_by, stat)
                         .ok()) {
                return false;
            }
            return true;
        };
        ASSERT_TRUE(p_reader->_expr_push_down(ctx->root(), get_stat_func));
    }

    {
        const std::function<bool(const FieldSchema*, ParquetPredicate::ColumnStat*)>&
                get_stat_func =
                        [&](const FieldSchema*, ParquetPredicate::ColumnStat* stat) -> bool {
            const auto& column_meta_data = doris_metadata.row_groups[1].columns[1].meta_data;
            auto col_schema = doris_file_metadata->schema().get_column(1);
            if (!ParquetPredicate::read_column_stats(col_schema, column_meta_data, nullptr,
                                                     doris_metadata.created_by, stat)
                         .ok()) {
                return false;
            }
            return true;
        };
        ASSERT_TRUE(p_reader->_expr_push_down(ctx->root(), get_stat_func));
    }
}

TEST_F(ParquetExprTest, test_is_null_2) { // int32_partial_null_col is null
    auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt64>());
    auto fn_eq = MockFnCall::create("is_null_pred");
    auto const_val = std::make_shared<MockLiteral>(
            ColumnHelper::create_column_with_name<DataTypeInt64>({100}));

    fn_eq->add_child(slot_ref);
    fn_eq->add_child(const_val);
    fn_eq->_node_type = TExprNodeType::FUNCTION_CALL;
    slot_ref->_slot_id = 0;
    slot_ref->set_expr_name("int32_partial_null_col");
    EXPECT_FALSE(fn_eq->is_constant());

    auto ctx = VExprContext::create_shared(fn_eq);
    ctx->_prepared = true;
    ctx->_opened = true;

    ASSERT_TRUE(p_reader->_check_expr_can_push_down(ctx->root()));

    {
        const std::function<bool(const FieldSchema*, ParquetPredicate::ColumnStat*)>&
                get_stat_func =
                        [&](const FieldSchema*, ParquetPredicate::ColumnStat* stat) -> bool {
            const auto& column_meta_data = doris_metadata.row_groups[0].columns[0].meta_data;
            auto col_schema = doris_file_metadata->schema().get_column(0);
            if (!ParquetPredicate::read_column_stats(col_schema, column_meta_data, nullptr,
                                                     doris_metadata.created_by, stat)
                         .ok()) {
                return false;
            }
            return true;
        };
        ASSERT_FALSE(p_reader->_expr_push_down(ctx->root(), get_stat_func));
    }

    {
        const std::function<bool(const FieldSchema*, ParquetPredicate::ColumnStat*)>&
                get_stat_func =
                        [&](const FieldSchema*, ParquetPredicate::ColumnStat* stat) -> bool {
            const auto& column_meta_data = doris_metadata.row_groups[1].columns[0].meta_data;
            auto col_schema = doris_file_metadata->schema().get_column(0);
            if (!ParquetPredicate::read_column_stats(col_schema, column_meta_data, nullptr,
                                                     doris_metadata.created_by, stat)
                         .ok()) {
                return false;
            }
            return true;
        };
        ASSERT_FALSE(p_reader->_expr_push_down(ctx->root(), get_stat_func));
    }
}

TEST_F(ParquetExprTest, test_min_max_p) {
    auto f = [&](int column_id, int row_group, Field* min_field, Field* max_field) {
        auto col_schema = doris_file_metadata->schema().get_column(column_id);
        const auto& column_meta_data =
                doris_metadata.row_groups[row_group].columns[column_id].meta_data;
        ParquetPredicate::ColumnStat stat;
        ASSERT_TRUE(ParquetPredicate::read_column_stats(col_schema, column_meta_data, nullptr,
                                                        doris_metadata.created_by, &stat)
                            .ok());

        ASSERT_TRUE(ParquetPredicate::get_min_max_value(col_schema, stat.encoded_min_value,
                                                        stat.encoded_max_value,
                                                        cctz::utc_time_zone(), min_field, max_field)
                            .ok());
    };

    {
        Field min_field;
        Field max_field;
        f(3, 0, &min_field, &max_field);

        auto col = ColumnHelper::create_column_with_name<DataTypeFloat64>({1.1f, 3.1f});

        std::cout << "min_field = " << min_field.get<float>() << "\n";
        std::cout << "max_field = " << max_field.get<float>() << "\n";

        std::cout << "min_field = " << min_field.get<double>() << "\n";
        std::cout << "max_field = " << max_field.get<double>() << "\n";

        Field ans_min = col.column->operator[](0);
        Field ans_max = col.column->operator[](1);
        ASSERT_EQ(ans_min, min_field);
        ASSERT_EQ(ans_max, max_field);
    }

    {
        Field min_field;
        Field max_field;
        f(6, 0, &min_field, &max_field);

        auto col = ColumnHelper::create_column_with_name<DataTypeInt64>({0, 1});

        Field ans_min = col.column->operator[](0);
        Field ans_max = col.column->operator[](1);
        ASSERT_EQ(ans_min, min_field);
        ASSERT_EQ(ans_max, max_field);
    }

    {
        Field min_field;
        Field max_field;
        f(7, 1, &min_field, &max_field);

        DateV2Value<DateV2ValueType> date1;
        std::string date_str1 = "2020-01-04";
        std::string format = "%Y-%m-%d";
        EXPECT_TRUE(date1.from_date_format_str(format.data(), format.size(), date_str1.data(),
                                               date_str1.size()));

        DateV2Value<DateV2ValueType> date2;
        std::string date_str2 = "2020-01-06";
        EXPECT_TRUE(date2.from_date_format_str(format.data(), format.size(), date_str2.data(),
                                               date_str2.size()));

        auto column = ColumnDateV2::create();
        auto& date_v2_data = column->get_data();
        date_v2_data.push_back(*reinterpret_cast<vectorized::UInt32*>(&date1));
        date_v2_data.push_back(*reinterpret_cast<vectorized::UInt32*>(&date2));

        Field ans_min = column->operator[](0);
        Field ans_max = column->operator[](1);
        ASSERT_EQ(ans_min, min_field);
        ASSERT_EQ(ans_max, max_field);
    }

    {
        Field min_field;
        Field max_field;
        f(8, 1, &min_field, &max_field);

        DateV2Value<DateTimeV2ValueType> datetime_v2_1;
        std::string origin_date = "2021-01-01 03:00:00.000";
        std::string date_format = "%Y-%m-%d %H:%i:%s.%f";
        EXPECT_TRUE(datetime_v2_1.from_date_format_str(date_format.data(), date_format.size(),
                                                       origin_date.data(), origin_date.size()));

        DateV2Value<DateTimeV2ValueType> datetime_v2_2;
        origin_date = "2021-01-01 05:00:00.000";
        EXPECT_TRUE(datetime_v2_2.from_date_format_str(date_format.data(), date_format.size(),
                                                       origin_date.data(), origin_date.size()));

        auto column = ColumnDateTimeV2::create();
        auto& date_v2_data = column->get_data();
        date_v2_data.push_back(*reinterpret_cast<vectorized::UInt64*>(&datetime_v2_1));
        date_v2_data.push_back(*reinterpret_cast<vectorized::UInt64*>(&datetime_v2_2));

        Field ans_min = column->operator[](0);
        Field ans_max = column->operator[](1);
        ASSERT_EQ(ans_min, min_field);
        ASSERT_EQ(ans_max, max_field);
    }

    {
        Field min_field;
        Field max_field;
        f(9, 0, &min_field, &max_field);

        Field ans_min = Field::create_field<TYPE_DECIMAL64>(DecimalField<Decimal64>(10000, 2));
        Field ans_max = Field::create_field<TYPE_DECIMAL64>(DecimalField<Decimal64>(10200, 2));
        ASSERT_EQ(ans_min, min_field);
        ASSERT_EQ(ans_max, max_field);
    }
    {
        Field min_field;
        Field max_field;
        f(10, 1, &min_field, &max_field);

        Field ans_min = Field::create_field<TYPE_DECIMAL64>(DecimalField<Decimal64>(1030000, 6));
        Field ans_max = Field::create_field<TYPE_DECIMAL64>(DecimalField<Decimal64>(1050000, 6));
        ASSERT_EQ(ans_min, min_field);
        ASSERT_EQ(ans_max, max_field);
    }
}

} // namespace vectorized
} // namespace doris
