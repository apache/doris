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

#include <cctz/time_zone.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stddef.h>

#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "exec/olap_common.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/file_meta_cache.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/timezone_utils.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/format/parquet/vparquet_reader.h"

namespace doris {
namespace vectorized {
class VExprContext;

class ParquetReaderTest : public testing::Test {
public:
    ParquetReaderTest() : cache(1024) {}

    FileMetaCache cache;
};

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

TEST_F(ParquetReaderTest, normal) {
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"boolean_col", "tinyint_col", "smallint_col",
                                                   "int_col",     "bigint_col",  "float_col",
                                                   "double_col"};
    std::vector<TPrimitiveType::type> table_column_types = {
            TPrimitiveType::BOOLEAN, TPrimitiveType::TINYINT, TPrimitiveType::SMALLINT,
            TPrimitiveType::INT,     TPrimitiveType::BIGINT,  TPrimitiveType::FLOAT,
            TPrimitiveType::DOUBLE};
    create_table_desc(t_desc_table, t_table_desc, table_column_names, table_column_types);
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    static_cast<void>(DescriptorTbl::create(&obj_pool, t_desc_table, &desc_tbl));

    auto slot_descs = desc_tbl->get_tuple_descriptor(0)->slots();
    auto local_fs = io::global_local_filesystem();
    io::FileReaderSPtr reader;
    static_cast<void>(
            local_fs->open_file("./be/test/exec/test_data/"
                                "parquet_scanner/type-decoder.parquet",
                                &reader));

    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
    auto tuple_desc = desc_tbl->get_tuple_descriptor(0);
    std::vector<std::string> column_names;
    for (int i = 0; i < slot_descs.size(); i++) {
        column_names.push_back(slot_descs[i]->col_name());
    }
    TFileScanRangeParams scan_params;
    TFileRangeDesc scan_range;
    {
        scan_range.start_offset = 0;
        scan_range.size = 1000;
    }
    auto p_reader = new ParquetReader(nullptr, scan_params, scan_range, 992, &ctz, nullptr, nullptr,
                                      &cache);
    p_reader->set_file_reader(reader);
    RuntimeState runtime_state((TQueryGlobals()));
    runtime_state.set_desc_tbl(desc_tbl);

    std::unordered_map<std::string, ColumnValueRangeType> colname_to_value_range;
    static_cast<void>(p_reader->init_reader(column_names, nullptr, {}, nullptr, nullptr, nullptr,
                                            nullptr, nullptr));
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;
    static_cast<void>(p_reader->set_fill_columns(partition_columns, missing_columns));
    BlockUPtr block = Block::create_unique();
    for (const auto& slot_desc : tuple_desc->slots()) {
        auto data_type =
                vectorized::DataTypeFactory::instance().create_data_type(slot_desc->type(), true);
        MutableColumnPtr data_column = data_type->create_column();
        block->insert(
                ColumnWithTypeAndName(std::move(data_column), data_type, slot_desc->col_name()));
    }
    bool eof = false;
    size_t read_row = 0;
    static_cast<void>(p_reader->get_next_block(block.get(), &read_row, &eof));
    for (auto& col : block->get_columns_with_type_and_name()) {
        ASSERT_EQ(col.column->size(), 10);
    }
    EXPECT_TRUE(eof);
    delete p_reader;
}

static ParquetReader* create_parquet_reader(TFileScanRangeParams& scan_params,
                                            std::vector<std::string> table_column_names,
                                            std::vector<TPrimitiveType::type> types) {
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;

    create_table_desc(t_desc_table, t_table_desc, table_column_names, types);
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    static_cast<void>(DescriptorTbl::create(&obj_pool, t_desc_table, &desc_tbl));

    auto slot_descs = desc_tbl->get_tuple_descriptor(0)->slots();
    auto local_fs = io::global_local_filesystem();
    io::FileReaderSPtr reader;
    static_cast<void>(
            local_fs->open_file("./be/test/exec/test_data/"
                                "parquet_scanner/type-decoder.parquet",
                                &reader));

    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
    std::vector<std::string> column_names;
    for (int i = 0; i < slot_descs.size(); i++) {
        column_names.push_back(slot_descs[i]->col_name());
    }
    TFileRangeDesc scan_range;
    {
        scan_range.start_offset = 0;
        scan_range.size = 1000;
    }
    auto p_reader =
            new ParquetReader(nullptr, scan_params, scan_range, 992, &ctz, nullptr, nullptr);
    p_reader->set_file_reader(reader);
    return p_reader;
}

TEST_F(ParquetReaderTest, use_column_name) {
    std::vector<std::string> table_column_names = {"boolean_col", "tinyint_col", "smallint_col",
                                                   "int_col",     "bigint_col",  "float_col",
                                                   "double_col"};
    std::vector<TPrimitiveType::type> table_column_types = {
            TPrimitiveType::BOOLEAN, TPrimitiveType::TINYINT, TPrimitiveType::SMALLINT,
            TPrimitiveType::INT,     TPrimitiveType::BIGINT,  TPrimitiveType::FLOAT,
            TPrimitiveType::DOUBLE};
    TFileScanRangeParams scan_params;

    auto p_reader = create_parquet_reader(scan_params, table_column_names, table_column_types);
    std::unordered_map<std::string, ColumnValueRangeType> colname_to_value_range;
    colname_to_value_range.emplace("boolean_col", ColumnValueRange<TYPE_BOOLEAN>("boolean_col"));
    colname_to_value_range.emplace("tinyint_col", ColumnValueRange<TYPE_TINYINT>("tinyint_col"));
    colname_to_value_range.emplace("smallint_col", ColumnValueRange<TYPE_SMALLINT>("smallint_col"));
    colname_to_value_range.emplace("int_col", ColumnValueRange<TYPE_INT>("int_col"));

    static_cast<void>(p_reader->init_reader(
            table_column_names, &colname_to_value_range, {}, nullptr, nullptr, nullptr, nullptr,
            nullptr, TableSchemaChangeHelper::ConstNode::get_instance(), false));

    std::vector<std::string> read_columns_ans = {"tinyint_col", "smallint_col", "int_col",
                                                 "bigint_col",  "boolean_col",  "float_col",
                                                 "double_col"};
    EXPECT_EQ(p_reader->_read_file_columns, read_columns_ans);

    std::vector<std::string> miss_columns_ans = {};
    EXPECT_EQ(p_reader->_missing_cols, miss_columns_ans);
    std::vector<std::string> colname_to_value_range_names_ans = {"tinyint_col", "smallint_col",
                                                                 "int_col", "boolean_col"};
    for (auto col : colname_to_value_range_names_ans) {
        EXPECT_TRUE(p_reader->_colname_to_value_range->contains(col));
    }
    EXPECT_EQ(p_reader->_colname_to_value_range->size(), colname_to_value_range_names_ans.size());
    delete p_reader;
}

} // namespace vectorized
} // namespace doris
