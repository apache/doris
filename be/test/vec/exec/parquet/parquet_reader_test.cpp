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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "io/local_file_reader.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/format/parquet/vparquet_reader.h"

namespace doris {
namespace vectorized {

class ParquetReaderTest : public testing::Test {
public:
    ParquetReaderTest() {}
};

TEST_F(ParquetReaderTest, normal) {
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;

    t_table_desc.id = 0;
    t_table_desc.tableType = TTableType::OLAP_TABLE;
    t_table_desc.numCols = 0;
    t_table_desc.numClusteringCols = 0;
    t_desc_table.tableDescriptors.push_back(t_table_desc);
    t_desc_table.__isset.tableDescriptors = true;

    // init boolean and numeric slot
    std::vector<std::string> numeric_types = {"boolean_col", "tinyint_col", "smallint_col",
                                              "int_col",     "bigint_col",  "float_col",
                                              "double_col"};
    for (int i = 0; i < numeric_types.size(); i++) {
        TSlotDescriptor tslot_desc;
        {
            tslot_desc.id = i;
            tslot_desc.parent = 0;
            TTypeDesc type;
            {
                TTypeNode node;
                node.__set_type(TTypeNodeType::SCALAR);
                TScalarType scalar_type;
                scalar_type.__set_type(TPrimitiveType::type(i + 2));
                node.__set_scalar_type(scalar_type);
                type.types.push_back(node);
            }
            tslot_desc.slotType = type;
            tslot_desc.columnPos = 0;
            tslot_desc.byteOffset = 0;
            tslot_desc.nullIndicatorByte = 0;
            tslot_desc.nullIndicatorBit = -1;
            tslot_desc.colName = numeric_types[i];
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
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    DescriptorTbl::create(&obj_pool, t_desc_table, &desc_tbl);

    auto slot_descs = desc_tbl->get_tuple_descriptor(0)->slots();
    LocalFileReader* reader =
            new LocalFileReader("./be/test/exec/test_data/parquet_scanner/type-decoder.parquet", 0);

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
    auto p_reader = new ParquetReader(nullptr, scan_params, scan_range, 992, &ctz);
    p_reader->set_file_reader(reader);
    RuntimeState runtime_state((TQueryGlobals()));
    runtime_state.set_desc_tbl(desc_tbl);
    runtime_state.init_mem_trackers();

    std::unordered_map<std::string, ColumnValueRangeType> colname_to_value_range;
    p_reader->init_reader(column_names, nullptr, nullptr);
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContext*> missing_columns;
    p_reader->set_fill_columns(partition_columns, missing_columns);
    Block* block = new Block();
    for (const auto& slot_desc : tuple_desc->slots()) {
        auto data_type =
                vectorized::DataTypeFactory::instance().create_data_type(slot_desc->type(), true);
        MutableColumnPtr data_column = data_type->create_column();
        block->insert(
                ColumnWithTypeAndName(std::move(data_column), data_type, slot_desc->col_name()));
    }
    bool eof = false;
    size_t read_row = 0;
    p_reader->get_next_block(block, &read_row, &eof);
    for (auto& col : block->get_columns_with_type_and_name()) {
        ASSERT_EQ(col.column->size(), 10);
    }
    EXPECT_TRUE(eof);
    delete block;
    delete p_reader;
}

} // namespace vectorized
} // namespace doris
