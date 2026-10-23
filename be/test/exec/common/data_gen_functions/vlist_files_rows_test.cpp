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

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gtest/gtest.h>

#include <array>

#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exec/common/data_gen_functions/vlist_file_tvf.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace doris {

TEST(ListFilesRowsTest, DirectoryNullsSizesAndProjectionsAcrossBatches) {
    const std::array<std::string, 3> names {"path", "size", "file"};
    const std::array<DataTypePtr, 3> types {std::make_shared<DataTypeString>(),
                                          std::make_shared<DataTypeInt64>(),
                                          std::make_shared<DataTypeFile>()};
    const std::vector<std::vector<size_t>> projections {
            {0, 1, 2}, {0}, {1}, {2}, {0, 1}, {0, 2}, {1, 2}, {2, 0}, {2, 1, 0}};
    for (bool recursive : {false, true}) {
        for (const auto& projection : projections) {
            TTupleDescriptor tuple_spec;
            tuple_spec.__set_id(0);
            TupleDescriptor tuple(tuple_spec, true);
            for (const auto index : projection) {
                TSlotDescriptor slot;
                slot.__set_id(static_cast<int>(index));
                slot.__set_parent(0);
                slot.__set_colName(names[index]);
                slot.__set_slotType(types[index]->to_thrift());
                slot.__set_nullIndicatorBit(index == 2 ? 0 : -1);
                tuple.add_slot(new SlotDescriptor(slot));
            }
            VListFileTVF function(0, &tuple);
            TTVFListFileScanRange params;
            params.__set_uri("s3://bucket/a/");
            params.__set_recursive(recursive);
            TScanRangeParams range;
            range.scan_range.data_gen_scan_range.__set_list_file_params(params);
            ASSERT_TRUE(function.set_scan_ranges({range}).ok());
            function._objects = {{.key = "a/"},
                                 {.key = "a/empty.txt"},
                                 {.key = "a/b/"},
                                 {.key = "a/c.png", .size = 3},
                                 {.key = "a/marker/", .size = 7}};
            function._has_more = false;
            RuntimeState state;
            state._query_options.__set_batch_size(1);
            Block block;
            bool eos = false;
            size_t rows = 0;
            while (!eos) {
                ASSERT_TRUE(function.get_next(&state, &block, &eos).ok());
                for (size_t row = 0; row < block.rows(); ++row) {
                    const bool directory = !recursive && (rows == 1 || rows == 3);
                    const int64_t size = rows == (recursive ? 1U : 2U) ? 3 : 0;
                    for (size_t column = 0; column < projection.size(); ++column) {
                        const auto field = (*block.get_by_position(column).column)[row];
                        switch (projection[column]) {
                        case 0:
                            EXPECT_EQ(field.get<TYPE_STRING>().ends_with('/'), directory);
                            break;
                        case 1:
                            EXPECT_EQ(field.get<TYPE_BIGINT>(), size);
                            break;
                        case 2:
                            EXPECT_EQ(field.is_null(), directory);
                            if (!directory) {
                                EXPECT_EQ(field.get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), size);
                            }
                            break;
                        }
                    }
                    ++rows;
                }
                block.clear_column_data();
            }
            EXPECT_EQ(rows, recursive ? 2 : 4);
            function._objects.clear();
            function._next_index = 0;
            eos = false;
            Block empty;
            ASSERT_TRUE(function.get_next(&state, &empty, &eos).ok());
            EXPECT_EQ(empty.rows(), 0);
            EXPECT_EQ(empty.columns(), projection.size());
            for (size_t column = 0; column < projection.size(); ++column) {
                EXPECT_EQ(empty.get_by_position(column).name, names[projection[column]]);
                EXPECT_EQ(empty.get_by_position(column).type->is_nullable(),
                          projection[column] == 2);
            }
            EXPECT_TRUE(eos);
        }
    }
}

} // namespace doris
