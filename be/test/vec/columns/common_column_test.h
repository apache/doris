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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include "olap/schema.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/columns_number.h"
#include "vec/core/field.h"
#include "vec/core/sort_block.h"
#include "vec/core/sort_description.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

// this test is gonna to be a column test template for all column which should make ut test to coverage the function defined in column
// for example column_array should test this function:
// size, reserve, resize, empty, byte_size, allocated_bytes, clone_resized,
// get_shrinked_column, filter, filter_by_selector, serialize_vec, deserialize_vec, get_max_row_byte_size
//
namespace doris::vectorized {

class CommonColumnTest : public ::testing::Test {
public:
    void SetUp() override {
        col_str = ColumnString::create();
        col_str->insert_data("aaa", 3);
        col_str->insert_data("bb", 2);
        col_str->insert_data("cccc", 4);

        col_int = ColumnInt64::create();
        col_int->insert_value(1);
        col_int->insert_value(2);
        col_int->insert_value(3);

        col_dcm = ColumnDecimal64::create(0, 3);
        col_dcm->insert_value(1.23);
        col_dcm->insert_value(4.56);
        col_dcm->insert_value(7.89);

        col_arr = ColumnArray::create(ColumnInt64::create(), ColumnArray::ColumnOffsets::create());
        Array array1 = {1, 2, 3};
        Array array2 = {4};
        col_arr->insert(array1);
        col_arr->insert(Array());
        col_arr->insert(array2);

        col_map = ColumnMap::create(ColumnString::create(), ColumnInt64::create(),
                                    ColumnArray::ColumnOffsets::create());
        Array k1 = {"a", "b", "c"};
        Array v1 = {1, 2, 3};
        Array k2 = {"d"};
        Array v2 = {4};
        Array a = Array();
        Map map1, map2, map3;
        map1.push_back(k1);
        map1.push_back(v1);
        col_map->insert(map1);
        map3.push_back(a);
        map3.push_back(a);
        col_map->insert(map3);
        map2.push_back(k2);
        map2.push_back(v2);
        col_map->insert(map2);
    }

public:
    ColumnString::MutablePtr col_str;
    ColumnInt64::MutablePtr col_int;
    ColumnDecimal64::MutablePtr col_dcm;
    ColumnArray::MutablePtr col_arr;
    ColumnMap::MutablePtr col_map;

    void checkColumn(const IColumn& col1, const IColumn& col2, const IDataType& dataType,
                     size_t column_size) {
        if (WhichDataType(dataType).is_map()) {
            auto map1 = check_and_get_column<ColumnMap>(col1);
            auto map2 = check_and_get_column<ColumnMap>(col2);
            const DataTypeMap& rhs_map = static_cast<const DataTypeMap&>(dataType);
            checkColumn(map1->get_keys(), map2->get_keys(), *rhs_map.get_key_type(),
                        map1->get_keys().size());
            checkColumn(map2->get_values(), map2->get_values(), *rhs_map.get_value_type(),
                        map1->get_values().size());
        } else {
            if (WhichDataType(dataType).is_int8()) {
                auto c1 = check_and_get_column<ColumnInt8>(col1);
                auto c2 = check_and_get_column<ColumnInt8>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_int16()) {
                auto c1 = check_and_get_column<ColumnInt16>(col1);
                auto c2 = check_and_get_column<ColumnInt16>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_int32()) {
                auto c1 = check_and_get_column<ColumnInt32>(col1);
                auto c2 = check_and_get_column<ColumnInt32>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_int64()) {
                auto c1 = check_and_get_column<ColumnInt64>(col1);
                auto c2 = check_and_get_column<ColumnInt64>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_int128()) {
                auto c1 = check_and_get_column<ColumnInt128>(col1);
                auto c2 = check_and_get_column<ColumnInt128>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_float32()) {
                auto c1 = check_and_get_column<ColumnFloat32>(col1);
                auto c2 = check_and_get_column<ColumnFloat32>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_float64()) {
                auto c1 = check_and_get_column<ColumnFloat64>(col1);
                auto c2 = check_and_get_column<ColumnFloat64>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_uint8()) {
                auto c1 = check_and_get_column<ColumnUInt8>(col1);
                auto c2 = check_and_get_column<ColumnUInt8>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_uint16()) {
                auto c1 = check_and_get_column<ColumnUInt16>(col1);
                auto c2 = check_and_get_column<ColumnUInt16>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_uint32()) {
                auto c1 = check_and_get_column<ColumnUInt32>(col1);
                auto c2 = check_and_get_column<ColumnUInt32>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_uint64()) {
                auto c1 = check_and_get_column<ColumnUInt64>(col1);
                auto c2 = check_and_get_column<ColumnUInt64>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_decimal()) {
                auto c1 = check_and_get_column<ColumnDecimal64>(col1);
                auto c2 = check_and_get_column<ColumnDecimal64>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_decimal32()) {
                auto c1 = check_and_get_column<ColumnDecimal32>(col1);
                auto c2 = check_and_get_column<ColumnDecimal32>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_decimal64()) {
                auto c1 = check_and_get_column<ColumnDecimal64>(col1);
                auto c2 = check_and_get_column<ColumnDecimal64>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_decimal128v2()) {
                auto c1 = check_and_get_column<ColumnDecimal128V2>(col1);
                auto c2 = check_and_get_column<ColumnDecimal128V2>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_decimal128v3()) {
                auto c1 = check_and_get_column<ColumnDecimal128V3>(col1);
                auto c2 = check_and_get_column<ColumnDecimal128V3>(col2);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else if (WhichDataType(dataType).is_decimal256()) {
                auto c1 = check_and_get_column<ColumnDecimal<Decimal256>>(col1);
                auto c2 = check_and_get_column<ColumnDecimal<Decimal256>>(col1);
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(c1->get_element(i), c2->get_element(i));
                }
            } else {
                for (size_t i = 0; i < column_size; ++i) {
                    EXPECT_EQ(col1.get_data_at(i), col2.get_data_at(i));
                }
            }
        }
    }

    void printColumn(const IColumn& column, const IDataType& dataType) {
        std::cout << "column total size: " << column.size() << std::endl;
        if (WhichDataType(dataType).is_map()) {
            auto map = check_and_get_column<ColumnMap>(column);
            std::cout << "map {keys, values}" << std::endl;
            const DataTypeMap& rhs_map = static_cast<const DataTypeMap&>(dataType);
            printColumn(map->get_keys(), *rhs_map.get_key_type());
            printColumn(map->get_values(), *rhs_map.get_value_type());
        } else if (WhichDataType(dataType).is_array()) {
            auto array = check_and_get_column<ColumnArray>(column);
            std::cout << "array: " << std::endl;
            const DataTypeArray& rhs_array = static_cast<const DataTypeArray&>(dataType);
            printColumn(array->get_data(), *rhs_array.get_nested_type());
        } else {
            size_t column_size = column.size();
            std::cout << column.get_name() << ": " << std::endl;
            if (WhichDataType(dataType).is_int8()) {
                auto col = check_and_get_column<ColumnInt8>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_int16()) {
                auto col = check_and_get_column<ColumnInt16>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_int32()) {
                auto col = check_and_get_column<ColumnInt32>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_int64()) {
                auto col = check_and_get_column<ColumnInt64>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_int128()) {
                auto col = check_and_get_column<ColumnInt128>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_float32()) {
                auto col = check_and_get_column<ColumnFloat32>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_float64()) {
                auto col = check_and_get_column<ColumnFloat64>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_uint8()) {
                auto col = check_and_get_column<ColumnUInt8>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_uint16()) {
                auto col = check_and_get_column<ColumnUInt16>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_uint32()) {
                auto col = check_and_get_column<ColumnUInt32>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_uint64()) {
                auto col = check_and_get_column<ColumnUInt64>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_uint128()) {
                auto col = check_and_get_column<ColumnUInt128>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_data_at(i) << " ";
                }
            } else if (WhichDataType(dataType).is_decimal()) {
                auto col = check_and_get_column<ColumnDecimal64>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_decimal32()) {
                auto col = check_and_get_column<ColumnDecimal32>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_decimal64()) {
                auto col = check_and_get_column<ColumnDecimal64>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_decimal128v2()) {
                auto col = check_and_get_column<ColumnDecimal128V2>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_decimal128v3()) {
                auto col = check_and_get_column<ColumnDecimal128V3>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_decimal256()) {
                auto col = check_and_get_column<ColumnDecimal<Decimal256>>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_element(i) << " ";
                }
            } else if (WhichDataType(dataType).is_date()) {
                auto col = check_and_get_column<ColumnDate>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_data_at(i) << " ";
                }
            } else if (WhichDataType(dataType).is_date_time()) {
                auto col = check_and_get_column<ColumnDateTime>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_data_at(i) << " ";
                }
            } else if (WhichDataType(dataType).is_date_v2()) {
                auto col = check_and_get_column<ColumnDateV2>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_data_at(i) << " ";
                }
            } else if (WhichDataType(dataType).is_date_time_v2()) {
                auto col = check_and_get_column<ColumnDateTimeV2>(column);
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << col->get_data_at(i) << " ";
                }
            } else {
                std::cout << "data type: " << dataType.get_name() << std::endl;
                std::cout << "column type: " << column.get_name() << std::endl;
                for (size_t i = 0; i < column_size; ++i) {
                    std::cout << column.get_data_at(i).to_string() << " ";
                }
            }
            std::cout << std::endl;
        }
    }
    // column size changed calculation:
    //  size, reserve, resize, empty, byte_size, allocated_bytes, clone_resized, get_shrinked_column
    //  cut(LIMIT operation), shrink
    void sizeAssert(MutableColumnPtr col, size_t expect_size) {
        EXPECT_EQ(col->size(), expect_size);
    }

    // empty just use size() == 0 to impl as default behavior
    void emptyAssert(MutableColumnPtr col) { EXPECT_EQ(col->size(), 0); }

    // reserve, resize, byte_size, allocated_bytes, clone_resized, get_shrinked_column
    void reserveAssert(MutableColumnPtr col, size_t expect_size) {
        col->reserve(expect_size);
        EXPECT_EQ(col->allocated_bytes(), expect_size);
    }

    //  cut(LIMIT operation) will cut the column with the given from and to, and return the new column
    //  notice return column is clone from origin column
    void cutAssert(MutableColumnPtr col, size_t from, size_t to, size_t expect_size) {
        auto ori = col->size();
        auto ptr = col->cut(from, to);
        EXPECT_EQ(ptr->size(), expect_size);
        EXPECT_EQ(col->size(), ori);
    }

    // shrink is cut/append the column with the given size, which called from Block::set_num_rows
    // and some Operator may call this set_num_rows to make rows satisfied, like limit operation
    // but different from cut behavior which
    // return column is mutate from origin column
    void shrinkAssert(MutableColumnPtr col, size_t shrink_size) {
        auto ptr = col->shrink(shrink_size);
        EXPECT_EQ(ptr->size(), shrink_size);
        EXPECT_EQ(col->size(), shrink_size);
    }

    // resize has fixed-column implementation and variable-column implementation
    // like string column, the resize will resize the offsets column but not the data column (because it doesn't matter the size of data column, all operation for string column is based on the offsets column)
    // like vector column, the resize will resize the data column
    // like array column, the resize will resize the offsets column and the data column (which in creator we have check staff for the size of data column is the same as the size of offsets column)
    void resizeAssert(MutableColumnPtr col, size_t expect_size) {
        col->resize(expect_size);
        EXPECT_EQ(col->size(), expect_size);
    }

    // replicate is clone with new column from the origin column, always from ColumnConst to expand the column
    void replicateAssert(MutableColumnPtr col, IColumn::Offsets& offsets) {
        auto new_col = col->replicate(offsets);
        EXPECT_EQ(new_col->size(), offsets.back());
    }

    // byte size is just appriximate size of the column
    //  as fixed column type, like column_vector, the byte size is sizeof(columnType) * size()
    //  as variable column type, like column_string, the byte size is sum of chars size() and offsets size * sizeof(offsetType)
    void byteSizeAssert(MutableColumnPtr col, size_t expect_size) {
        EXPECT_EQ(col->byte_size(), expect_size);
    }

    // allocated bytes is the real size of the column
    void allocatedBytesAssert(MutableColumnPtr col, size_t expect_size) {
        EXPECT_EQ(col->allocated_bytes(), expect_size);
    }

    // clone_resized will clone the column and cut/append to the new column with the size of the original column
    void cloneResizedAssert(MutableColumnPtr col, size_t expect_size) {
        auto new_col = col->clone_resized(expect_size);
        EXPECT_EQ(new_col->size(), expect_size);
    }

    // get_shrinked_column should only happened in char-type column or nested char-type column
    //  just shrink the end zeros for char-type column which happened in segmentIterator
    //    eg. column_desc: char(6), insert into char(3), the char(3) will padding the 3 zeros at the end for writing to disk.
    //       but we select should just print the char(3) without the padding zeros
    //  limit and topN operation will trigger this function call
    void getShrinkedColumnAssert(MutableColumnPtr col, size_t spcific_size_defined) {
        EXPECT_TRUE(col->could_shrinked_column());
        auto new_col = col->get_shrinked_column();
        for (size_t i = 0; i < new_col->size(); i++) {
            EXPECT_EQ(col->get_data_at(i).size, spcific_size_defined);
        }
    }

    //serialize and deserialize which usually used in AGG function:
    //  serialize_value_into_arena, deserialize_and_insert_from_arena (called by AggregateFunctionDistinctMultipleGenericData, group_array_intersect, nested-types serder like: DataTypeArraySerDe::write_one_cell_to_jsonb)
    void ser_deserialize_with_arena_impl(MutableColumns& columns, const DataTypes& data_types) {
        /// check serialization is reversible.
        Arena arena;
        MutableColumns argument_columns(data_types.size());
        const char* pos = nullptr;
        StringRef key(pos, 0);
        {
            // serialize
            for (size_t r = 0; r < columns[0]->size(); ++r) {
                for (size_t i = 0; i < columns.size(); ++i) {
                    auto cur_ref = columns[i]->serialize_value_into_arena(r, arena, pos);
                    key.data = cur_ref.data - key.size;
                    key.size += cur_ref.size;
                    printColumn(*columns[i], *data_types[i]);
                }
            }
        }

        {
            // deserialize
            for (size_t i = 0; i < data_types.size(); ++i) {
                argument_columns[i] = data_types[i]->create_column();
            }
            const char* begin = key.data;
            for (size_t r = 0; r < columns[0]->size(); ++r) {
                for (size_t i = 0; i < argument_columns.size(); ++i) {
                    begin = argument_columns[i]->deserialize_and_insert_from_arena(begin);
                    printColumn(*argument_columns[i], *data_types[i]);
                }
            }
            //            for (size_t i = 0; i < argument_columns.size(); ++i) {
            //                auto& column = argument_columns[i];
            //                begin = column->deserialize_and_insert_from_arena(begin);
            //                printColumn(*column, *data_types[i]);
            //            }
        }
        {
            // check column data equal
            for (size_t i = 0; i < columns.size(); ++i) {
                EXPECT_EQ(columns[i]->size(), argument_columns[i]->size());
                checkColumn(*columns[i], *argument_columns[i], *data_types[i], columns[0]->size());
            }
        }
    }

    //  serialize_vec, deserialize_vec (called by MethodSerialized.init_serialized_keys), here are some scenarios:
    //    1/ AggState: groupby key column which be serialized to hash-table key, eg.AggLocalState::_emplace_into_hash_table
    //    2/ JoinState: hash join key column which be serialized to hash-table key, or probe column which be serialized to hash-table key, eg.ProcessHashTableBuild, ProcessHashTableProbe<JoinOpType>::probe_side_output_column
    //  serialize_vec_with_null_map, deserialize_vec_with_null_map which only called by ColumnNullable serialize_vec and deserialize_vec, and derived by other columns
    //  get_max_row_byte_size used in MethodSerialized which calculating the memory size for vectorized serialization of aggregation keys.
    void ser_deser_vec(MutableColumns& columns, DataTypes dataTypes) {
        // step1. make input_keys with given rows for a block
        size_t rows = columns[0]->size();
        std::vector<StringRef> input_keys;
        input_keys.resize(rows);
        MutableColumns check_columns(columns.size());
        int c = 0;
        for (auto& column : columns) {
            check_columns[c] = column->clone_empty();
            ++c;
        }

        // step2. calculate the needed memory size for vectorized serialization of aggregation keys
        size_t max_one_row_byte_size = 0;
        for (const auto& column : columns) {
            max_one_row_byte_size += column->get_max_row_byte_size();
        }
        size_t memory_size = max_one_row_byte_size * rows;
        Arena arena(memory_size);
        auto* serialized_key_buffer = reinterpret_cast<uint8_t*>(arena.alloc(memory_size));

        // serialize the keys into arena
        {
            // step3. serialize the keys into arena
            for (size_t i = 0; i < rows; ++i) {
                input_keys[i].data =
                        reinterpret_cast<char*>(serialized_key_buffer + i * max_one_row_byte_size);
                input_keys[i].size = 0;
                std::cout << "input:" << input_keys[i].to_string() << std::endl;
            }
            for (const auto& column : columns) {
                column->serialize_vec(input_keys, rows, max_one_row_byte_size);
            }
        }
        // deserialize the keys from arena into columns
        {
            // step4. deserialize the keys from arena into columns
            for (auto& column : check_columns) {
                column->deserialize_vec(input_keys, rows);
            }
        }
        // check the deserialized columns
        {
            for (size_t i = 0; i < columns.size(); ++i) {
                EXPECT_EQ(columns[i]->size(), check_columns[i]->size());
                checkColumn(*columns[i], *check_columns[i], *dataTypes[i], rows);
            }
        }
    }

    PaddedPODArray<UInt8> create_filter(std::vector<uint8_t> data) {
        PaddedPODArray<UInt8> filter;
        filter.insert(filter.end(), data.begin(), data.end());
        return filter;
    }

    // filter calculation:
    //  filter (called in Block::filter_block_internal to filter data with filter filled with 0or1 array, like: [0,1,0,1])
    //   used in join to filter next block by row_ids, and filter column by row_ids in first read which called in SegmentIterator
    void filterAssert(MutableColumnPtr col, std::vector<uint8_t> filter, size_t expect_size) {
        EXPECT_EQ(col->size(), filter.size());
        auto filted_col = col->filter(create_filter(filter), expect_size);
        EXPECT_EQ(filted_col->size(), expect_size);
        int iter = 0;
        for (size_t i = 0; i < filter.size(); i++) {
            if (filter[i]) {
                EXPECT_EQ(col->get_data_at(i), filted_col->get_data_at(iter));
                iter++;
            }
        }
    }

    //  filter_by_selector (called SegmentIterator::copy_column_data_by_selector,
    //  now just used in filter column, according to the selector to
    //  select size of row_ids for column by given column, which only used for predict_column and column_dictionary, column_nullable sometimes in Schema::get_predicate_column_ptr() also will return)
    void filterBySelectorAssert(vectorized::IColumn::MutablePtr col, std::vector<uint16_t> selector,
                                const IDataType& dt, MutableColumnPtr should_sel_col,
                                size_t expect_size) {
        // only used in column_nullable and predict_column, column_dictionary
        EXPECT_TRUE(col->is_nullable() || col->is_column_dictionary() ||
                    col->is_predicate_column());
        // for every data type should assert behavior in own UT case
        col->clear();
        col->insert_many_defaults(should_sel_col->size());
        std::cout << "col size:" << col->size() << std::endl;
        auto sel = should_sel_col->clone_empty();
        Status st = col->filter_by_selector(selector.data(), expect_size, sel);
        EXPECT_EQ(st, Status::OK());
        EXPECT_EQ(sel->size(), expect_size);
        printColumn(*sel, dt);
    }

    void assertPermutationsWithLimit(const IColumn::Permutation& lhs,
                                     const IColumn::Permutation& rhs, size_t limit) {
        if (limit == 0) {
            limit = lhs.size();
        }

        for (size_t i = 0; i < limit; ++i) {
            ASSERT_EQ(lhs[i], rhs[i]);
        }
    }

    // this function is common function to produce column, according to ColumnValueGetter
    // this range_size can be set, and it is helpfully make common column data which can be inserted into columns.
    template <typename ColumnValueGetter>
    void generateRanges(std::vector<std::vector<Field>>& ranges, size_t range_size,
                        ColumnValueGetter getter) {
        for (auto& range : ranges) {
            range.clear();
        }

        size_t ranges_size = ranges.size();

        for (size_t range_index = 0; range_index < ranges_size; ++range_index) {
            for (size_t index_in_range = 0; index_in_range < range_size; ++index_in_range) {
                auto value = getter(range_index, index_in_range);
                ranges[range_index].emplace_back(value);
            }
        }
    }

    void insertRangesIntoColumn(std::vector<std::vector<Field>>& ranges,
                                IColumn::Permutation& ranges_permutations,
                                vectorized::IColumn& column) {
        for (const auto& range_permutation : ranges_permutations) {
            auto& range = ranges[range_permutation];
            for (auto& value : range) {
                column.insert(value);
            }
        }
    }

    // this function helps to check sort permutation behavior for column
    void stableGetColumnPermutation(const IColumn& column, bool ascending, size_t limit,
                                    int nan_direction_hint, IColumn::Permutation& out_permutation) {
        (void)(limit);

        size_t size = column.size();
        out_permutation.resize(size);
        std::iota(out_permutation.begin(), out_permutation.end(),
                  IColumn::Permutation::value_type(0));

        std::stable_sort(out_permutation.begin(), out_permutation.end(),
                         [&](size_t lhs, size_t rhs) {
                             int res = column.compare_at(lhs, rhs, column, nan_direction_hint);
                             // to check element in column is sorted or not
                             if (ascending)
                                 return res < 0;
                             else
                                 return res > 0;
                         });
    }

    // sort calculation: (which used in sort_block )
    //   get_permutation
    // this function helps check permutation result with sort & limit
    //  by given a ColumnCreateFunc and ColumnValueGetter which how to generate a column value
    template <typename ColumnCreateFunc, typename ColumnValueGetter>
    void assertColumnPermutations(vectorized::IColumn& column,
                                  ColumnValueGetter columnValueGetter) {
        static constexpr size_t ranges_size = 3;
        static const std::vector<size_t> range_sizes = {1, 5, 50, 500};

        std::vector<std::vector<Field>> ranges(ranges_size);
        std::vector<size_t> ranges_permutations(ranges_size);
        std::iota(ranges_permutations.begin(), ranges_permutations.end(),
                  IColumn::Permutation::value_type(0));

        IColumn::Permutation actual_permutation;
        IColumn::Permutation expected_permutation;

        for (const auto& range_size : range_sizes) {
            // step1. generate range field data for column
            generateRanges(ranges, range_size, columnValueGetter);
            std::sort(ranges_permutations.begin(), ranges_permutations.end());
            IColumn::Permutation permutation(ranges_permutations.size());
            for (size_t i = 0; i < ranges_permutations.size(); ++i) {
                permutation[i] = ranges_permutations[i];
            }

            while (true) {
                // step2. insert range field data into column
                insertRangesIntoColumn(ranges, permutation, column);
                static constexpr size_t limit_parts = 4;

                size_t column_size = column.size();
                size_t column_limit_part = (column_size / limit_parts) + 1;

                for (size_t limit = 0; limit < column_size; limit += column_limit_part) {
                    assertColumnPermutation(column, true, limit, -1, actual_permutation,
                                            expected_permutation);
                    assertColumnPermutation(column, true, limit, 1, actual_permutation,
                                            expected_permutation);

                    assertColumnPermutation(column, false, limit, -1, actual_permutation,
                                            expected_permutation);
                    assertColumnPermutation(column, false, limit, 1, actual_permutation,
                                            expected_permutation);
                }

                assertColumnPermutation(column, true, 0, -1, actual_permutation,
                                        expected_permutation);
                assertColumnPermutation(column, true, 0, 1, actual_permutation,
                                        expected_permutation);

                assertColumnPermutation(column, false, 0, -1, actual_permutation,
                                        expected_permutation);
                assertColumnPermutation(column, false, 0, 1, actual_permutation,
                                        expected_permutation);

                if (!std::next_permutation(ranges_permutations.begin(), ranges_permutations.end()))
                    break;
            }
        }
    }

    // sort calculation: (which used in sort_block )
    //    get_permutation means sort data in Column as sort order
    //    limit should be set to limit the sort result
    //    nan_direction_hint deal with null|NaN value
    void assertColumnPermutation(const IColumn& column, bool ascending, size_t limit,
                                 int nan_direction_hint, IColumn::Permutation& actual_permutation,
                                 IColumn::Permutation& expected_permutation) {
        // step1. get expect permutation as stabled sort
        stableGetColumnPermutation(column, ascending, limit, nan_direction_hint,
                                   expected_permutation);

        // step2. get permutation by column
        column.get_permutation(ascending, limit, nan_direction_hint, expected_permutation);

        if (limit == 0) {
            limit = actual_permutation.size();
        }

        // step3. check the permutation result
        assertPermutationsWithLimit(actual_permutation, expected_permutation, limit);
    }

    //  permute()
    //   1/ Key topN set read_orderby_key_reverse = true; SegmentIterator::next_batch will permute the column by the given permutation(which reverse the rows of current segment)
    //  should check rows with the given permutation
    void assertPermute(MutableColumns& cols, IColumn::Permutation& permutation, size_t num_rows) {
        std::vector<ColumnPtr> res_permuted;
        for (auto& col : cols) {
            res_permuted.emplace_back(col->permute(permutation, num_rows));
        }
        // check the permutation result for rowsize
        size_t res_rows = res_permuted[0]->size();
        for (auto& col : res_permuted) {
            EXPECT_EQ(col->size(), res_rows);
        }
    }

    // sort_column
    //  1/ sort_column (called in sort_block to sort the column by given permutation)
    void assertSortColumn(IColumn& column, IColumn::Permutation& permutation, size_t num_rows) {
        // just make a simple sort function to sort the column
        std::vector<ColumnPtr> res_sorted;
        // SortColumnDescription:
        //    Column number;
        //    int direction;           /// 1 - ascending, -1 - descending.
        //    int nulls_direction;     /// 1 - NULLs and NaNs are greater, -1 - less.
        std::vector<SortColumnDescription> sort_desc;
        SortColumnDescription _column_with_descend_null_greater = {1, -1, 1};
        SortColumnDescription _column_with_descend_null_less = {1, -1, -1};
        SortColumnDescription _column_with_ascend_null_greater = {1, 1, 1};
        SortColumnDescription _column_with_ascend_null_less = {1, 1, -1};
        sort_desc.emplace_back(_column_with_descend_null_greater);
        sort_desc.emplace_back(_column_with_descend_null_less);
        sort_desc.emplace_back(_column_with_ascend_null_greater);
        sort_desc.emplace_back(_column_with_ascend_null_less);
        EqualFlags flags(num_rows, 1);
        EqualRange range {0, num_rows};
        for (auto& column_with_sort_desc : sort_desc) {
            ColumnSorter sorter({&column, column_with_sort_desc}, 0);
            column.sort_column(&sorter, flags, permutation, range, num_rows);
        };
        // check the sort result for flags and ranges
        for (size_t i = 0; i < num_rows; i++) {
            std::cout << "i: " << i << " " << flags[i] << std::endl;
        }
    }
};

} // namespace doris::vectorized