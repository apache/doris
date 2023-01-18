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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/ObjectUtils.h
// and modified by Doris

#include <vec/columns/column_array.h>
#include <vec/columns/column_object.h>
#include <vec/common/object_util.h>
#include <vec/core/field.h>
#include <vec/data_types/data_type_array.h>
#include <vec/data_types/data_type_object.h>
#include <vec/functions/simple_function_factory.h>
#include <vec/json/parse2column.h>

#include <vec/data_types/data_type_factory.hpp>

#include "gen_cpp/FrontendService.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "olap/rowset/rowset_writer_context.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "util/thrift_rpc_helper.h"

namespace doris::vectorized::object_util {
template <size_t I, typename Tuple>
static auto extract_vector(const std::vector<Tuple>& vec) {
    static_assert(I < std::tuple_size_v<Tuple>);
    std::vector<std::tuple_element_t<I, Tuple>> res;
    res.reserve(vec.size());
    for (const auto& elem : vec) res.emplace_back(std::get<I>(elem));
    return res;
}

size_t get_number_of_dimensions(const IDataType& type) {
    if (const auto* type_array = typeid_cast<const DataTypeArray*>(&type))
        return type_array->get_number_of_dimensions();
    return 0;
}
size_t get_number_of_dimensions(const IColumn& column) {
    if (const auto* column_array = check_and_get_column<ColumnArray>(column))
        return column_array->get_number_of_dimensions();
    return 0;
}

DataTypePtr get_base_type_of_array(const DataTypePtr& type) {
    /// Get raw pointers to avoid extra copying of type pointers.
    const DataTypeArray* last_array = nullptr;
    const auto* current_type = type.get();
    while (const auto* type_array = typeid_cast<const DataTypeArray*>(current_type)) {
        current_type = type_array->get_nested_type().get();
        last_array = type_array;
    }
    return last_array ? last_array->get_nested_type() : type;
}

Array create_empty_array_field(size_t num_dimensions) {
    DCHECK(num_dimensions > 0);
    Array array;
    Array* current_array = &array;
    for (size_t i = 1; i < num_dimensions; ++i) {
        current_array->push_back(Array());
        current_array = &current_array->back().get<Array&>();
    }
    return array;
}

// Status convert_objects_to_tuples(Block& block) {
//     for (auto& column : block) {
//         if (!column.type->is_object()) continue;
//         auto& column_object = assert_cast<ColumnObject&>(*column.column->assume_mutable());
//         column_object.finalize();
//         const auto& subcolumns = column_object.get_subcolumns();
//         if (!column_object.is_finalized())
//             return Status::InvalidArgument("Can't convert object to tuple");
//         PathsInData tuple_paths;
//         DataTypes tuple_types;
//         Columns tuple_columns;
//         for (const auto& entry : subcolumns) {
//             tuple_paths.emplace_back(entry->path);
//             tuple_types.emplace_back(entry->data.getLeastCommonType());
//             tuple_columns.emplace_back(entry->data.get_finalized_column_ptr());
//         }
//         std::tie(column.column, column.type) =
//                 unflatten_tuple(tuple_paths, tuple_types, tuple_columns);
//     }
//     return Status::OK();
// }
// 
// DataTypePtr unflatten_tuple(const PathsInData& paths, const DataTypes& tuple_types) {
//     assert(paths.size() == tuple_types.size());
//     Columns tuple_columns;
//     tuple_columns.reserve(tuple_types.size());
//     for (const auto& type : tuple_types) tuple_columns.emplace_back(type->create_column());
//     return unflatten_tuple(paths, tuple_types, tuple_columns).second;
// }

ColumnPtr reduce_number_of_dimensions(ColumnPtr column, size_t dimensions_to_reduce) {
    while (dimensions_to_reduce--) {
        const auto* column_array = typeid_cast<const ColumnArray*>(column.get());
        DCHECK(column_array) << "Not enough dimensions to reduce";
        column = column_array->get_data_ptr();
    }
    return column;
}
DataTypePtr reduce_number_of_dimensions(DataTypePtr type, size_t dimensions_to_reduce) {
    while (dimensions_to_reduce--) {
        const auto* type_array = typeid_cast<const DataTypeArray*>(type.get());
        DCHECK(type_array) << "Not enough dimensions to reduce";
        type = type_array->get_nested_type();
    }
    return type;
}

struct ColumnWithTypeAndDimensions {
    ColumnPtr column;
    DataTypePtr type;
    size_t array_dimensions;
};
using SubcolumnsTreeWithColumns = SubcolumnsTree<ColumnWithTypeAndDimensions>;
using Node = SubcolumnsTreeWithColumns::Node;

// Creates data type and column from tree of subcolumns.
// ColumnWithTypeAndDimensions create_type_from_node(const Node* node) {
//     auto collect_tuple_elemets = [](const auto& children) {
//         std::vector<std::tuple<String, ColumnWithTypeAndDimensions>> tuple_elements;
//         tuple_elements.reserve(children.size());
//         //for (const auto & [name, child] : children)
//         for (auto it = children.begin(); it != children.end(); ++it) {
//             auto column = create_type_from_node(it->get_second().get());
//             tuple_elements.emplace_back(it->get_first(), std::move(column));
//         }
//         /// Sort to always create the same type for the same set of subcolumns.
//         std::sort(tuple_elements.begin(), tuple_elements.end(),
//                   [](const auto& lhs, const auto& rhs) {
//                       return std::get<0>(lhs) < std::get<0>(rhs);
//                   });
//         auto tuple_names = extract_vector<0>(tuple_elements);
//         auto tuple_columns = extract_vector<1>(tuple_elements);
//         return std::make_tuple(std::move(tuple_names), std::move(tuple_columns));
//     };
//     if (node->kind == Node::SCALAR) {
//         return node->data;
//     } else if (node->kind == Node::NESTED) {
//         auto [tuple_names, tuple_columns] = collect_tuple_elemets(node->children);
//         Columns offsets_columns;
//         offsets_columns.reserve(tuple_columns[0].array_dimensions + 1);
//         /// If we have a Nested node and child node with anonymous array levels
//         /// we need to push a Nested type through all array levels.
//         /// Example: { "k1": [[{"k2": 1, "k3": 2}] } should be parsed as
//         /// `k1 Array(Nested(k2 Int, k3 Int))` and k1 is marked as Nested
//         /// and `k2` and `k3` has anonymous_array_level = 1 in that case.
//         const auto& current_array = assert_cast<const ColumnArray&>(*node->data.column);
//         offsets_columns.push_back(current_array.get_offsets_ptr());
//         auto first_column = tuple_columns[0].column;
//         for (size_t i = 0; i < tuple_columns[0].array_dimensions; ++i) {
//             const auto& column_array = assert_cast<const ColumnArray&>(*first_column);
//             offsets_columns.push_back(column_array.get_offsets_ptr());
//             first_column = column_array.get_data_ptr();
//         }
//         size_t num_elements = tuple_columns.size();
//         Columns tuple_elements_columns(num_elements);
//         DataTypes tuple_elements_types(num_elements);
//         /// Reduce extra array dimensions to get columns and types of Nested elements.
//         for (size_t i = 0; i < num_elements; ++i) {
//             assert(tuple_columns[i].array_dimensions == tuple_columns[0].array_dimensions);
//             tuple_elements_columns[i] = reduce_number_of_dimensions(
//                     tuple_columns[i].column, tuple_columns[i].array_dimensions);
//             tuple_elements_types[i] = reduce_number_of_dimensions(
//                     tuple_columns[i].type, tuple_columns[i].array_dimensions);
//         }
//         auto result_column = ColumnArray::create(ColumnTuple::create(tuple_elements_columns),
//                                                  offsets_columns.back());
//         auto result_type = std::make_shared<DataTypeArray>(
//                 std::make_shared<DataTypeTuple>(tuple_elements_types, tuple_names));
//         /// Recreate result Array type and Array column.
//         for (auto it = offsets_columns.rbegin() + 1; it != offsets_columns.rend(); ++it) {
//             result_column = ColumnArray::create(result_column, *it);
//             result_type = std::make_shared<DataTypeArray>(result_type);
//         }
//         return {result_column, result_type, tuple_columns[0].array_dimensions};
//     } else {
//         auto [tuple_names, tuple_columns] = collect_tuple_elemets(node->children);
//         size_t num_elements = tuple_columns.size();
//         Columns tuple_elements_columns(num_elements);
//         DataTypes tuple_elements_types(num_elements);
//         for (size_t i = 0; i < tuple_columns.size(); ++i) {
//             assert(tuple_columns[i].array_dimensions == tuple_columns[0].array_dimensions);
//             tuple_elements_columns[i] = tuple_columns[i].column;
//             tuple_elements_types[i] = tuple_columns[i].type;
//         }
//         auto result_column = ColumnTuple::create(tuple_elements_columns);
//         auto result_type = std::make_shared<DataTypeTuple>(tuple_elements_types, tuple_names);
//         return {result_column, result_type, tuple_columns[0].array_dimensions};
//     }
// }
//
// std::pair<ColumnPtr, DataTypePtr> unflatten_tuple(const PathsInData& paths,
//                                                   const DataTypes& tuple_types,
//                                                   const Columns& tuple_columns) {
//     assert(paths.size() == tuple_types.size());
//     assert(paths.size() == tuple_columns.size());
//     /// We add all paths to the subcolumn tree and then create a type from it.
//     /// The tree stores column, type and number of array dimensions
//     /// for each intermediate node.
//     SubcolumnsTreeWithColumns tree;
//     for (size_t i = 0; i < paths.size(); ++i) {
//         auto column = tuple_columns[i];
//         auto type = tuple_types[i];
//         const auto& parts = paths[i].get_parts();
//         size_t num_parts = parts.size();
//         size_t pos = 0;
//         tree.add(paths[i], [&](Node::Kind kind, bool exists) -> std::shared_ptr<Node> {
//             DCHECK(pos < num_parts) << "Not enough name parts for path" << paths[i].get_path()
//                                     << "Expected at least " << pos + 1 << ", got " << num_parts;
//             size_t array_dimensions = kind == Node::NESTED ? 1 : parts[pos].anonymous_array_level;
//             ColumnWithTypeAndDimensions current_column {column, type, array_dimensions};
//             /// Get type and column for next node.
//             if (array_dimensions) {
//                 type = reduce_number_of_dimensions(type, array_dimensions);
//                 column = reduce_number_of_dimensions(column, array_dimensions);
//             }
//             ++pos;
//             if (exists) return nullptr;
//             return kind == Node::SCALAR ? std::make_shared<Node>(kind, current_column, paths[i])
//                                         : std::make_shared<Node>(kind, current_column);
//         });
//     }
//     auto [column, type, _] = create_type_from_node(tree.get_root());
//     return std::make_pair(std::move(column), std::move(type));
// }
//
// void flatten_tuple_impl(PathInDataBuilder& builder, DataTypePtr type,
//                         std::vector<PathInData::Parts>& new_paths, DataTypes& new_types) {
//     if (const auto* type_tuple = typeid_cast<const DataTypeTuple*>(type.get())) {
//         const auto& tuple_names = type_tuple->get_element_names();
//         const auto& tuple_types = type_tuple->get_elements();
//
//         for (size_t i = 0; i < tuple_names.size(); ++i) {
//             builder.append(tuple_names[i], false);
//             flatten_tuple_impl(builder, tuple_types[i], new_paths, new_types);
//             builder.pop_back();
//         }
//     }
//     // TODO(lhy) flatten nested array
//     //else if (const auto * type_array = typeid_cast<const DataTypeArray *>(type.get()))
//     //{
//     //    PathInDataBuilder element_builder;
//     //    std::vector<PathInData::Parts> element_paths;
//     //    DataTypes element_types;
//
//     //    flatten_tuple_impl(element_builder, type_array->get_nested_type(), element_paths, element_types);
//     //    assert(element_paths.size() == element_types.size());
//
//     //    for (size_t i = 0; i < element_paths.size(); ++i)
//     //    {
//     //        builder.append(element_paths[i], true);
//     //        new_paths.emplace_back(builder.get_parts());
//     //        new_types.emplace_back(std::make_shared<DataTypeArray>(element_types[i]));
//     //        builder.pop_back(element_paths[i].size());
//     //    }
//     //}
//     else {
//         new_paths.emplace_back(builder.get_parts());
//         new_types.emplace_back(type);
//     }
// }
//
// /// @offsets_columns are used as stack of array offsets and allows to recreate Array columns.
// void flatten_tuple_impl(const ColumnPtr& column, Columns& new_columns, Columns& offsets_columns) {
//     if (const auto* column_tuple = check_and_get_column<ColumnTuple>(column.get())) {
//         const auto& subcolumns = column_tuple->get_columns();
//         for (const auto& subcolumn : subcolumns)
//             flatten_tuple_impl(subcolumn, new_columns, offsets_columns);
//     } else {
//         // TODO(lhy) flatten nested array
//         new_columns.push_back(column);
//     }
// }
//
// std::pair<PathsInData, DataTypes> flatten_tuple(const DataTypePtr& type) {
//     std::vector<PathInData::Parts> new_path_parts;
//     DataTypes new_types;
//     PathInDataBuilder builder;
//
//     flatten_tuple_impl(builder, type, new_path_parts, new_types);
//
//     PathsInData new_paths(new_path_parts.begin(), new_path_parts.end());
//     return {new_paths, new_types};
// }
//
// void flatten_tuple(Block& block) {
//     size_t pos = 0;
//     Columns columns;
//     PathsInData paths;
//     DataTypes types;
//     for (auto& column : block) {
//         if (check_and_get_column<ColumnTuple>(column.column)) {
//             columns = flatten_tuple(column.column);
//             std::tie(paths, types) = flatten_tuple(column.type);
//             DCHECK_EQ(columns.size(), types.size());
//             break;
//         }
//         ++pos;
//     }
//     DCHECK_EQ(pos, block.columns() - 1);
//     block.erase(pos);
//     for (size_t i = 0; i < columns.size(); ++i) {
//         if (paths[i].get_path() != ColumnObject::COLUMN_NAME_DUMMY)
//             block.insert(ColumnWithTypeAndName {columns[i], types[i], paths[i].get_path()});
//     }
// }
//
// Columns flatten_tuple(const ColumnPtr& column) {
//     Columns new_columns;
//     Columns offsets_columns;
//
//     flatten_tuple_impl(column, new_columns, offsets_columns);
//     return new_columns;
// }

FieldType get_field_type(const IDataType* data_type) {
    switch (data_type->get_type_id()) {
    case TypeIndex::UInt8:
        return FieldType::OLAP_FIELD_TYPE_UNSIGNED_TINYINT;
    case TypeIndex::UInt16:
        return FieldType::OLAP_FIELD_TYPE_UNSIGNED_SMALLINT;
    case TypeIndex::UInt32:
        return FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT;
    case TypeIndex::UInt64:
        return FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT;
    case TypeIndex::Int8:
        return FieldType::OLAP_FIELD_TYPE_TINYINT;
    case TypeIndex::Int16:
        return FieldType::OLAP_FIELD_TYPE_SMALLINT;
    case TypeIndex::Int32:
        return FieldType::OLAP_FIELD_TYPE_INT;
    case TypeIndex::Int64:
        return FieldType::OLAP_FIELD_TYPE_BIGINT;
    case TypeIndex::Float32:
        return FieldType::OLAP_FIELD_TYPE_FLOAT;
    case TypeIndex::Float64:
        return FieldType::OLAP_FIELD_TYPE_DOUBLE;
    case TypeIndex::Decimal32:
        return FieldType::OLAP_FIELD_TYPE_DECIMAL;
    case TypeIndex::Array:
        return FieldType::OLAP_FIELD_TYPE_ARRAY;
    case TypeIndex::String:
        return FieldType::OLAP_FIELD_TYPE_STRING;
    case TypeIndex::Date:
        return FieldType::OLAP_FIELD_TYPE_DATE;
    case TypeIndex::DateTime:
        return FieldType::OLAP_FIELD_TYPE_DATETIME;
    case TypeIndex::Tuple:
        return FieldType::OLAP_FIELD_TYPE_STRUCT;
    // TODO add more types
    default:
        LOG(FATAL) << "unknow type";
        return FieldType::OLAP_FIELD_TYPE_UNKNOWN;
    }
}

Status parse_object_column(ColumnObject& dest, const IColumn& src, bool need_finalize,
                           const int* row_begin, const int* row_end) {
    assert(src.is_column_string());
    const ColumnString* parsing_column {nullptr};
    if (!src.is_nullable()) {
        parsing_column = reinterpret_cast<const ColumnString*>(src.get_ptr().get());
    } else {
        auto nullable_column = reinterpret_cast<const ColumnNullable*>(src.get_ptr().get());
        parsing_column = reinterpret_cast<const ColumnString*>(
                nullable_column->get_nested_column().get_ptr().get());
    }
    std::vector<StringRef> jsons;
    if (row_begin != nullptr) {
        assert(row_end);
        for (auto x = row_begin; x != row_end; ++x) {
            StringRef ref = parsing_column->get_data_at(*x);
            jsons.push_back(ref);
        }
    } else {
        for (size_t i = 0; i < parsing_column->size(); ++i) {
            StringRef ref = parsing_column->get_data_at(i);
            jsons.push_back(ref);
        }
    }
    // batch parse
    RETURN_IF_ERROR(parse_json_to_variant(dest, jsons));

    if (need_finalize) {
        dest.finalize();
    }
    return Status::OK();
}

Status parse_object_column(Block& block, size_t position) {
    // parse variant column and rewrite column
    auto col = block.get_by_position(position).column;
    const std::string& col_name = block.get_by_position(position).name;
    if (!col->is_column_string()) {
        return Status::InvalidArgument("only ColumnString can be parsed to ColumnObject");
    }
    vectorized::DataTypePtr type(
            std::make_shared<vectorized::DataTypeObject>("", col->is_nullable()));
    auto column_object = type->create_column();
    RETURN_IF_ERROR(
            parse_object_column(assert_cast<ColumnObject&>(column_object->assume_mutable_ref()),
                                *col, true /*need finalize*/, nullptr, nullptr));
    // replace by object
    block.safe_get_by_position(position).column = column_object->get_ptr();
    block.safe_get_by_position(position).type = type;
    block.safe_get_by_position(position).name = col_name;
    return Status::OK();
}

void flatten_object(Block& block, size_t pos, bool replace_if_duplicated) {
    auto column_object_ptr =
            assert_cast<ColumnObject*>(block.get_by_position(pos).column->assume_mutable().get());
    if (column_object_ptr->empty()) {
        block.erase(pos);
        return;
    }
    size_t num_rows = column_object_ptr->size();
    assert(block.rows() <= num_rows);
    assert(column_object_ptr->is_finalized());
    Columns subcolumns;
    DataTypes types;
    Names names;
    for (auto& subcolumn : column_object_ptr->get_subcolumns()) {
        subcolumns.push_back(subcolumn->data.get_finalized_column().get_ptr());
        types.push_back(subcolumn->data.getLeastCommonType());
        names.push_back(subcolumn->path.get_path());
    }
    block.erase(pos);
    for (size_t i = 0; i < subcolumns.size(); ++i) {
        // block may already contains this column, eg. key columns, we should ignore
        // or replcace the same column from object subcolumn
        if (block.has(names[i])) {
            if (replace_if_duplicated) {
                auto& column_type_name = block.get_by_name(names[i]);
                column_type_name.column = subcolumns[i];
                column_type_name.type = types[i];
            }
            continue;
        }
        block.insert(ColumnWithTypeAndName {subcolumns[i], types[i], names[i]});
    }

    // fill default value
    for (auto& [column, _1, _2] : block.get_columns_with_type_and_name()) {
        if (column->size() < num_rows) {
            column->assume_mutable()->insert_many_defaults(num_rows - column->size());
        }
    }
}

Status flatten_object(Block& block, bool replace_if_duplicated) {
    auto object_pos =
            std::find_if(block.begin(), block.end(), [](const ColumnWithTypeAndName& column) {
                return column.type->get_type_id() == TypeIndex::VARIANT;
            });
    if (object_pos != block.end()) {
        flatten_object(block, object_pos - block.begin(), replace_if_duplicated);
    }
    return Status::OK();
}

bool is_conversion_required_between_integers(const IDataType& lhs, const IDataType& rhs) {
    WhichDataType which_lhs(lhs);
    WhichDataType which_rhs(rhs);
    bool is_native_int = which_lhs.is_native_int() && which_rhs.is_native_int();
    bool is_native_uint = which_lhs.is_native_uint() && which_rhs.is_native_uint();
    return (is_native_int || is_native_uint) &&
           lhs.get_size_of_value_in_memory() <= rhs.get_size_of_value_in_memory();
}

bool is_conversion_required_between_integers(FieldType lhs, FieldType rhs) {
    // We only support signed integers for semi-structure data at present
    // TODO add unsigned integers
    if (lhs == OLAP_FIELD_TYPE_BIGINT) {
        return !(rhs == OLAP_FIELD_TYPE_TINYINT || rhs == OLAP_FIELD_TYPE_SMALLINT ||
                 rhs == OLAP_FIELD_TYPE_INT || rhs == OLAP_FIELD_TYPE_BIGINT);
    }
    if (lhs == OLAP_FIELD_TYPE_INT) {
        return !(rhs == OLAP_FIELD_TYPE_TINYINT || rhs == OLAP_FIELD_TYPE_SMALLINT ||
                 rhs == OLAP_FIELD_TYPE_INT);
    }
    if (lhs == OLAP_FIELD_TYPE_SMALLINT) {
        return !(rhs == OLAP_FIELD_TYPE_TINYINT || rhs == OLAP_FIELD_TYPE_SMALLINT);
    }
    if (lhs == OLAP_FIELD_TYPE_TINYINT) {
        return !(rhs == OLAP_FIELD_TYPE_TINYINT);
    }
    return true;
}

Status cast_column(const ColumnWithTypeAndName& arg, const DataTypePtr& type, ColumnPtr* result) {
    ColumnsWithTypeAndName arguments {arg,
                                      {type->create_column_const_with_default_value(1), type, ""}};
    auto function = SimpleFunctionFactory::instance().get_function("CAST", arguments, type);
    Block tmp_block {arguments};
    // the 0 position is input argument, the 1 position is to type argument, the 2 position is result argument
    vectorized::ColumnNumbers argnum;
    argnum.emplace_back(0);
    argnum.emplace_back(1);
    size_t result_column = tmp_block.columns();
    tmp_block.insert({nullptr, type, arg.name});
    RETURN_IF_ERROR(
            function->execute(nullptr, tmp_block, argnum, result_column, arg.column->size()));
    *result = std::move(tmp_block.get_by_position(result_column).column);
    return Status::OK();
}

static void get_column_def(const vectorized::DataTypePtr& data_type, const std::string& name,
                           TColumnDef* column) {
    if (!name.empty()) {
        column->columnDesc.__set_columnName(name);
    }
    if (data_type->is_nullable()) {
        const auto& real_type = static_cast<const DataTypeNullable&>(*data_type);
        column->columnDesc.__set_isAllowNull(true);
        get_column_def(real_type.get_nested_type(), "", column);
        return;
    }
    column->columnDesc.__set_columnType(to_thrift(get_primitive_type(data_type->get_type_id())));
    if (data_type->get_type_id() == TypeIndex::Array) {
        TColumnDef child;
        column->columnDesc.__set_children({});
        get_column_def(assert_cast<const DataTypeArray*>(data_type.get())->get_nested_type(), "",
                       &child);
        column->columnDesc.columnLength =
                TabletColumn::get_field_length_by_type(column->columnDesc.columnType, 0);
        column->columnDesc.children.push_back(child.columnDesc);
        return;
    }
    if (data_type->get_type_id() == TypeIndex::Tuple) {
        // TODO
        // auto tuple_type = assert_cast<const DataTypeTuple*>(data_type.get());
        // DCHECK_EQ(tuple_type->get_elements().size(), tuple_type->get_element_names().size());
        // for (size_t i = 0; i < tuple_type->get_elements().size(); ++i) {
        //     TColumnDef child;
        //     get_column_def(tuple_type->get_element(i), tuple_type->get_element_names()[i], &child);
        //     column->columnDesc.children.push_back(child.columnDesc);
        // }
        // return;
    }
    if (data_type->get_type_id() == TypeIndex::String) {
        return;
    }
    if (WhichDataType(*data_type).is_simple()) {
        column->columnDesc.__set_columnLength(data_type->get_size_of_value_in_memory());
        return;
    }
    return;
}

// send an empty add columns rpc, the rpc response will fill with base schema info
// maybe we could seperate this rpc from add columns rpc
Status send_fetch_full_base_schema_view_rpc(FullBaseSchemaView* schema_view) {
    TAddColumnsRequest req;
    TAddColumnsResult res;
    TTabletInfo tablet_info;
    req.__set_table_name(schema_view->table_name);
    req.__set_db_name(schema_view->db_name);
    req.__set_table_id(schema_view->table_id);
    auto master_addr = ExecEnv::GetInstance()->master_info()->network_address;
    Status rpc_st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&req, &res](FrontendServiceConnection& client) { client->addColumns(res, req); },
            config::txn_commit_rpc_timeout_ms);
    if (!rpc_st.ok()) {
        return Status::InternalError("Failed to fetch schema info, encounter rpc failure");
    }
    // TODO(lhy) handle more status code
    if (res.status.status_code != TStatusCode::OK) {
        LOG(WARNING) << "failed to fetch schema info, code:" << res.status.status_code
                     << ", msg:" << res.status.error_msgs[0];
        return Status::InvalidArgument(
                fmt::format("Failed to fetch schema info, {}", res.status.error_msgs[0]));
    }
    for (const auto& column : res.allColumns) {
        schema_view->column_name_to_column[column.column_name] = column;
    }
    schema_view->schema_version = res.schema_version;
    return Status::OK();
}

// Do batch add columns schema change
// only the base table supported
Status send_add_columns_rpc(ColumnsWithTypeAndName column_type_names,
                            FullBaseSchemaView* schema_view) {
    if (column_type_names.empty()) {
        return Status::OK();
    }
    TAddColumnsRequest req;
    TAddColumnsResult res;
    TTabletInfo tablet_info;
    req.__set_table_name(schema_view->table_name);
    req.__set_db_name(schema_view->db_name);
    req.__set_table_id(schema_view->table_id);
    // TODO(lhy) more configurable
    req.__set_type_conflict_free(true);
    for (const auto& column_type_name : column_type_names) {
        TColumnDef col;
        get_column_def(column_type_name.type, column_type_name.name, &col);
        req.addColumns.push_back(col);
    }
    auto master_addr = ExecEnv::GetInstance()->master_info()->network_address;
    Status rpc_st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&req, &res](FrontendServiceConnection& client) { client->addColumns(res, req); },
            config::txn_commit_rpc_timeout_ms);
    if (!rpc_st.ok()) {
        return Status::InternalError("Failed to do schema change, rpc error");
    }
    // TODO(lhy) handle more status code
    if (res.status.status_code != TStatusCode::OK) {
        LOG(WARNING) << "failed to do schema change, code:" << res.status.status_code
                     << ", msg:" << res.status.error_msgs[0];
        return Status::InvalidArgument(
                fmt::format("Failed to do schema change, {}", res.status.error_msgs[0]));
    }
    size_t sz = res.allColumns.size();
    if (sz < column_type_names.size()) {
        return Status::InternalError(
                fmt::format("Unexpected result columns {}, expected at least {}",
                            res.allColumns.size(), column_type_names.size()));
    }
    for (const auto& column : res.allColumns) {
        schema_view->column_name_to_column[column.column_name] = column;
    }
    schema_view->schema_version = res.schema_version;
    return Status::OK();
}



template <typename ColumnInserterFn>
void align_block_by_name_and_type(MutableBlock* mblock, const Block* block, size_t row_cnt,
                                  ColumnInserterFn inserter) {
    assert(!mblock->get_names().empty());
    const auto& names = mblock->get_names();
    [[maybe_unused]] const auto& data_types = mblock->data_types();
    size_t num_rows = mblock->rows();
    for (size_t i = 0; i < mblock->columns(); ++i) {
        auto& dst = mblock->get_column_by_position(i);
        if (!block->has(names[i])) {
            dst->insert_many_defaults(row_cnt);
        } else {
            assert(data_types[i]->equals(*block->get_by_name(names[i]).type));
            const auto& src = *(block->get_by_name(names[i]).column.get());
            inserter(src, dst);
        }
    }
    for (const auto& [column, type, name] : *block) {
        // encounter a new column
        if (!mblock->has(name)) {
            auto new_column = type->create_column();
            new_column->insert_many_defaults(num_rows);
            inserter(*column.get(), new_column);
            mblock->mutable_columns().push_back(std::move(new_column));
            mblock->data_types().push_back(type);
            mblock->get_names().push_back(name);
        }
    }

#ifndef NDEBUG
    // Check all columns rows matched
    num_rows = mblock->rows();
    for (size_t i = 0; i < mblock->columns(); ++i) {
        DCHECK_EQ(mblock->mutable_columns()[i]->size(), num_rows); 
    }
#endif
}

void align_block_by_name_and_type(MutableBlock* mblock, const Block* block, const int* row_begin,
                                  const int* row_end) {
    align_block_by_name_and_type(mblock, block, row_end - row_begin,
                                 [row_begin, row_end](const IColumn& src, MutableColumnPtr& dst) {
                                     dst->insert_indices_from(src, row_begin, row_end);
                                 });
}
void align_block_by_name_and_type(MutableBlock* mblock, const Block* block, size_t row_begin,
                                  size_t length) {
    align_block_by_name_and_type(mblock, block, length,
                                 [row_begin, length](const IColumn& src, MutableColumnPtr& dst) {
                                     dst->insert_range_from(src, row_begin, length);
                                 });
}

void align_append_block_by_selector(MutableBlock* mblock,
                const Block* block, const IColumn::Selector& selector) {
    // append by selector with alignment
    assert(!mblock->get_names().empty());
    align_block_by_name_and_type(mblock, block, selector.size(), 
                                [&selector](const IColumn& src, MutableColumnPtr& dst) {
                                    src.append_data_by_selector(dst, selector);
                                });
}

void LocalSchemaChangeRecorder::add_extended_columns(const TabletColumn& new_column,
                                                          int32_t schema_version) {
    std::lock_guard<std::mutex> lock(_lock);
    _schema_version = std::max(_schema_version, schema_version);
    auto it = _extended_columns.find(new_column.name());
    if (it != _extended_columns.end()) {
        return;
    }
    _extended_columns.emplace_hint(it, new_column.name(), new_column);
}

bool LocalSchemaChangeRecorder::has_extended_columns() {
    std::lock_guard<std::mutex> lock(_lock);
    return !_extended_columns.empty();
}

std::map<std::string, TabletColumn> LocalSchemaChangeRecorder::copy_extended_columns() {
    std::lock_guard<std::mutex> lock(_lock);
    return _extended_columns;
}

const TabletColumn& LocalSchemaChangeRecorder::column(const std::string& col_name) {
    std::lock_guard<std::mutex> lock(_lock);
    assert(_extended_columns.find(col_name) != _extended_columns.end());
    return _extended_columns[col_name];
}

int32_t LocalSchemaChangeRecorder::schema_version() {
    std::lock_guard<std::mutex> lock(_lock);
    return _schema_version;
}

} // namespace doris::vectorized::object_util
