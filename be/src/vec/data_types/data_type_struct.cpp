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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeTuple.cpp
// and modified by Doris

#include "vec/data_types/data_type_struct.h"

#include <ctype.h>
#include <fmt/format.h>
#include <gen_cpp/data.pb.h>
#include <glog/logging.h>
#include <string.h>

#include <algorithm>
#include <cstddef>
#include <memory>
#include <ostream>
#include <typeinfo>
#include <unordered_set>
#include <utility>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_struct.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/string_ref.h"

namespace doris::vectorized {

DataTypeStruct::DataTypeStruct(const DataTypes& elems_)
        : elems(elems_), have_explicit_names(false) {
    /// Automatically assigned names in form of '1', '2', ...
    size_t size = elems.size();
    names.resize(size);
    for (size_t i = 0; i < size; ++i) {
        names[i] = std::to_string(i + 1);
    }
}

static Status check_tuple_names(const Strings& names) {
    std::unordered_set<String> names_set;
    for (const auto& name : names) {
        if (name.empty()) {
            return Status::InvalidArgument("Names of tuple elements cannot be empty");
        }

        if (!names_set.insert(name).second) {
            return Status::InvalidArgument("Names of tuple elements must be unique");
        }
    }

    return {};
}

DataTypeStruct::DataTypeStruct(const DataTypes& elems_, const Strings& names_)
        : elems(elems_), names(names_), have_explicit_names(true) {
    size_t size = elems.size();
    if (names.size() != size) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Wrong number of names passed to constructor of DataTypeStruct");
        __builtin_unreachable();
    }

    Status st = check_tuple_names(names);
}

std::string DataTypeStruct::do_get_name() const {
    size_t size = elems.size();
    std::stringstream s;

    s << "Struct(";
    for (size_t i = 0; i < size; ++i) {
        if (i != 0) {
            s << ", ";
        }
        s << names[i] << ":";
        s << elems[i]->get_name();
    }
    s << ")";

    return s.str();
}

std::string DataTypeStruct::to_string(const IColumn& column, size_t row_num) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    auto& struct_column = assert_cast<const ColumnStruct&>(*ptr);

    std::string str;
    str += "{";
    for (size_t idx = 0; idx < elems.size(); idx++) {
        if (idx != 0) {
            str += ", ";
        }
        str += elems[idx]->to_string(struct_column.get_column(idx), row_num);
    }
    str += "}";
    return str;
}

void DataTypeStruct::to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    auto& struct_column = assert_cast<const ColumnStruct&>(*ptr);
    ostr.write("{", 1);
    for (size_t idx = 0; idx < elems.size(); idx++) {
        if (idx != 0) {
            ostr.write(", ", 2);
        }
        elems[idx]->to_string(struct_column.get_column(idx), row_num, ostr);
    }
    ostr.write("}", 1);
}

MutableColumnPtr DataTypeStruct::create_column() const {
    size_t size = elems.size();
    MutableColumns tuple_columns(size);
    for (size_t i = 0; i < size; ++i) {
        tuple_columns[i] = elems[i]->create_column();
    }
    return ColumnStruct::create(std::move(tuple_columns));
}

Status DataTypeStruct::check_column(const IColumn& column) const {
    const auto* column_struct = DORIS_TRY(check_column_nested_type<ColumnStruct>(column));
    if (elems.size() != column_struct->tuple_size()) {
        return Status::InvalidArgument(
                "ColumnStruct has {} elements, but DataTypeStruct has {} elements",
                column_struct->tuple_size(), elems.size());
    }
    for (size_t i = 0; i < elems.size(); ++i) {
        const auto& elem = elems[i];
        const auto& child_column = column_struct->get_column(i);
        RETURN_IF_ERROR(elem->check_column(child_column));
    }
    return Status::OK();
}

Field DataTypeStruct::get_default() const {
    size_t size = elems.size();
    Tuple t;
    for (size_t i = 0; i < size; ++i) {
        t.push_back(elems[i]->get_default());
    }
    return Field::create_field<TYPE_STRUCT>(t);
}

bool DataTypeStruct::equals(const IDataType& rhs) const {
    if (typeid(rhs) != typeid(*this)) {
        return false;
    }

    const DataTypeStruct& rhs_tuple = static_cast<const DataTypeStruct&>(rhs);

    size_t size = elems.size();
    if (size != rhs_tuple.elems.size()) {
        return false;
    }

    for (size_t i = 0; i < size; ++i) {
        if (!elems[i]->equals(*rhs_tuple.elems[i])) {
            return false;
        }
    }

    return true;
}

size_t DataTypeStruct::get_position_by_name(const String& name) const {
    size_t size = elems.size();
    for (size_t i = 0; i < size; ++i) {
        if (names[i] == name) {
            return i;
        }
    }
    throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                           "Struct doesn't have element with name  " + name);
    __builtin_unreachable();
}

std::optional<size_t> DataTypeStruct::try_get_position_by_name(const String& name) const {
    size_t size = elems.size();
    for (size_t i = 0; i < size; ++i) {
        if (names[i] == name) {
            return std::optional<size_t>(i);
        }
    }
    return std::nullopt;
}

String DataTypeStruct::get_name_by_position(size_t i) const {
    return names[i];
}

// binary : const flag| row num | read saved num| childs
// childs : child1 | child2 ...
int64_t DataTypeStruct::get_uncompressed_serialized_bytes(const IColumn& column,
                                                          int be_exec_version) const {
    if (be_exec_version >= USE_CONST_SERDE) {
        auto size = sizeof(bool) + sizeof(size_t) + sizeof(size_t);
        bool is_const_column = is_column_const(column);
        const IColumn* data_column = &column;
        if (is_const_column) {
            const auto& const_column = assert_cast<const ColumnConst&>(column);
            data_column = &(const_column.get_data_column());
        }
        const auto& struct_column = assert_cast<const ColumnStruct&>(*data_column);
        DCHECK(elems.size() == struct_column.tuple_size());
        int64_t bytes = 0;
        for (size_t i = 0; i < elems.size(); ++i) {
            bytes += elems[i]->get_uncompressed_serialized_bytes(struct_column.get_column(i),
                                                                 be_exec_version);
        }
        return size + bytes;
    } else {
        auto ptr = column.convert_to_full_column_if_const();
        const auto& struct_column = assert_cast<const ColumnStruct&>(*ptr.get());
        DCHECK(elems.size() == struct_column.tuple_size());

        int64_t bytes = 0;
        for (size_t i = 0; i < elems.size(); ++i) {
            bytes += elems[i]->get_uncompressed_serialized_bytes(struct_column.get_column(i),
                                                                 be_exec_version);
        }
        return bytes;
    }
}

char* DataTypeStruct::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    if (be_exec_version >= USE_CONST_SERDE) {
        const auto* data_column = &column;
        [[maybe_unused]] size_t real_need_copy_num = 0;
        buf = serialize_const_flag_and_row_num(&data_column, buf, &real_need_copy_num);

        const auto& struct_column = assert_cast<const ColumnStruct&>(*data_column);
        DCHECK(elems.size() == struct_column.tuple_size());
        for (size_t i = 0; i < elems.size(); ++i) {
            buf = elems[i]->serialize(struct_column.get_column(i), buf, be_exec_version);
        }
        return buf;
    } else {
        auto ptr = column.convert_to_full_column_if_const();
        const auto& struct_column = assert_cast<const ColumnStruct&>(*ptr.get());
        DCHECK(elems.size() == struct_column.tuple_size());

        for (size_t i = 0; i < elems.size(); ++i) {
            buf = elems[i]->serialize(struct_column.get_column(i), buf, be_exec_version);
        }
        return buf;
    }
}
const char* DataTypeStruct::deserialize(const char* buf, MutableColumnPtr* column,
                                        int be_exec_version) const {
    if (be_exec_version >= USE_CONST_SERDE) {
        auto* origin_column = column->get();
        [[maybe_unused]] size_t real_have_saved_num = 0;
        buf = deserialize_const_flag_and_row_num(buf, column, &real_have_saved_num);

        auto* struct_column = assert_cast<ColumnStruct*>(origin_column);
        DCHECK(elems.size() == struct_column->tuple_size());
        for (size_t i = 0; i < elems.size(); ++i) {
            auto child_column = struct_column->get_column_ptr(i)->assume_mutable();
            buf = elems[i]->deserialize(buf, &child_column, be_exec_version);
        }
        return buf;
    } else {
        auto* struct_column = assert_cast<ColumnStruct*>(column->get());
        DCHECK(elems.size() == struct_column->tuple_size());

        for (size_t i = 0; i < elems.size(); ++i) {
            auto child_column = struct_column->get_column_ptr(i)->assume_mutable();
            buf = elems[i]->deserialize(buf, &child_column, be_exec_version);
        }
        return buf;
    }
}

void DataTypeStruct::to_pb_column_meta(PColumnMeta* col_meta) const {
    IDataType::to_pb_column_meta(col_meta);
    for (size_t i = 0; i < elems.size(); ++i) {
        auto child = col_meta->add_children();
        child->set_name(names[i]);
        elems[i]->to_pb_column_meta(child);
    }
}

bool DataTypeStruct::text_can_contain_only_valid_utf8() const {
    return std::all_of(elems.begin(), elems.end(),
                       [](auto&& elem) { return elem->text_can_contain_only_valid_utf8(); });
}

bool DataTypeStruct::have_maximum_size_of_value() const {
    return std::all_of(elems.begin(), elems.end(),
                       [](auto&& elem) { return elem->have_maximum_size_of_value(); });
}

bool DataTypeStruct::is_comparable() const {
    return std::all_of(elems.begin(), elems.end(),
                       [](auto&& elem) { return elem->is_comparable(); });
}

size_t DataTypeStruct::get_size_of_value_in_memory() const {
    size_t res = 0;
    for (const auto& elem : elems) {
        res += elem->get_size_of_value_in_memory();
    }
    return res;
}

} // namespace doris::vectorized
