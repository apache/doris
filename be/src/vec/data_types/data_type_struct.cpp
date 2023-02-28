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
        LOG(FATAL) << "Wrong number of names passed to constructor of DataTypeStruct";
    }

    Status st = check_tuple_names(names);
    //if (!st.ok()) {
    //}
}

std::string DataTypeStruct::do_get_name() const {
    size_t size = elems.size();
    std::stringstream s;

    s << "Struct(";
    for (size_t i = 0; i < size; ++i) {
        if (i != 0) {
            s << ", ";
        }

        // if (have_explicit_names) {
        //     s << back_quote_if_need(names[i]) << ' ';
        // }

        s << elems[i]->get_name();
    }
    s << ")";

    return s.str();
}

Status DataTypeStruct::from_string(ReadBuffer& rb, IColumn* column) const {
    DCHECK(!rb.eof());
    auto* struct_column = assert_cast<ColumnStruct*>(column);

    if (*rb.position() != '{') {
        return Status::InvalidArgument("Struct does not start with '{' character, found '{}'",
                                       *rb.position());
    }
    if (rb.count() < 2 || *(rb.end() - 1) != '}') {
        return Status::InvalidArgument("Struct does not end with '}' character, found '{}'",
                                       *(rb.end() - 1));
    }

    // here need handle the empty struct '{}'
    if (rb.count() == 2) {
        return Status::OK();
    }

    ++rb.position();
    std::vector<ReadBuffer> field_rbs;
    field_rbs.reserve(elems.size());

    // here get the value "jack" and 20 from {"name":"jack","age":20}
    while (!rb.eof()) {
        size_t field_len = 0;
        auto start = rb.position();
        while (!rb.eof() && *start != ',' && *start != '}') {
            field_len++;
            start++;
        }
        if (field_len >= rb.count()) {
            return Status::InvalidArgument("Invalid Length");
        }
        ReadBuffer field_rb(rb.position(), field_len);

        size_t len = 0;
        auto start_rb = field_rb.position();
        while (!field_rb.eof() && *start_rb != ':') {
            len++;
            start_rb++;
        }
        ReadBuffer field(field_rb.position() + len + 1, field_rb.count() - len - 1);

        if (field.count() >= 2 && ((*field.position() == '"' && *(field.end() - 1) == '"') ||
                                   (*field.position() == '\'' && *(field.end() - 1) == '\''))) {
            ReadBuffer field_no_quote(field.position() + 1, field.count() - 2);
            field_rbs.push_back(field_no_quote);
        } else {
            field_rbs.push_back(field);
        }

        rb.position() += field_len + 1;
    }

    for (size_t idx = 0; idx < elems.size(); idx++) {
        elems[idx]->from_string(field_rbs[idx], &struct_column->get_column(idx));
    }

    return Status::OK();
}

std::string DataTypeStruct::to_string(const IColumn& column, size_t row_num) const {
    auto ptr = column.convert_to_full_column_if_const();
    auto& struct_column = assert_cast<const ColumnStruct&>(*ptr.get());

    std::stringstream ss;
    ss << "{";
    for (size_t idx = 0; idx < elems.size(); idx++) {
        if (idx != 0) {
            ss << ", ";
        }
        ss << elems[idx]->to_string(struct_column.get_column(idx), row_num);
    }
    ss << "}";
    return ss.str();
}

void DataTypeStruct::to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const {
    auto ptr = column.convert_to_full_column_if_const();
    auto& struct_column = assert_cast<const ColumnStruct&>(*ptr.get());
    ostr.write("{", 1);
    for (size_t idx = 0; idx < elems.size(); idx++) {
        if (idx != 0) {
            ostr.write(", ", 2);
        }
        elems[idx]->to_string(struct_column.get_column(idx), row_num, ostr);
    }
    ostr.write("}", 1);
}

static inline IColumn& extract_element_column(IColumn& column, size_t idx) {
    return assert_cast<ColumnStruct&>(column).get_column(idx);
}

template <typename F>
static void add_element_safe(const DataTypes& elems, IColumn& column, F&& impl) {
    /// We use the assumption that tuples of zero size do not exist.
    size_t old_size = column.size();

    try {
        impl();

        // Check that all columns now have the same size.
        size_t new_size = column.size();

        for (auto i = 0; i < elems.size(); i++) {
            const auto& element_column = extract_element_column(column, i);
            if (element_column.size() != new_size) {
                // This is not a logical error because it may work with
                // user-supplied data.
                LOG(FATAL) << "Cannot read a tuple because not all elements are present";
            }
        }
    } catch (...) {
        for (auto i = 0; i < elems.size(); i++) {
            auto& element_column = extract_element_column(column, i);

            if (element_column.size() > old_size) {
                element_column.pop_back(1);
            }
        }

        throw;
    }
}

MutableColumnPtr DataTypeStruct::create_column() const {
    size_t size = elems.size();
    MutableColumns tuple_columns(size);
    for (size_t i = 0; i < size; ++i) {
        tuple_columns[i] = elems[i]->create_column();
    }
    return ColumnStruct::create(std::move(tuple_columns));
}

Field DataTypeStruct::get_default() const {
    return Tuple();
}

void DataTypeStruct::insert_default_into(IColumn& column) const {
    add_element_safe(elems, column, [&] {
        for (auto i = 0; i < elems.size(); i++) {
            elems[i]->insert_default_into(extract_element_column(column, i));
        }
    });
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
    LOG(FATAL) << "Struct doesn't have element with name '" + name + "'";
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
    if (i == 0 || i > names.size()) {
        fmt::memory_buffer error_msg;
        fmt::format_to(error_msg, "Index of tuple element ({}) if out range ([1, {}])", i,
                       names.size());
        LOG(FATAL) << fmt::to_string(error_msg);
    }

    return names[i - 1];
}

int64_t DataTypeStruct::get_uncompressed_serialized_bytes(const IColumn& column,
                                                          int be_exec_version) const {
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

char* DataTypeStruct::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    auto ptr = column.convert_to_full_column_if_const();
    const auto& struct_column = assert_cast<const ColumnStruct&>(*ptr.get());
    DCHECK(elems.size() == struct_column.tuple_size());

    for (size_t i = 0; i < elems.size(); ++i) {
        buf = elems[i]->serialize(struct_column.get_column(i), buf, be_exec_version);
    }
    return buf;
}

const char* DataTypeStruct::deserialize(const char* buf, IColumn* column,
                                        int be_exec_version) const {
    auto* struct_column = assert_cast<ColumnStruct*>(column);
    DCHECK(elems.size() == struct_column->tuple_size());

    for (size_t i = 0; i < elems.size(); ++i) {
        buf = elems[i]->deserialize(buf, &struct_column->get_column(i), be_exec_version);
    }
    return buf;
}

void DataTypeStruct::to_pb_column_meta(PColumnMeta* col_meta) const {
    IDataType::to_pb_column_meta(col_meta);
    for (size_t i = 0; i < elems.size(); ++i) {
        elems[i]->to_pb_column_meta(col_meta->add_children());
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

size_t DataTypeStruct::get_maximum_size_of_value_in_memory() const {
    size_t res = 0;
    for (const auto& elem : elems) {
        res += elem->get_maximum_size_of_value_in_memory();
    }
    return res;
}

size_t DataTypeStruct::get_size_of_value_in_memory() const {
    size_t res = 0;
    for (const auto& elem : elems) {
        res += elem->get_size_of_value_in_memory();
    }
    return res;
}

} // namespace doris::vectorized
