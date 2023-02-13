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

        // for (auto i : collections::range(0, elems.size())) {
        for (auto i = 0; i < elems.size(); i++) {
            const auto& element_column = extract_element_column(column, i);
            if (element_column.size() != new_size) {
                // This is not a logical error because it may work with
                // user-supplied data.
                LOG(FATAL) << "Cannot read a tuple because not all elements are present";
            }
        }
    } catch (...) {
        // for (const auto& i : collections::range(0, elems.size())) {
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

// MutableColumnPtr DataTypeStruct::create_column(const ISerialization& serialization) const {
//     /// If we read subcolumn of nested Tuple, it may be wrapped to SerializationNamed
//     /// several times to allow to reconstruct the substream path name.
//     /// Here we don't need substream path name, so we drop first several wrapper serializations.

//     const auto* current_serialization = &serialization;
//     while (const auto* serialization_named =
//                    typeid_cast<const SerializationNamed*>(current_serialization))
//         current_serialization = serialization_named->get_nested().get();

//     const auto* serialization_tuple = typeid_cast<const SerializationTuple*>(current_serialization);
//     if (!serialization_tuple)
//         throw Exception(ErrorCodes::LOGICAL_ERROR,
//                         "Unexpected serialization to create column of type Tuple");

//     const auto& element_serializations = serialization_tuple->getElementsSerializations();

//     size_t size = elems.size();
//     assert(element_serializations.size() == size);
//     MutableColumns tuple_columns(size);
//     for (size_t i = 0; i < size; ++i) {
//         tuple_columns[i] = elems[i]->create_column(*element_serializations[i]->get_nested());
//     }

//     return ColumnStruct::create(std::move(tuple_columns));
// }

Field DataTypeStruct::get_default() const {
    return Tuple();
    //return Tuple(collections::map<Tuple>(
    //             elems, [](const DataTypePtr& elem) { return elem->get_default(); }));
}

void DataTypeStruct::insert_default_into(IColumn& column) const {
    add_element_safe(elems, column, [&] {
        // for (const auto& i : collections::range(0, elems.size()))
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

// bool DataTypeStruct::has_dynamic_subcolumns() const {
//     return std::any_of(elems.begin(), elems.end(),
//                        [](auto&& elem) { return elem->has_dynamic_subcolumns(); });
// }

// SerializationPtr DataTypeStruct::do_get_default_serialization() const {
//     SerializationTuple::ElementSerializations serializations(elems.size());

//     for (size_t i = 0; i < elems.size(); ++i) {
//         String elem_name = have_explicit_names ? names[i] : toString(i + 1);
//         auto serialization = elems[i]->get_default_serialization();
//         serializations[i] = std::make_shared<SerializationNamed>(serialization, elem_name);
//     }

//     return std::make_shared<SerializationTuple>(std::move(serializations), have_explicit_names);
// }

// SerializationPtr DataTypeStruct::get_serialization(const SerializationInfo& info) const {
//     SerializationTuple::ElementSerializations serializations(elems.size());
//     const auto& info_tuple = assert_cast<const SerializationInfoTuple&>(info);

//     for (size_t i = 0; i < elems.size(); ++i) {
//         String elem_name = have_explicit_names ? names[i] : toString(i + 1);
//         auto serialization = elems[i]->get_serialization(*info_tuple.get_element_info(i));
//         serializations[i] = std::make_shared<SerializationNamed>(serialization, elem_name);
//     }

//     return std::make_shared<SerializationTuple>(std::move(serializations), have_explicit_names);
// }

// MutableSerializationInfoPtr DataTypeStruct::create_serialization_info(
//         const SerializationInfo::Settings& settings) const {
//     MutableSerializationInfos infos;
//     infos.reserve(elems.size());
//     for (const auto& elem : elems) {
//         infos.push_back(elem->create_serializationInfo(settings));
//     }

//     return std::make_shared<SerializationInfoTuple>(std::move(infos), names, settings);
// }

// SerializationInfoPtr DataTypeStruct::get_serialization_info(const IColumn& column) const {
//     if (const auto* column_const = check_and_get_column<ColumnConst>(&column)) {
//         return get_serialization_info(column_const->get_data_column());
//     }

//     MutableSerializationInfos infos;
//     infos.reserve(elems.size());

//     const auto& column_tuple = assert_cast<const ColumnStruct&>(column);
//     assert(elems.size() == column_tuple.get_columns().size());

//     for (size_t i = 0; i < elems.size(); ++i) {
//         auto element_info = elems[i]->get_serialization_info(column_tuple.getColumn(i));
//         infos.push_back(const_pointer_cast<SerializationInfo>(element_info));
//     }

//     return std::make_shared<SerializationInfoTuple>(std::move(infos), names,
//                                                     SerializationInfo::Settings {});
// }

// static DataTypePtr create(const ASTPtr& arguments) {
//     if (!arguments || arguments->children.empty())
//         throw Exception("Struct cannot be empty", ErrorCodes::EMPTY_DATA_PASSED);

//     DataTypes nested_types;
//     nested_types.reserve(arguments->children.size());

//     Strings names;
//     names.reserve(arguments->children.size());

//     for (const ASTPtr& child : arguments->children) {
//         if (const auto* name_and_type_pair = child->as<ASTNameTypePair>()) {
//             nested_types.emplace_back(DataTypeFactory::instance().get(name_and_type_pair->type));
//             names.emplace_back(name_and_type_pair->name);
//         } else
//             nested_types.emplace_back(DataTypeFactory::instance().get(child));
//     }

//     if (names.empty())
//         return std::make_shared<DataTypeStruct>(nested_types);
//     else if (names.size() != nested_types.size())
//         throw Exception("Names are specified not for all elements of Struct type",
//                         ErrorCodes::BAD_ARGUMENTS);
//     else
//         return std::make_shared<DataTypeStruct>(nested_types, names);
// }

// void registerDataTypeStruct(DataTypeFactory& factory) {
//     factory.registerDataType("Struct", create);
// }

} // namespace doris::vectorized
