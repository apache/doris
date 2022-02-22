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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeArray.h
// and modified by Doris

#include "vec/data_types/data_type_array.h"

#include "gen_cpp/data.pb.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

namespace ErrorCodes {
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

DataTypeArray::DataTypeArray(const DataTypePtr & nested_)
    : nested{nested_} {
}

MutableColumnPtr DataTypeArray::create_column() const {
    return ColumnArray::create(nested->create_column(), ColumnArray::ColumnOffsets::create());
}

Field DataTypeArray::get_default() const {
    return Array();
}

bool DataTypeArray::equals(const IDataType & rhs) const {
    return typeid(rhs) == typeid(*this) && nested->equals(*static_cast<const DataTypeArray &>(rhs).nested);
}

size_t DataTypeArray::get_number_of_dimensions() const {
    const DataTypeArray * nested_array = typeid_cast<const DataTypeArray *>(nested.get());
    if (!nested_array)
        return 1;
    return 1 + nested_array->get_number_of_dimensions();   /// Every modern C++ compiler optimizes tail recursion.
}

/*
size_t DataTypeArray::serialize(const IColumn& column, PColumn* pcolumn) const {
    size_t size_before_compress = 0;
    auto ptr = column.convert_to_full_column_if_const();
    const auto& array_column = assert_cast<const ColumnArray&>(*ptr.get());

    // set pcolumn data type
    pcolumn->set_type(PColumn::ARRAY);

    // compute the mem need to be allocate
    auto off_len = (column.size() + 1) * sizeof(IColumn::Offset);
    pcolumn->mutable_binary()->resize(off_len);
    auto* data = pcolumn->mutable_binary()->data();

    // serialize offsets of the array
    *reinterpret_cast<IColumn::Offset*>(data) = column.size();
    data += sizeof(IColumn::Offset);
    memcpy(data, array_column.get_offsets().data(), column.size() * sizeof(IColumn::Offset));

    // serialize nested data of array
    auto* sub_pcolumn = pcolumn->add_sub_columns();
    size_before_compress += nested->serialize(*array_column.get_data_ptr(), sub_pcolumn);

    return size_before_compress + compress_binary(pcolumn);
}

void DataTypeArray::deserialize(const PColumn& pcolumn, IColumn* column) const {
    auto* array_column = assert_cast<ColumnArray*>(column);
    auto& offsets = array_column->get_offsets();
    std::string uncompressed;
    read_binary(pcolumn, &uncompressed);

    // deserialize offsets of the array
    auto* origin_data = uncompressed.data();
    uint32_t column_len = *reinterpret_cast<IColumn::Offset*>(origin_data);
    origin_data += sizeof(IColumn::Offset);
    offsets.resize(column_len);
    memcpy(offsets.data(), origin_data, sizeof(IColumn::Offset) * column_len);

    // deserialize nested data of array
    nested->deserialize(pcolumn.sub_columns(0), array_column->get_data_ptr()->assume_mutable());
}
*/
char* DataTypeArray::serialize(const IColumn& column, char* buf) const {
    return nullptr;
}
const char* DataTypeArray::deserialize(const char* buf, IColumn* column) const {
    return nullptr;
}

} // namespace doris::vectorized
