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

#pragma once

#include <gen_cpp/parquet_types.h>
#include <stddef.h>

#include "common/status.h"
#include "vec/data_types/data_type.h"
#include "vec/exec/format/parquet/decoder.h"
#include "vec/exec/format/parquet/parquet_column_convert.h"
#include "vec/exec/format/parquet/parquet_common.h"
namespace doris {
namespace vectorized {
class ColumnSelectVector;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

template <tparquet::Type::type PhysicalType>
class FixLengthPlainDecoder final : public Decoder {
public:
    FixLengthPlainDecoder() {};
    ~FixLengthPlainDecoder() override = default;

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector, bool is_dict_filter) override;

    template <bool has_filter>
    Status _decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                          ColumnSelectVector& select_vector, bool is_dict_filter);

    Status skip_values(size_t num_values) override;

protected:
    template <bool has_filter>
    Status _decode_numeric(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector);

    template <bool has_filter>
    Status _decode_string(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector);
};

template <tparquet::Type::type PhysicalType>
Status FixLengthPlainDecoder<PhysicalType>::skip_values(size_t num_values) {
    _offset += _type_length * num_values;
    if (UNLIKELY(_offset > _data->size)) {
        return Status::IOError("Out-of-bounds access in parquet data decoder");
    }
    return Status::OK();
}

template <tparquet::Type::type PhysicalType>
Status FixLengthPlainDecoder<PhysicalType>::decode_values(MutableColumnPtr& doris_column,
                                                          DataTypePtr& data_type,
                                                          ColumnSelectVector& select_vector,
                                                          bool is_dict_filter) {
    if (select_vector.has_filter()) {
        return _decode_values<true>(doris_column, data_type, select_vector, is_dict_filter);
    } else {
        return _decode_values<false>(doris_column, data_type, select_vector, is_dict_filter);
    }
}

template <tparquet::Type::type PhysicalType>
template <bool has_filter>
Status FixLengthPlainDecoder<PhysicalType>::_decode_values(MutableColumnPtr& doris_column,
                                                           DataTypePtr& data_type,
                                                           ColumnSelectVector& select_vector,
                                                           bool is_dict_filter) {
    size_t non_null_size = select_vector.num_values() - select_vector.num_nulls();
    if (UNLIKELY(_offset + _type_length * non_null_size > _data->size)) {
        return Status::IOError("Out-of-bounds access in parquet data decoder");
    }

    if constexpr (PhysicalType == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
        return _decode_string<has_filter>(doris_column, select_vector);
    } else {
        return _decode_numeric<has_filter>(doris_column, select_vector);
    }
}

template <tparquet::Type::type PhysicalType>
template <bool has_filter>
Status FixLengthPlainDecoder<PhysicalType>::_decode_string(MutableColumnPtr& doris_column,
                                                           ColumnSelectVector& select_vector) {
    auto& string_column = static_cast<ColumnString&>(*doris_column);

    auto& data = string_column.get_chars();
    size_t data_index = data.size();
    data.resize(data_index +
                _type_length * (select_vector.num_values() - select_vector.num_filtered()));
    auto& offset = string_column.get_offsets();
    size_t offset_index = offset.size();
    offset.resize(offset_index + select_vector.num_values() - select_vector.num_filtered());

    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            memcpy(data.data() + data_index, _data->data + _offset, _type_length * run_length);
            _offset += _type_length * run_length;
            data_index += _type_length * run_length;

            for (int i = 0; i < run_length; i++) {
                offset[offset_index] = offset[offset_index - 1] + _type_length;
                offset_index++;
            }
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            doris_column->insert_many_defaults(run_length);
            break;
        }
        case ColumnSelectVector::FILTERED_CONTENT: {
            _offset += _type_length * run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_NULL: {
            // do nothing
            break;
        }
        }
    }
    return Status::OK();
}

template <tparquet::Type::type PhysicalType>
template <bool has_filter>
Status FixLengthPlainDecoder<PhysicalType>::_decode_numeric(MutableColumnPtr& doris_column,
                                                            ColumnSelectVector& select_vector) {
    auto& column_data = reinterpret_cast<ColumnVector<Int8>&>(*doris_column).get_data();
    size_t data_index = column_data.size();
    column_data.resize(data_index +
                       _type_length * (select_vector.num_values() - select_vector.num_filtered()));
    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            memcpy(column_data.data() + data_index, _data->data + _offset,
                   run_length * _type_length);
            _offset += run_length * _type_length;
            data_index += run_length * _type_length;
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            data_index += run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_CONTENT: {
            _offset += _type_length * run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_NULL: {
            // do nothing
            break;
        }
        }
    }
    return Status::OK();
}
} // namespace doris::vectorized
