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
    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            std::vector<StringRef> string_values;
            string_values.reserve(run_length);
            for (size_t i = 0; i < run_length; ++i) {
                char* buf_start = _data->data + _offset;
                string_values.emplace_back(buf_start, _type_length);
                _offset += _type_length;
            }
            doris_column->insert_many_strings(&string_values[0], run_length);
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
    if constexpr (PhysicalType == tparquet::Type::FIXED_LEN_BYTE_ARRAY ||
                  PhysicalType == tparquet::Type::BYTE_ARRAY) {
        return Status::OK();
    } else {
        using ColumnType = ParquetConvert::PhysicalTypeTraits<PhysicalType>::ColumnType;
        using DataType = ParquetConvert::PhysicalTypeTraits<PhysicalType>::DataType;

        auto& column_data = static_cast<ColumnType&>(*doris_column).get_data();
        size_t data_index = column_data.size();
        column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
        ColumnSelectVector::DataReadType read_type;
        while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                for (size_t i = 0; i < run_length; ++i) {
                    char* buf_start = _data->data + _offset;
                    column_data[data_index++] = *(DataType*)buf_start;
                    _offset += _type_length;
                }
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
}

} // namespace doris::vectorized
