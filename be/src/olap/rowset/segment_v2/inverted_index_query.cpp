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

#include "olap/rowset/segment_v2/inverted_index_query.h"

#include "olap/column_predicate.h"
#include "olap/key_coder.h"
#include "olap/utils.h"
#include "util/time.h"

namespace doris {
namespace segment_v2 {

//NOTE: TYPE_DATETIME is a special case, because it is stored as an int64_t in olap field type,
//but it is declared as an uint64_t in primitive type, so we need to set it separately.
template <>
const typename InvertedIndexQuery<TYPE_DATETIME>::CppType
        InvertedIndexQuery<TYPE_DATETIME>::TYPE_MAX = 99991231235959L;
template <>
const typename InvertedIndexQuery<TYPE_DATETIME>::CppType
        InvertedIndexQuery<TYPE_DATETIME>::TYPE_MIN = 101000000;

template <PrimitiveType Type>
Status InvertedIndexQuery<Type>::add_value(InvertedIndexQueryOp op, const CppType& value) {
    if (is_match_query(op) || is_equal_query(op)) {
        return add_fixed_value(op, value);
    }
    if (_high_value > _low_value) {
        switch (op) {
        case InvertedIndexQueryOp::GREATER_THAN_QUERY: {
            if (value >= _low_value) {
                _low_value = value;
                _low_value_encoded.clear();
                _value_key_coder->full_encode_ascending(&value, &_low_value_encoded);
                _low_op = op;
            }
            break;
        }

        case InvertedIndexQueryOp::GREATER_EQUAL_QUERY: {
            if (value > _low_value) {
                _low_value = value;
                _low_value_encoded.clear();
                _value_key_coder->full_encode_ascending(&value, &_low_value_encoded);
                _low_op = op;
            }
            break;
        }

        case InvertedIndexQueryOp::LESS_THAN_QUERY: {
            if (value <= _high_value) {
                _high_value = value;
                _high_value_encoded.clear();
                _value_key_coder->full_encode_ascending(&value, &_high_value_encoded);
                _high_op = op;
            }
            break;
        }

        case InvertedIndexQueryOp::LESS_EQUAL_QUERY: {
            if (value < _high_value) {
                _high_value = value;
                _high_value_encoded.clear();
                _value_key_coder->full_encode_ascending(&value, &_high_value_encoded);
                _high_op = op;
            }
            break;
        }

        default: {
            return Status::InternalError("Add value failed! Unsupported InvertedIndexQueryOp {}",
                                         op);
        }
        }
    }

    return Status::OK();
}

template <PrimitiveType Type>
Status InvertedIndexQuery<Type>::add_value(InvertedIndexQueryOp op, PredicateParams* params) {
    CppType value;
    from_string(params->value, value, params->precision, params->scale);

    if (is_match_query(op) || is_equal_query(op)) {
        return add_fixed_value(op, value);
    }
    if (_high_value > _low_value) {
        switch (op) {
        case InvertedIndexQueryOp::GREATER_THAN_QUERY: {
            if (value >= _low_value) {
                _low_value = value;
                //_low_value_str = value_str;
                // NOTE:full_encode_ascending will append encoded data to the end of buffer.
                // so we need to clear buffer first.
                _low_value_encoded.clear();
                _value_key_coder->full_encode_ascending(&value, &_low_value_encoded);
                _low_op = op;
            }
            break;
        }

        case InvertedIndexQueryOp::GREATER_EQUAL_QUERY: {
            if (value > _low_value) {
                _low_value = value;
                _low_value_encoded.clear();
                _value_key_coder->full_encode_ascending(&value, &_low_value_encoded);
                _low_op = op;
            }
            break;
        }

        case InvertedIndexQueryOp::LESS_THAN_QUERY: {
            if (value <= _high_value) {
                _high_value = value;
                _high_value_encoded.clear();
                _value_key_coder->full_encode_ascending(&value, &_high_value_encoded);
                _high_op = op;
            }
            break;
        }

        case InvertedIndexQueryOp::LESS_EQUAL_QUERY: {
            if (value < _high_value) {
                _high_value = value;
                _high_value_encoded.clear();
                _value_key_coder->full_encode_ascending(&value, &_high_value_encoded);
                _high_op = op;
            }
            break;
        }

        default: {
            return Status::InternalError(
                    "Add value string fail! Unsupported InvertedIndexQueryOp {}", op);
        }
        }
    }

    return Status::OK();
}

template <PrimitiveType Type>
Status InvertedIndexQuery<Type>::from_string(const std::string& str_value, CppType& value,
                                             int precision, int scale) {
    StringParser::ParseResult result;
    if constexpr (Type == TYPE_TINYINT || Type == TYPE_SMALLINT || Type == TYPE_INT ||
                  Type == TYPE_BIGINT || Type == TYPE_LARGEINT) {
        std::from_chars(str_value.data(), str_value.data() + str_value.size(), value);
    } else if constexpr (Type == TYPE_FLOAT) {
        value = std::stof(str_value, nullptr);
    } else if constexpr (Type == TYPE_DOUBLE) {
        value = std::stod(str_value, nullptr);
    } else if constexpr (Type == TYPE_DATE) {
        value = timestamp_from_date(str_value);
    } else if constexpr (Type == TYPE_DATEV2) {
        value = timestamp_from_date_v2(str_value);
    } else if constexpr (Type == TYPE_DATETIME) {
        value = timestamp_from_datetime(str_value);
    } else if constexpr (Type == TYPE_DATETIMEV2) {
        value = timestamp_from_datetime_v2(str_value);
    } else if constexpr (Type == TYPE_CHAR || Type == TYPE_VARCHAR || Type == TYPE_STRING) {
        value = str_value;
    } else if constexpr (Type == TYPE_BOOLEAN) {
        value = StringParser::string_to_bool(str_value.c_str(), str_value.size(), &result);
    } else if constexpr (Type == TYPE_DECIMALV2) {
        decimal12_t tmp = {precision, scale};
        value.from_string(str_value);
        value = tmp;
    } else if constexpr (Type == TYPE_DECIMAL32 || Type == TYPE_DECIMAL64 ||
                         Type == TYPE_DECIMAL128I) {
        value = StringParser::string_to_decimal<int128_t>(str_value.data(), str_value.size(),
                                                          precision, scale, &result);
    } else {
        return Status::InternalError("from_string fail! Unsupported primitive type {}.", Type);
    }

    return Status::OK();
}

template class InvertedIndexQuery<TYPE_BOOLEAN>;
template class InvertedIndexQuery<TYPE_TINYINT>;
template class InvertedIndexQuery<TYPE_SMALLINT>;
template class InvertedIndexQuery<TYPE_INT>;
template class InvertedIndexQuery<TYPE_BIGINT>;
template class InvertedIndexQuery<TYPE_LARGEINT>;
template class InvertedIndexQuery<TYPE_FLOAT>;
template class InvertedIndexQuery<TYPE_DOUBLE>;
template class InvertedIndexQuery<TYPE_DECIMALV2>;
template class InvertedIndexQuery<TYPE_DATEV2>;
template class InvertedIndexQuery<TYPE_DATE>;
template class InvertedIndexQuery<TYPE_DATETIME>;
template class InvertedIndexQuery<TYPE_DATETIMEV2>;
template class InvertedIndexQuery<TYPE_CHAR>;
template class InvertedIndexQuery<TYPE_VARCHAR>;
template class InvertedIndexQuery<TYPE_STRING>;
template class InvertedIndexQuery<TYPE_DECIMAL32>;
template class InvertedIndexQuery<TYPE_DECIMAL64>;
template class InvertedIndexQuery<TYPE_DECIMAL128I>;

} // namespace segment_v2
} // namespace doris