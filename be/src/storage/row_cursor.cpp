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

#include "storage/row_cursor.h"

#include <glog/logging.h>

#include <algorithm>
#include <numeric>
#include <ostream>

#include "common/cast_set.h"
#include "common/consts.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "storage/field.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/types.h"
#include "util/slice.h"

namespace doris {
#include "common/compile_check_begin.h"
using namespace ErrorCode;

RowCursor::RowCursor() = default;
RowCursor::~RowCursor() = default;
RowCursor::RowCursor(RowCursor&&) noexcept = default;
RowCursor& RowCursor::operator=(RowCursor&&) noexcept = default;

void RowCursor::_init_schema(TabletSchemaSPtr schema, uint32_t column_count) {
    std::vector<uint32_t> columns(column_count);
    std::iota(columns.begin(), columns.end(), 0);
    _schema.reset(new Schema(schema->columns(), columns));
}

void RowCursor::_init_schema(const std::shared_ptr<Schema>& shared_schema, uint32_t column_count) {
    _schema.reset(new Schema(*shared_schema));
}

Status RowCursor::init(TabletSchemaSPtr schema, size_t num_columns) {
    if (num_columns > schema->num_columns()) {
        return Status::Error<INVALID_ARGUMENT>(
                "Input param are invalid. Column count is bigger than num_columns of schema. "
                "column_count={}, schema.num_columns={}",
                num_columns, schema->num_columns());
    }
    _init_schema(schema, cast_set<uint32_t>(num_columns));
    // Initialize all fields as null (TYPE_NULL).
    _fields.resize(num_columns);
    return Status::OK();
}

Status RowCursor::init(TabletSchemaSPtr schema, const OlapTuple& tuple) {
    size_t key_size = tuple.size();
    if (key_size > schema->num_columns()) {
        return Status::Error<INVALID_ARGUMENT>(
                "Input param are invalid. Column count is bigger than num_columns of schema. "
                "column_count={}, schema.num_columns={}",
                key_size, schema->num_columns());
    }
    _init_schema(schema, cast_set<uint32_t>(key_size));
    return from_tuple(tuple);
}

Status RowCursor::init(TabletSchemaSPtr schema, const OlapTuple& tuple,
                       const std::shared_ptr<Schema>& shared_schema) {
    size_t key_size = tuple.size();
    if (key_size > schema->num_columns()) {
        return Status::Error<INVALID_ARGUMENT>(
                "Input param are invalid. Column count is bigger than num_columns of schema. "
                "column_count={}, schema.num_columns={}",
                key_size, schema->num_columns());
    }
    _init_schema(shared_schema, cast_set<uint32_t>(key_size));
    return from_tuple(tuple);
}

Status RowCursor::init_scan_key(TabletSchemaSPtr schema, std::vector<Field> fields) {
    size_t key_size = fields.size();
    if (key_size > schema->num_columns()) {
        return Status::Error<INVALID_ARGUMENT>(
                "Input param are invalid. Column count is bigger than num_columns of schema. "
                "column_count={}, schema.num_columns={}",
                key_size, schema->num_columns());
    }
    _init_schema(schema, cast_set<uint32_t>(key_size));
    _fields = std::move(fields);
    return Status::OK();
}

Status RowCursor::from_tuple(const OlapTuple& tuple) {
    if (tuple.size() != _schema->num_column_ids()) {
        return Status::Error<INVALID_ARGUMENT>(
                "column count does not match. tuple_size={}, field_count={}", tuple.size(),
                _schema->num_column_ids());
    }
    _fields.resize(tuple.size());
    for (size_t i = 0; i < tuple.size(); ++i) {
        _fields[i] = tuple.get_field(i);
    }
    return Status::OK();
}

RowCursor RowCursor::clone() const {
    RowCursor result;
    result._schema = std::make_unique<Schema>(*_schema);
    result._fields = _fields;
    return result;
}

void RowCursor::pad_char_fields() {
    for (size_t i = 0; i < _fields.size(); ++i) {
        const StorageField* col = _schema->column(cast_set<uint32_t>(i));
        if (col->type() == FieldType::OLAP_FIELD_TYPE_CHAR && !_fields[i].is_null()) {
            String padded = _fields[i].get<TYPE_CHAR>();
            padded.resize(col->length(), '\0');
            _fields[i] = Field::create_field<TYPE_CHAR>(std::move(padded));
        }
    }
}

std::string RowCursor::to_string() const {
    std::string result;
    for (size_t i = 0; i < _fields.size(); ++i) {
        if (i > 0) {
            result.append("|");
        }
        if (_fields[i].is_null()) {
            result.append("1&NULL");
        } else {
            result.append("0&");
            result.append(_fields[i].to_debug_string(
                    _schema->column(cast_set<uint32_t>(i))->get_scale()));
        }
    }
    return result;
}

// Convert a Field value to its storage representation via PrimitiveTypeConvertor and encode.
// For most types this is an identity conversion; for DATE, DATETIME, DECIMALV2 it does
// actual conversion to the olap storage format.
template <PrimitiveType PT>
static void encode_non_string_field(const StorageField* storage_field, const Field& f,
                                    bool full_encode, std::string* buf) {
    auto storage_val = PrimitiveTypeConvertor<PT>::to_storage_field_type(f.get<PT>());
    if (full_encode) {
        storage_field->full_encode_ascending(&storage_val, buf);
    } else {
        storage_field->encode_ascending(&storage_val, buf);
    }
}

void RowCursor::_encode_field(const StorageField* storage_field, const Field& f, bool full_encode,
                              std::string* buf) const {
    FieldType ft = storage_field->type();

    if (field_is_slice_type(ft)) {
        // String types: CHAR, VARCHAR, STRING — all stored as String in Field.
        const String& str = f.get<TYPE_STRING>();

        if (ft == FieldType::OLAP_FIELD_TYPE_CHAR) {
            // CHAR type: must pad with \0 to the declared column length
            size_t col_len = storage_field->length();
            String padded(col_len, '\0');
            memcpy(padded.data(), str.data(), std::min(str.size(), col_len));

            Slice slice(padded.data(), col_len);
            if (full_encode) {
                storage_field->full_encode_ascending(&slice, buf);
            } else {
                storage_field->encode_ascending(&slice, buf);
            }
        } else {
            // VARCHAR / STRING: use actual length
            Slice slice(str.data(), str.size());
            if (full_encode) {
                storage_field->full_encode_ascending(&slice, buf);
            } else {
                storage_field->encode_ascending(&slice, buf);
            }
        }
        return;
    }

    // Non-string types: convert Field value to storage format via PrimitiveTypeConvertor,
    // then encode. For most types this is an identity conversion.
    switch (ft) {
    case FieldType::OLAP_FIELD_TYPE_BOOL:
        encode_non_string_field<TYPE_BOOLEAN>(storage_field, f, full_encode, buf);
        break;
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        encode_non_string_field<TYPE_TINYINT>(storage_field, f, full_encode, buf);
        break;
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        encode_non_string_field<TYPE_SMALLINT>(storage_field, f, full_encode, buf);
        break;
    case FieldType::OLAP_FIELD_TYPE_INT:
        encode_non_string_field<TYPE_INT>(storage_field, f, full_encode, buf);
        break;
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        encode_non_string_field<TYPE_BIGINT>(storage_field, f, full_encode, buf);
        break;
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
        encode_non_string_field<TYPE_LARGEINT>(storage_field, f, full_encode, buf);
        break;
    case FieldType::OLAP_FIELD_TYPE_FLOAT:
        encode_non_string_field<TYPE_FLOAT>(storage_field, f, full_encode, buf);
        break;
    case FieldType::OLAP_FIELD_TYPE_DOUBLE:
        encode_non_string_field<TYPE_DOUBLE>(storage_field, f, full_encode, buf);
        break;
    case FieldType::OLAP_FIELD_TYPE_DATE:
        encode_non_string_field<TYPE_DATE>(storage_field, f, full_encode, buf);
        break;
    case FieldType::OLAP_FIELD_TYPE_DATETIME:
        encode_non_string_field<TYPE_DATETIME>(storage_field, f, full_encode, buf);
        break;
    case FieldType::OLAP_FIELD_TYPE_DATEV2:
        encode_non_string_field<TYPE_DATEV2>(storage_field, f, full_encode, buf);
        break;
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
        encode_non_string_field<TYPE_DATETIMEV2>(storage_field, f, full_encode, buf);
        break;
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ:
        encode_non_string_field<TYPE_TIMESTAMPTZ>(storage_field, f, full_encode, buf);
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL:
        encode_non_string_field<TYPE_DECIMALV2>(storage_field, f, full_encode, buf);
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
        encode_non_string_field<TYPE_DECIMAL32>(storage_field, f, full_encode, buf);
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
        encode_non_string_field<TYPE_DECIMAL64>(storage_field, f, full_encode, buf);
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
        encode_non_string_field<TYPE_DECIMAL128I>(storage_field, f, full_encode, buf);
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256:
        encode_non_string_field<TYPE_DECIMAL256>(storage_field, f, full_encode, buf);
        break;
    case FieldType::OLAP_FIELD_TYPE_IPV4:
        encode_non_string_field<TYPE_IPV4>(storage_field, f, full_encode, buf);
        break;
    case FieldType::OLAP_FIELD_TYPE_IPV6:
        encode_non_string_field<TYPE_IPV6>(storage_field, f, full_encode, buf);
        break;
    default:
        LOG(FATAL) << "unsupported field type for encoding: " << int(ft);
        break;
    }
}

template <bool is_mow>
void RowCursor::encode_key_with_padding(std::string* buf, size_t num_keys,
                                        bool padding_minimal) const {
    for (uint32_t cid = 0; cid < num_keys; cid++) {
        auto* storage_field = _schema->column(cid);
        if (storage_field == nullptr) {
            if (padding_minimal) {
                buf->push_back(KeyConsts::KEY_MINIMAL_MARKER);
            } else {
                if (is_mow) {
                    buf->push_back(KeyConsts::KEY_NORMAL_NEXT_MARKER);
                } else {
                    buf->push_back(KeyConsts::KEY_MAXIMAL_MARKER);
                }
            }
            break;
        }

        if (cid >= _fields.size() || _fields[cid].is_null()) {
            buf->push_back(KeyConsts::KEY_NULL_FIRST_MARKER);
            continue;
        }

        buf->push_back(KeyConsts::KEY_NORMAL_MARKER);
        _encode_field(storage_field, _fields[cid], is_mow, buf);
    }
}

// Explicit template instantiations
template void RowCursor::encode_key_with_padding<false>(std::string*, size_t, bool) const;
template void RowCursor::encode_key_with_padding<true>(std::string*, size_t, bool) const;

template <bool full_encode>
void RowCursor::encode_key(std::string* buf, size_t num_keys) const {
    for (uint32_t cid = 0; cid < num_keys; cid++) {
        if (cid >= _fields.size() || _fields[cid].is_null()) {
            buf->push_back(KeyConsts::KEY_NULL_FIRST_MARKER);
            continue;
        }
        buf->push_back(KeyConsts::KEY_NORMAL_MARKER);
        _encode_field(_schema->column(cid), _fields[cid], full_encode, buf);
    }
}

template void RowCursor::encode_key<false>(std::string*, size_t) const;
template void RowCursor::encode_key<true>(std::string*, size_t) const;

#include "common/compile_check_end.h"
} // namespace doris
