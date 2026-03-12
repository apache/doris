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

#include <cstring>

#include "common/consts.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "core/value/vdatetime_value.h"
#include "storage/index/primary_key_index.h"
#include "storage/index/short_key_index.h"
#include "storage/olap_common.h"
#include "storage/row_cursor.h"
#include "storage/segment/column_writer.h"
#include "storage/segment/segment_writer.h"
#include "util/slice.h"

namespace doris::segment_v2 {

// Test-only subclass that provides RowCursor-based append_row.
// Production code should only use SegmentWriter::append_block.
// SegmentWriter declares TestSegmentWriter as a friend so we can access private members.
class TestSegmentWriter : public SegmentWriter {
public:
    using SegmentWriter::SegmentWriter;

    Status append_row(const RowCursor& row) {
        for (size_t cid = 0; cid < _column_writers.size(); ++cid) {
            const auto& f = row.field(cid);
            if (f.is_null()) {
                RETURN_IF_ERROR(_column_writers[cid]->append(true, nullptr));
                continue;
            }
            auto ft = row.schema()->column(cid)->type();
            // Convert Field to storage format that ColumnWriter expects
            alignas(16) char buf[sizeof(__int128)];
            const void* ptr = nullptr;
            switch (ft) {
            case FieldType::OLAP_FIELD_TYPE_CHAR:
            case FieldType::OLAP_FIELD_TYPE_VARCHAR:
            case FieldType::OLAP_FIELD_TYPE_STRING: {
                const auto& s = f.get<TYPE_STRING>();
                Slice slice(s.data(), s.size());
                RETURN_IF_ERROR(_column_writers[cid]->append(false, &slice));
                continue;
            }
            case FieldType::OLAP_FIELD_TYPE_DATE: {
                auto v = f.get<TYPE_DATE>().to_olap_date();
                memcpy(buf, &v, sizeof(v));
                ptr = buf;
                break;
            }
            case FieldType::OLAP_FIELD_TYPE_DATETIME: {
                auto v = f.get<TYPE_DATETIME>().to_olap_datetime();
                memcpy(buf, &v, sizeof(v));
                ptr = buf;
                break;
            }
            case FieldType::OLAP_FIELD_TYPE_DECIMAL: {
                auto v = PrimitiveTypeConvertor<TYPE_DECIMALV2>::to_storage_field_type(
                        f.get<TYPE_DECIMALV2>());
                memcpy(buf, &v, sizeof(v));
                ptr = buf;
                break;
            }
#define FIXED_LEN_CASE(OLAP_TYPE, PTYPE)                      \
    case FieldType::OLAP_TYPE:                                \
        ptr = reinterpret_cast<const void*>(&f.get<PTYPE>()); \
        break;
                FIXED_LEN_CASE(OLAP_FIELD_TYPE_BOOL, TYPE_BOOLEAN)
                FIXED_LEN_CASE(OLAP_FIELD_TYPE_TINYINT, TYPE_TINYINT)
                FIXED_LEN_CASE(OLAP_FIELD_TYPE_SMALLINT, TYPE_SMALLINT)
                FIXED_LEN_CASE(OLAP_FIELD_TYPE_INT, TYPE_INT)
                FIXED_LEN_CASE(OLAP_FIELD_TYPE_BIGINT, TYPE_BIGINT)
                FIXED_LEN_CASE(OLAP_FIELD_TYPE_LARGEINT, TYPE_LARGEINT)
                FIXED_LEN_CASE(OLAP_FIELD_TYPE_FLOAT, TYPE_FLOAT)
                FIXED_LEN_CASE(OLAP_FIELD_TYPE_DOUBLE, TYPE_DOUBLE)
                FIXED_LEN_CASE(OLAP_FIELD_TYPE_DATEV2, TYPE_DATEV2)
                FIXED_LEN_CASE(OLAP_FIELD_TYPE_DATETIMEV2, TYPE_DATETIMEV2)
                FIXED_LEN_CASE(OLAP_FIELD_TYPE_TIMESTAMPTZ, TYPE_TIMESTAMPTZ)
                FIXED_LEN_CASE(OLAP_FIELD_TYPE_DECIMAL32, TYPE_DECIMAL32)
                FIXED_LEN_CASE(OLAP_FIELD_TYPE_DECIMAL64, TYPE_DECIMAL64)
                FIXED_LEN_CASE(OLAP_FIELD_TYPE_DECIMAL128I, TYPE_DECIMAL128I)
                FIXED_LEN_CASE(OLAP_FIELD_TYPE_DECIMAL256, TYPE_DECIMAL256)
                FIXED_LEN_CASE(OLAP_FIELD_TYPE_IPV4, TYPE_IPV4)
                FIXED_LEN_CASE(OLAP_FIELD_TYPE_IPV6, TYPE_IPV6)
#undef FIXED_LEN_CASE
            default:
                return Status::InternalError("Unsupported field type in append_row: {}", int(ft));
            }
            RETURN_IF_ERROR(_column_writers[cid]->append(false, const_cast<void*>(ptr)));
        }
        std::string full_encoded_key;
        row.encode_key<true>(&full_encoded_key, _num_sort_key_columns);
        if (_tablet_schema->has_sequence_col()) {
            full_encoded_key.push_back(KeyConsts::KEY_NORMAL_MARKER);
            auto cid = _tablet_schema->sequence_col_idx();
            row.encode_single_field(cid, &full_encoded_key, true /*full_encode*/);
        }

        if (_is_mow_with_cluster_key()) {
            return Status::InternalError(
                    "TestSegmentWriter::append_row does not support mow tables with cluster key");
        } else if (_is_mow()) {
            RETURN_IF_ERROR(_primary_key_index_builder->add_item(full_encoded_key));
        } else {
            // At the beginning of one block, so add a short key index entry
            if ((_num_rows_written % _opts.num_rows_per_block) == 0) {
                std::string encoded_key;
                row.encode_key(&encoded_key, _num_short_key_columns);
                RETURN_IF_ERROR(_short_key_index_builder->add_item(encoded_key));
            }
            set_min_max_key(full_encoded_key);
        }
        ++_num_rows_written;
        return Status::OK();
    }
};

} // namespace doris::segment_v2
