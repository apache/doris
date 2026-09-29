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

#include "common/consts.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "storage/index/primary_key_index.h"
#include "storage/index/short_key_index.h"
#include "storage/olap_common.h"
#include "storage/row_cursor.h"
#include "storage/segment/column_writer.h"
#include "storage/segment/vertical_segment_writer.h"

namespace doris::segment_v2 {

// Test-only subclass with a RowCursor-based append_row. Production code feeds
// the writer through append_block or write_block. TestVerticalSegmentWriter is
// a friend of VerticalSegmentWriter, so it can reach its private members.
class TestVerticalSegmentWriter : public VerticalSegmentWriter {
public:
    using VerticalSegmentWriter::VerticalSegmentWriter;

    // Each Field becomes a one-row column of the column's data type, so every
    // value takes the production path: a NaN canonicalised, the null bit
    // recorded.
    Status append_row(const RowCursor& row) {
        for (size_t cid = 0; cid < _column_writers.size(); ++cid) {
            const TabletColumn& column = *row.schema()->column(cid);
            auto data_type =
                    DataTypeFactory::instance().create_data_type(column, column.is_nullable());
            auto one_row = data_type->create_column();
            one_row->insert(row.field(cid));
            RETURN_IF_ERROR(_column_writers[cid]->append(*one_row, 0, 1));
        }
        std::string full_encoded_key;
        row.encode_key<true>(&full_encoded_key, _tablet_schema->num_key_columns());
        if (_tablet_schema->has_sequence_col()) {
            full_encoded_key.push_back(KeyConsts::KEY_NORMAL_MARKER);
            auto cid = _tablet_schema->sequence_col_idx();
            row.encode_single_field(cid, &full_encoded_key, true /*full_encode*/);
        }

        if (_is_mow_with_cluster_key()) {
            return Status::InternalError(
                    "TestVerticalSegmentWriter::append_row does not support mow tables with "
                    "cluster key");
        } else if (_is_mow()) {
            RETURN_IF_ERROR(_primary_key_index_builder->add_item(full_encoded_key));
        } else {
            // At the beginning of one block, so add a short key index entry
            if ((_num_rows_written % _opts.num_rows_per_block) == 0) {
                std::string encoded_key;
                row.encode_key(&encoded_key, _tablet_schema->num_short_key_columns());
                RETURN_IF_ERROR(_short_key_index_builder->add_item(encoded_key));
            }
            _set_min_max_key(full_encoded_key);
        }
        ++_num_rows_written;
        return Status::OK();
    }
};

} // namespace doris::segment_v2
