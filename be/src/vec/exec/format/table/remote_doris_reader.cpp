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

#include "remote_doris_reader.h"

#include <iostream>
#include <map>
#include <memory>
#include <string>

#include "arrow/flight/client.h"
#include "arrow/flight/types.h"
#include "arrow/ipc/reader.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "common/status.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "util/arrow/utils.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"

RemoteDorisReader::RemoteDorisReader(const std::vector<SlotDescriptor*>& file_slot_descs,
                                     RuntimeState* state, RuntimeProfile* profile,
                                     const TFileRangeDesc& range)
        : _range(range), _file_slot_descs(file_slot_descs) {
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, _ctzz);
}

Status RemoteDorisReader::init_reader() {
    RETURN_DORIS_STATUS_IF_ERROR(init_stream());
    DCHECK(_stream != nullptr);
    return Status::OK();
}

Status RemoteDorisReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    arrow::flight::FlightStreamChunk chunk;
    RETURN_DORIS_STATUS_IF_ERROR(_stream->Next().Value(&chunk));

    if (!chunk.data) {
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }

    // convert arrow batch to block
    auto batch = chunk.data;
    auto num_rows = batch->num_rows();
    auto num_columns = batch->num_columns();
    for (int c = 0; c < num_columns; ++c) {
        arrow::Array* column = batch->column(c).get();

        std::string column_name = batch->schema()->field(c)->name();
        if (!_col_name_to_block_idx->contains(column_name)) {
            return Status::InternalError("column {} not found in block {}", column_name,
                                         block->dump_structure());
        }

        try {
            const vectorized::ColumnWithTypeAndName& column_with_name =
                    block->get_by_position((*_col_name_to_block_idx)[column_name]);
            RETURN_IF_ERROR(column_with_name.type->get_serde()->read_column_from_arrow(
                    column_with_name.column->assume_mutable_ref(), column, 0, num_rows, _ctzz));
        } catch (Exception& e) {
            return Status::InternalError(
                    "Failed to convert from arrow to block, column_name: {}, e: {}", column_name,
                    e.what());
        }
    }

    *read_rows += num_rows;
    return Status::OK();
}

Status RemoteDorisReader::get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type,
                                      std::unordered_set<std::string>* missing_cols) {
    for (const auto& slot : _file_slot_descs) {
        name_to_type->emplace(slot->col_name(), slot->type());
    }
    return Status::OK();
}

Status RemoteDorisReader::close() {
    RETURN_DORIS_STATUS_IF_ERROR(_flight_client->Close());
    return Status::OK();
}

arrow::Status RemoteDorisReader::init_stream() {
    ARROW_ASSIGN_OR_RAISE(auto location,
                          arrow::flight::Location::Parse(
                                  _range.table_format_params.remote_doris_params.location_uri));
    ARROW_ASSIGN_OR_RAISE(auto ticket,
                          arrow::flight::Ticket::Deserialize(
                                  _range.table_format_params.remote_doris_params.ticket));
    ARROW_ASSIGN_OR_RAISE(_flight_client, arrow::flight::FlightClient::Connect(location));
    ARROW_ASSIGN_OR_RAISE(_stream, _flight_client->DoGet(ticket));

    return arrow::Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
