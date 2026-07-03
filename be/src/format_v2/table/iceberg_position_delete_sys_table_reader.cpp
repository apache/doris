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

#include "format_v2/table/iceberg_position_delete_sys_table_reader.h"

#include <sstream>
#include <utility>

#include "core/block/block.h"
#include "format/table/iceberg_position_delete_sys_table_reader.h"
#include "runtime/runtime_state.h"

namespace doris::format::iceberg {

IcebergPositionDeleteSysTableV2Reader::~IcebergPositionDeleteSysTableV2Reader() = default;

Status IcebergPositionDeleteSysTableV2Reader::prepare_split(
        const format::SplitReadOptions& options) {
    RETURN_IF_ERROR(close());
    _current_range = options.current_range;
    _has_split = true;
    return Status::OK();
}

Status IcebergPositionDeleteSysTableV2Reader::get_block(Block* block, bool* eos) {
    SCOPED_TIMER(_profile.exec_timer);
    DORIS_CHECK(block != nullptr);
    DORIS_CHECK(eos != nullptr);
    DORIS_CHECK(block->columns() == _projected_columns.size());
    block->clear_column_data(_projected_columns.size());

    if (*eos) {
        return Status::OK();
    }
    if (!_has_split) {
        *eos = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(_open_reader());
    if (_batch_size > 0) {
        _reader->set_batch_size(_batch_size);
    }

    size_t read_rows = 0;
    bool reader_eof = false;
    RETURN_IF_ERROR(_reader->get_next_block(block, &read_rows, &reader_eof));
    if (reader_eof) {
        RETURN_IF_ERROR(close());
        if (read_rows == 0) {
            *eos = true;
            return Status::OK();
        }
    }
    *eos = false;
    return Status::OK();
}

Status IcebergPositionDeleteSysTableV2Reader::close() {
    if (_reader != nullptr) {
        _reader->collect_profile_before_close();
        RETURN_IF_ERROR(_reader->close());
        _reader.reset();
    }
    _has_split = false;
    return format::TableReader::close();
}

std::string IcebergPositionDeleteSysTableV2Reader::debug_string() const {
    std::ostringstream out;
    out << "IcebergPositionDeleteSysTableV2Reader{base=" << format::TableReader::debug_string()
        << ", has_split=" << _has_split << "}";
    return out.str();
}

Status IcebergPositionDeleteSysTableV2Reader::_open_reader() {
    if (_reader != nullptr) {
        return Status::OK();
    }
    if (!_has_split) {
        return Status::InternalError(
                "Iceberg position delete system table v2 reader has no prepared split");
    }
    if (_file_slot_descs == nullptr) {
        return Status::InvalidArgument(
                "Iceberg position delete system table v2 reader requires file slot descriptors");
    }

    std::unique_ptr<GenericReader> reader =
            ::doris::IcebergPositionDeleteSysTableReader::create_unique(
                    *_file_slot_descs, _runtime_state, _scanner_profile, _current_range,
                    _scan_params, nullptr);
    ReaderInitContext ctx;
    ctx.state = _runtime_state;
    ctx.params = _scan_params;
    ctx.range = &_current_range;
    ctx.push_down_agg_type = _push_down_agg_type;
    RETURN_IF_ERROR(reader->init_reader(&ctx));
    _reader = std::move(reader);
    return Status::OK();
}

} // namespace doris::format::iceberg
