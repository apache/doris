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

#include "format_v2/file_reader.h"

#include <sstream>

#include "format_v2/column_mapper.h"
#include "io/fs/buffered_reader.h"
#include "io/fs/tracing_file_reader.h"
#include "runtime/runtime_state.h"

namespace doris::format {
namespace {

template <typename T, typename Formatter>
std::string join_debug_strings(const std::vector<T>& values, Formatter formatter) {
    std::ostringstream out;
    out << "[";
    for (size_t i = 0; i < values.size(); ++i) {
        if (i > 0) {
            out << ", ";
        }
        out << formatter(values[i]);
    }
    out << "]";
    return out.str();
}

} // namespace

std::string FileScanRequest::debug_string() const {
    std::ostringstream out;
    out << "FileScanRequest{predicate_columns="
        << join_debug_strings(
                   predicate_columns,
                   [](const LocalColumnIndex& projection) { return projection.debug_string(); })
        << ", non_predicate_columns="
        << join_debug_strings(
                   non_predicate_columns,
                   [](const LocalColumnIndex& projection) { return projection.debug_string(); })
        << ", local_positions={";
    size_t position_idx = 0;
    for (const auto& [column_id, block_position] : local_positions) {
        if (position_idx++ > 0) {
            out << ", ";
        }
        out << column_id << ":" << block_position;
    }
    out << "}, conjunct_count=" << conjuncts.size()
        << ", delete_conjunct_count=" << delete_conjuncts.size() << "}";
    return out.str();
}

Status FileReader::init(RuntimeState* state) {
    _init_profile();
    SCOPED_RAW_TIMER(&_reader_statistics.file_reader_create_time);
    ++_reader_statistics.open_file_num;
    io::FileReaderOptions reader_options =
            FileFactory::get_reader_options(state->query_options(), *_file_description);
    _file_reader = DORIS_TRY(io::DelegateReader::create_file_reader(
            _profile, *_system_properties, *_file_description, reader_options,
            io::DelegateReader::AccessMode::RANDOM, _io_ctx));
    // IOContext can be present without file_reader_stats in standalone tests or callers that only
    // need extra IO state. TracingFileReader dereferences the stats pointer on every read, so only
    // wrap the physical reader when stats collection is actually available.
    _tracing_file_reader = _io_ctx && _io_ctx->file_reader_stats
                                   ? std::make_shared<io::TracingFileReader>(
                                             _file_reader, _io_ctx->file_reader_stats)
                                   : _file_reader;
    _eof = false;
    return Status::OK();
}

std::unique_ptr<TableColumnMapper> FileReader::create_column_mapper(
        TableColumnMapperOptions options) const {
    return std::make_unique<TableColumnMapper>(std::move(options));
}

} // namespace doris::format
