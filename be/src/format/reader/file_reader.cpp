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

#include "format/reader/file_reader.h"

#include <sstream>

#include "io/fs/buffered_reader.h"
#include "io/fs/tracing_file_reader.h"
#include "runtime/runtime_state.h"

namespace doris::reader {
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

std::string int_vector_debug_string(const std::vector<int32_t>& values) {
    std::ostringstream out;
    out << "[";
    for (size_t i = 0; i < values.size(); ++i) {
        if (i > 0) {
            out << ", ";
        }
        out << values[i];
    }
    out << "]";
    return out.str();
}

} // namespace

std::string FileColumnPredicateFilter::debug_string() const {
    std::ostringstream out;
    out << "FileColumnPredicateFilter{file_column_id=" << file_column_id
        << ", file_child_id_path=" << int_vector_debug_string(file_child_id_path)
        << ", predicate_count=" << predicates.size() << "}";
    return out.str();
}

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
        << ", delete_conjunct_count=" << delete_conjuncts.size() << ", column_predicate_filters="
        << join_debug_strings(
                   column_predicate_filters,
                   [](const FileColumnPredicateFilter& filter) { return filter.debug_string(); })
        << "}";
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
    _tracing_file_reader = _io_ctx ? std::make_shared<io::TracingFileReader>(
                                             _file_reader, _io_ctx->file_reader_stats)
                                   : _file_reader;
    _eof = false;
    return Status::OK();
}

} // namespace doris::reader
