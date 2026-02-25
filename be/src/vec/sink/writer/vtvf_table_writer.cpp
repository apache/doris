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

#include "vec/sink/writer/vtvf_table_writer.h"

#include <fmt/format.h>

#include "common/status.h"
#include "io/file_factory.h"
#include "runtime/runtime_state.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

VTVFTableWriter::VTVFTableWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs,
                                 std::shared_ptr<pipeline::Dependency> dep,
                                 std::shared_ptr<pipeline::Dependency> fin_dep)
        : AsyncResultWriter(output_exprs, dep, fin_dep) {
    _tvf_sink = t_sink.tvf_table_sink;
}

Status VTVFTableWriter::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;

    // Init profile counters
    RuntimeProfile* writer_profile = profile->create_child("VTVFTableWriter", true, true);
    _written_rows_counter = ADD_COUNTER(writer_profile, "NumWrittenRows", TUnit::UNIT);
    _written_data_bytes = ADD_COUNTER(writer_profile, "WrittenDataBytes", TUnit::BYTES);
    _file_write_timer = ADD_TIMER(writer_profile, "FileWriteTime");
    _writer_close_timer = ADD_TIMER(writer_profile, "FileWriterCloseTime");

    _file_path = _tvf_sink.file_path;
    _max_file_size_bytes =
            _tvf_sink.__isset.max_file_size_bytes ? _tvf_sink.max_file_size_bytes : 0;

    VLOG_DEBUG << "TVF table writer open, query_id=" << print_id(_state->query_id())
               << ", tvf_name=" << _tvf_sink.tvf_name << ", file_path=" << _tvf_sink.file_path
               << ", file_format=" << _tvf_sink.file_format << ", file_type=" << _tvf_sink.file_type
               << ", max_file_size_bytes=" << _max_file_size_bytes
               << ", columns_count=" << (_tvf_sink.__isset.columns ? _tvf_sink.columns.size() : 0);

    return _create_next_file_writer();
}

Status VTVFTableWriter::write(RuntimeState* state, vectorized::Block& block) {
    COUNTER_UPDATE(_written_rows_counter, block.rows());
    state->update_num_rows_load_total(block.rows());

    {
        SCOPED_TIMER(_file_write_timer);
        RETURN_IF_ERROR(_vfile_writer->write(block));
    }

    _current_written_bytes = _vfile_writer->written_len();

    // Auto-split if max file size is set
    if (_max_file_size_bytes > 0) {
        RETURN_IF_ERROR(_create_new_file_if_exceed_size());
    }

    return Status::OK();
}

Status VTVFTableWriter::close(Status status) {
    if (!status.ok()) {
        return status;
    }

    SCOPED_TIMER(_writer_close_timer);
    return _close_file_writer(true);
}

Status VTVFTableWriter::_create_file_writer(const std::string& file_name) {
    bool use_jni = _tvf_sink.__isset.writer_type && _tvf_sink.writer_type == TTVFWriterType::JNI;

    if (!use_jni) {
        // Native path: create file writer via FileFactory
        TFileType::type file_type = _tvf_sink.file_type;
        std::map<std::string, std::string> properties;
        if (_tvf_sink.__isset.properties) {
            properties = _tvf_sink.properties;
        }

        _file_writer_impl = DORIS_TRY(FileFactory::create_file_writer(
                file_type, _state->exec_env(), {}, properties, file_name,
                {.write_file_cache = false, .sync_file_data = false}));
    }

    // Factory creates either JNI or native transformer
    RETURN_IF_ERROR(create_tvf_format_transformer(_tvf_sink, _state,
                                                  use_jni ? nullptr : _file_writer_impl.get(),
                                                  _vec_output_expr_ctxs, &_vfile_writer));

    VLOG_DEBUG << "TVF table writer created file: " << file_name
               << ", format=" << _tvf_sink.file_format << ", use_jni=" << use_jni
               << ", query_id=" << print_id(_state->query_id());

    return _vfile_writer->open();
}

Status VTVFTableWriter::_create_next_file_writer() {
    std::string file_name;
    RETURN_IF_ERROR(_get_next_file_name(&file_name));
    return _create_file_writer(file_name);
}

Status VTVFTableWriter::_close_file_writer(bool done) {
    if (_vfile_writer) {
        RETURN_IF_ERROR(_vfile_writer->close());
        COUNTER_UPDATE(_written_data_bytes, _vfile_writer->written_len());
        _vfile_writer.reset(nullptr);
    } else if (_file_writer_impl && _file_writer_impl->state() != io::FileWriter::State::CLOSED) {
        RETURN_IF_ERROR(_file_writer_impl->close());
    }

    if (!done) {
        RETURN_IF_ERROR(_create_next_file_writer());
    }
    return Status::OK();
}

Status VTVFTableWriter::_create_new_file_if_exceed_size() {
    if (_max_file_size_bytes <= 0 || _current_written_bytes < _max_file_size_bytes) {
        return Status::OK();
    }
    SCOPED_TIMER(_writer_close_timer);
    RETURN_IF_ERROR(_close_file_writer(false));
    _current_written_bytes = 0;
    return Status::OK();
}

Status VTVFTableWriter::_get_next_file_name(std::string* file_name) {
    std::string ext;
    switch (_tvf_sink.file_format) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
        ext = "csv";
        break;
    case TFileFormatType::FORMAT_PARQUET:
        ext = "parquet";
        break;
    case TFileFormatType::FORMAT_ORC:
        ext = "orc";
        break;
    default:
        ext = "dat";
        break;
    }

    // file_path is a prefix, generate: {prefix}{query_id}_{idx}.{ext}
    std::string query_id_str = print_id(_state->query_id());
    *file_name = fmt::format("{}{}_{}.{}", _file_path, query_id_str, _file_idx, ext);
    _file_idx++;
    return Status::OK();
}

} // namespace doris::vectorized
