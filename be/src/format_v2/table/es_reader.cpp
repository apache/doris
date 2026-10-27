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

#include "format_v2/table/es_reader.h"

#include <utility>

#include "core/block/block.h"
#include "exec/es/es_scan_reader.h"
#include "exec/es/es_scroll_parser.h"
#include "exec/es/es_scroll_query.h"
#include "format_v2/column_mapper.h"
#include "format_v2/materialized_reader_util.h"
#include "runtime/descriptors.h"
#include "runtime/file_scan_profile.h"
#include "runtime/runtime_state.h"

namespace doris::format::es {
namespace {

Status validate_es_range(const TFileRangeDesc& range) {
    if (!range.__isset.table_format_params || range.table_format_params.table_format_type != "es" ||
        !range.table_format_params.__isset.es_params) {
        return Status::InvalidArgument("ES v2 reader requires es table format and es_params");
    }
    return Status::OK();
}

class HttpScrollStream final : public EsScrollStream {
public:
    HttpScrollStream(const std::string& host, const std::map<std::string, std::string>& properties,
                     bool docvalues)
            : _reader(host, properties, docvalues) {}
    Status open() override { return _reader.open(); }
    Status next(bool* eof, std::unique_ptr<ScrollParser>& parser) override {
        return _reader.get_next(eof, parser);
    }
    Status close() override { return _reader.close(); }

private:
    ESScanReader _reader;
};

} // namespace

EsFileReader::EsFileReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                           std::unique_ptr<io::FileDescription>& file_description,
                           std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
                           TFileRangeDesc range, const TFileScanRangeParams& params,
                           std::vector<SlotDescriptor*> file_slot_descs,
                           EsScrollStreamFactory stream_factory)
        : FileReader(system_properties, file_description, std::move(io_ctx), profile),
          _range(std::move(range)),
          _params(params),
          _file_slots(std::move(file_slot_descs)),
          _stream_factory(std::move(stream_factory)) {}

EsFileReader::~EsFileReader() {
    WARN_IF_ERROR(close(), "Failed to close ES scroll stream");
}

void EsFileReader::_init_profile() {
    if (_profile == nullptr) {
        return;
    }
    _io_time = file_scan_profile::ensure_hierarchy(_profile).io;
    const char* parent = "EsFileReader";
    _total_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, parent, file_scan_profile::FILE_READER, 1);
    _read_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "EsReadTime", parent, 1);
    _materialize_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "EsMaterializeTime", parent, 1);
    _filter_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "EsFilterTime", parent, 1);
    _batches_read = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "EsBatchesRead", TUnit::UNIT, parent, 1);
    _rows_read = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "EsRowsRead", TUnit::UNIT, parent, 1);
}

Status EsFileReader::init(RuntimeState* state) {
    DORIS_CHECK(state != nullptr);
    _state = state;
    set_batch_size(state->batch_size());
    _init_profile();
    RETURN_IF_ERROR(validate_es_range(_range));
    _eof = false;
    return Status::OK();
}

Status EsFileReader::get_schema(std::vector<ColumnDefinition>* schema) const {
    DORIS_CHECK(schema != nullptr);
    schema->clear();
    schema->reserve(_file_slots.size());
    for (size_t i = 0; i < _file_slots.size(); ++i) {
        const auto* slot = _file_slots[i];
        schema->push_back({.identifier = Field::create_field<TYPE_INT>(cast_set<int32_t>(i)),
                           .local_id = cast_set<int32_t>(i),
                           .name = slot->col_name(),
                           .type = slot->type(),
                           .children = materialized_schema_children(slot->type())});
    }
    return Status::OK();
}

std::unique_ptr<TableColumnMapper> EsFileReader::create_column_mapper(
        TableColumnMapperOptions options) const {
    return std::make_unique<MaterializedColumnMapper>(options);
}

void EsFileReader::set_batch_size(size_t batch_size) {
    DORIS_CHECK(batch_size > 0);
    _batch_size = batch_size;
}

Status EsFileReader::open(std::shared_ptr<FileScanRequest> request) {
    SCOPED_TIMER(_total_time);
    RETURN_IF_ERROR(FileReader::open(std::move(request)));
    auto status = _open_stream();
    if (!status.ok()) {
        WARN_IF_ERROR(close(), "Failed to clear ES scroll after open error");
    }
    return status;
}

Status EsFileReader::_open_stream() {
    DORIS_CHECK(_stream == nullptr);
    RETURN_IF_CANCELLED(_state);
    if (_io_ctx != nullptr && _io_ctx->should_stop) {
        _eof = true;
        return Status::OK();
    }
    std::vector<std::string> names;
    for (const auto& [id, position] : _request->local_positions) {
        DORIS_CHECK(id.value() >= 0 && id.value() < _file_slots.size());
        _read_slots.push_back(_file_slots[id.value()]);
        _read_positions.push_back(position.value());
        names.push_back(_file_slots[id.value()]->col_name());
    }
    auto properties = _params.es_properties;
    for (const auto& [key, value] : _range.table_format_params.es_params) {
        properties[key] = value;
    }
    properties[ESScanReader::KEY_BATCH_SIZE] = std::to_string(_batch_size);
    _docvalue_context = _params.es_docvalue_context;
    // A source-only field such as _id cannot use doc values, even when other requested fields can.
    // The mapper may retain hidden predicate columns beyond the FE's original output projection.
    for (const auto& name : names) {
        if (name == "_id" || !_docvalue_context.contains(name)) {
            properties[ESScanReader::KEY_DOC_VALUES_MODE] = "0";
            break;
        }
    }
    const auto host = ESScanReader::select_host(properties);
    for (const auto* key :
         {ESScanReader::KEY_INDEX, ESScanReader::KEY_SHARD, ESScanReader::KEY_QUERY_DSL}) {
        if (!properties.contains(key) || properties.at(key).empty()) {
            return Status::InvalidArgument("ES v2 reader requires property {}", key);
        }
    }
    if (host.empty()) {
        return Status::InvalidArgument("ES v2 reader requires a host");
    }
    properties[ESScanReader::KEY_HOST_PORT] = host;
    bool docvalues = false;
    properties[ESScanReader::KEY_QUERY] =
            ESScrollQueryBuilder::build(properties, names, _docvalue_context, &docvalues);
    _stream = _stream_factory ? _stream_factory(host, properties, docvalues)
                              : std::make_unique<HttpScrollStream>(host, properties, docvalues);
    DORIS_CHECK(_stream != nullptr);
    {
        SCOPED_TIMER(_io_time);
        SCOPED_TIMER(_read_time);
        RETURN_IF_ERROR(_stream->open());
    }
    // ESScanReader caches the first response in open(). Parse it now to capture the scroll ID so
    // LIMIT/cancellation before the first block can still clear the server-side search context.
    RETURN_IF_ERROR(_next_page());
    _eof = false;
    return Status::OK();
}

Status EsFileReader::_next_page() {
    SCOPED_TIMER(_io_time);
    SCOPED_TIMER(_read_time);
    RETURN_IF_ERROR(_stream->next(&_source_eof, _parser));
    _page_rows_remaining = _source_eof ? 0 : _parser->get_size();
    if (!_source_eof && _batches_read != nullptr) {
        COUNTER_UPDATE(_batches_read, 1);
    }
    return Status::OK();
}

Status EsFileReader::get_block(Block* block, size_t* rows, bool* eof) {
    SCOPED_TIMER(_total_time);
    DORIS_CHECK(block != nullptr && rows != nullptr && eof != nullptr);
    *rows = 0;
    *eof = false;
    if (_eof) {
        *eof = true;
        return Status::OK();
    }
    auto status = _read_block(block, rows, eof);
    if (!status.ok()) {
        WARN_IF_ERROR(close(), "Failed to clear ES scroll after read error");
        return status;
    }
    if (*eof) {
        return close();
    }
    return Status::OK();
}

Status EsFileReader::_read_block(Block* block, size_t* rows, bool* eof) {
    {
        auto guard = block->mutate_columns_scoped();
        auto& columns = guard.mutable_columns();
        while (*rows < _batch_size) {
            RETURN_IF_CANCELLED(_state);
            if (_io_ctx != nullptr && _io_ctx->should_stop) {
                *eof = true;
                return Status::OK();
            }
            if (_page_rows_remaining == 0) {
                if (_source_eof) {
                    break;
                }
                RETURN_IF_ERROR(_next_page());
                if (_page_rows_remaining == 0) {
                    continue;
                }
            }
            SCOPED_TIMER(_materialize_time);
            bool line_eof = false;
            // The parser sees physical slots only; the request owns their file-local block order.
            RETURN_IF_ERROR(_parser->fill_columns(_read_slots, columns, &line_eof,
                                                  _docvalue_context, _state->timezone_obj(),
                                                  _read_positions));
            DORIS_CHECK(!line_eof);
            --_page_rows_remaining;
            ++*rows;
        }
    }
    _record_scan_rows(cast_set<int64_t>(*rows));
    if (_rows_read != nullptr) {
        COUNTER_UPDATE(_rows_read, *rows);
    }
    *eof = _source_eof && *rows == 0;
    SCOPED_TIMER(_filter_time);
    return apply_materialized_reader_filters(_request.get(), _io_ctx.get(), block, rows);
}

Status EsFileReader::close() {
    auto stream = std::move(_stream);
    _parser.reset();
    _request.reset();
    _read_slots.clear();
    _read_positions.clear();
    _page_rows_remaining = 0;
    _source_eof = true;
    _eof = true;
    // Release ownership before closing so a failed DELETE is reported once, including destruction.
    return stream != nullptr ? stream->close() : Status::OK();
}

EsReader::EsReader(EsScrollStreamFactory stream_factory)
        : _stream_factory(std::move(stream_factory)) {}

Status EsReader::init(TableReadOptions&& options) {
    DORIS_CHECK(options.file_slot_descs != nullptr && options.scan_params != nullptr);
    return TableReader::init(std::move(options));
}

Status EsReader::prepare_split(const SplitReadOptions& options) {
    RETURN_IF_ERROR(validate_es_range(options.current_range));
    return TableReader::prepare_split(options);
}

Status EsReader::create_file_reader(std::unique_ptr<FileReader>* reader) {
    *reader = std::make_unique<EsFileReader>(_system_properties, _current_task->data_file, _io_ctx,
                                             _scanner_profile, _current_file_range_desc,
                                             *_scan_params, *_file_slot_descs, _stream_factory);
    return Status::OK();
}

} // namespace doris::format::es
