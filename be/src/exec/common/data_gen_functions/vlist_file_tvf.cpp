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

#include "exec/common/data_gen_functions/vlist_file_tvf.h"

#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>

#include <algorithm>
#include <array>

#include "core/data_type/data_type_file.h"
#include "core/field.h"
#include "core/value/file_value.h"
#include "io/file_factory.h"
#include "io/fs/s3_file_system.h"
#include "runtime/runtime_state.h"
#include "util/s3_uri.h"

namespace doris {
VListFileTVF::VListFileTVF(TupleId tuple_id, const TupleDescriptor* tuple_desc)
        : VDataGenFunctionInf(tuple_id, tuple_desc) {}

VListFileTVF::~VListFileTVF() = default;

Status VListFileTVF::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    DCHECK(scan_ranges.size() == 1);
    const auto& params = scan_ranges[0].scan_range.data_gen_scan_range.list_file_params;
    S3URI uri(params.uri);
    RETURN_IF_ERROR(uri.parse(true));
    _properties = params.resource.properties;
    _path.bucket = uri.get_bucket();
    _path.prefix = uri.get_key();
    if (!_path.prefix.empty() && !_path.prefix.ends_with('/')) {
        _path.prefix += '/';
    }
    _path.delimiter = params.recursive ? "" : "/";
    static constexpr std::array<std::string_view, 3> NAMES {"path", "size", "file"};
    _column_indices.clear();
    for (const auto* slot : _tuple_desc->slots()) {
        const auto* const field = std::ranges::find(NAMES, slot->col_name());
        DCHECK(field != NAMES.end());
        _column_indices.push_back(std::distance(NAMES.begin(), field));
    }
    return Status::OK();
}

Status VListFileTVF::_fetch_next_page(RuntimeState* state) {
    RETURN_IF_CANCELLED(state);
    if (!_filesystem) {
        io::FSPropertiesRef properties(TFileType::FILE_S3);
        properties.properties = &_properties;
        io::FileDescription description;
        description.path = "s3://" + _path.bucket;
        auto filesystem = DORIS_TRY(FileFactory::create_fs(properties, description));
        _filesystem = std::static_pointer_cast<io::S3FileSystem>(filesystem);
        RETURN_IF_CANCELLED(state);
    }

    RETURN_IF_CANCELLED(state);
    const auto client = _filesystem->client_holder()->get();
    // Release the previous page before the provider allocates its replacement.
    _objects = std::vector<io::ObjectMeta>();
    auto page = client->list_objects_page(_path, _continuation_token);
    RETURN_IF_CANCELLED(state);
    if (!page.resp.ok()) {
        return {page.resp.status.code, std::move(page.resp.status.msg)};
    }
    _objects = std::move(page.objects);
    for (auto& prefix : page.common_prefixes) {
        _objects.emplace_back(ObjectMeta {.key = std::move(prefix)});
    }
    _next_index = 0;
    _continuation_token = std::move(page.continuation_token);
    _has_more = page.has_more;
    return Status::OK();
}

Status VListFileTVF::get_next(RuntimeState* state, Block* block, bool* eos) {
    DCHECK(block->rows() == 0);
    DCHECK(!_tuple_desc->slots().empty());
    RETURN_IF_CANCELLED(state);
    *eos = false;

    if (!block->mem_reuse()) {
        for (const auto* slot : _tuple_desc->slots()) {
            block->insert({slot->get_empty_mutable_column(), slot->get_data_type_ptr(),
                           slot->col_name()});
        }
    }
    // Restore borrowed column owners on success, cancellation and remote errors.
    auto scoped_columns = block->mutate_columns_scoped();
    auto& columns = scoped_columns.mutable_columns();
    auto& output = columns[0];
    while (output->size() < static_cast<size_t>(state->batch_size())) {
        if (_next_index == _objects.size()) {
            if (!output->empty()) {
                break;
            }
            if (!_has_more) {
                *eos = true;
                break;
            }
            RETURN_IF_ERROR(_fetch_next_page(state));
            continue;
        }
        RETURN_IF_CANCELLED(state);
        const auto& object = _objects[_next_index++];
        const bool is_directory = object.key.ends_with('/');
        // The prefix marker names the listed directory itself; recursive scans omit all markers.
        if (is_directory && (_path.delimiter.empty() || object.key == _path.prefix)) {
            continue;
        }
        std::array<Field, 3> fields {
                Field::create_field<TYPE_STRING>("s3://" + _path.bucket + "/" + object.key),
                Field::create_field<TYPE_BIGINT>(is_directory ? 0 : object.size), Field()};
        if (!is_directory) {
            File value(DataTypeFile::FIELD_COUNT);
            value[0] = fields[0];
            value[2] = fields[1];
            value[3] = Field::create_field<TYPE_STRING>(
                    std::string(infer_file_content_type_from_name(object.key)));
            if (auto checksum = file_checksum_from_etag(object.etag)) {
                value[4] = Field::create_field<TYPE_STRING>(std::move(*checksum));
            }
            RETURN_IF_ERROR(validate_file(value));
            fields[2] = Field::create_field<TYPE_FILE>(std::move(value));
        }
        for (size_t i = 0; i < columns.size(); ++i) {
            columns[i]->insert(fields[_column_indices[i]]);
        }
    }
    *eos = _next_index == _objects.size() && !_has_more;
    return Status::OK();
}

} // namespace doris
