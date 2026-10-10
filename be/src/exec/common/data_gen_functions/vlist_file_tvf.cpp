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
#include <chrono>

#include "core/data_type/data_type_file.h"
#include "core/field.h"
#include "core/value/file_value.h"
#include "core/value/vdatetime_value.h"
#include "io/file_factory.h"
#include "io/fs/s3_file_system.h"
#include "runtime/runtime_state.h"
#include "util/url_coding.h"

namespace doris {
namespace {
std::string list_file_uri_key(std::string_view key) {
    static constexpr char HEX[] = "0123456789ABCDEF";
    std::string encoded;
    encoded.reserve(key.size());
    for (const unsigned char ch : key) {
        if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') ||
            ch == '-' || ch == '.' || ch == '_' || ch == '~' || ch == '/') {
            encoded += ch;
        } else {
            encoded += '%';
            encoded += HEX[ch >> 4];
            encoded += HEX[ch & 0xf];
        }
    }
    return encoded;
}
} // namespace

VListFileTVF::VListFileTVF(TupleId tuple_id, const TupleDescriptor* tuple_desc)
        : VDataGenFunctionInf(tuple_id, tuple_desc) {}

VListFileTVF::~VListFileTVF() = default;

Status VListFileTVF::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    DCHECK(scan_ranges.size() == 1);
    const auto& params = scan_ranges[0].scan_range.data_gen_scan_range.list_file_params;
    if (params.resource.file_type != TFileType::FILE_S3) {
        return Status::InvalidArgument("list_file requires an S3 resource");
    }
    auto bucket = params.resource.properties.find("AWS_BUCKET");
    if (bucket == params.resource.properties.end()) {
        bucket = params.resource.properties.find("s3.bucket");
    }
    if (bucket == params.resource.properties.end() || bucket->second.empty()) {
        return Status::InvalidArgument(
                "list_file S3 resource snapshot requires AWS_BUCKET or s3.bucket");
    }
    if (!params.uri.starts_with("s3://")) {
        return Status::InvalidArgument("list_file requires a full s3:// URI");
    }
    const std::string_view location(params.uri);
    const auto key_start = location.find('/', 5);
    const auto uri_bucket = location.substr(
            5, key_start == std::string_view::npos ? std::string_view::npos : key_start - 5);
    if (uri_bucket != bucket->second) {
        return Status::InvalidArgument("list_file URI bucket does not match the resource bucket");
    }
    _properties = params.resource.properties;
    _path.bucket = uri_bucket;
    const auto uri_key = key_start == std::string_view::npos ? std::string_view()
                                                             : location.substr(key_start + 1);
    std::string encoded_key;
    // url_decode follows HTML form rules for '+'. An S3 URI path keeps a literal '+'.
    for (const auto ch : uri_key) {
        if (ch == '+') {
            encoded_key += "%2B";
        } else {
            encoded_key += ch;
        }
    }
    if (!url_decode(encoded_key, &_path.prefix)) {
        return Status::InvalidArgument("list_file directory URI has invalid percent encoding");
    }
    if (!_path.prefix.empty() && !_path.prefix.ends_with('/')) {
        _path.prefix += '/';
    }
    _path.delimiter = params.recursive ? "" : "/";
    static constexpr std::array<std::string_view, 4> NAMES {"path", "size", "modification_time",
                                                            "file"};
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
    _objects = std::vector<ObjectMeta>();
    auto page = client->list_objects_page(_path, _continuation_token);
    RETURN_IF_CANCELLED(state);
    if (!page.resp.ok()) {
        return {page.resp.status.code, std::move(page.resp.status.msg)};
    }
    _objects = std::move(page.objects);
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

    // Retain the mutable owner locally. Only publish after remote I/O and FILE validation succeed,
    // so an error cannot leave a reused block with a moved-from column.
    MutableColumns columns;
    for (const auto* slot : _tuple_desc->slots()) {
        columns.push_back(slot->get_empty_mutable_column());
    }
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
        if (object.key.ends_with('/')) {
            continue;
        }
        File value(DataTypeFile::FIELD_COUNT);
        value[0] = Field::create_field<TYPE_STRING>("s3://" + _path.bucket + "/" +
                                                    list_file_uri_key(object.key));
        value[2] = Field::create_field<TYPE_BIGINT>(object.size);
        value[3] = Field::create_field<TYPE_STRING>(
                std::string(infer_file_content_type_from_name(object.key)));
        RETURN_IF_ERROR(validate_file(value));
        Field modification_time;
        if (object.modification_time_ms) {
            const auto milliseconds = *object.modification_time_ms;
            const auto seconds = std::chrono::floor<std::chrono::seconds>(
                                         std::chrono::milliseconds(milliseconds))
                                         .count();
            DateV2Value<DateTimeV2ValueType> datetime;
            datetime.from_unixtime(
                    std::pair<int64_t, int64_t> {seconds, (milliseconds - seconds * 1000) * 1000},
                    state->timezone_obj());
            modification_time = Field::create_field<TYPE_DATETIMEV2>(datetime.to_date_int_val());
        }
        std::array<Field, 4> fields {value[0], value[2], std::move(modification_time),
                                     Field::create_field<TYPE_FILE>(std::move(value))};
        for (size_t i = 0; i < columns.size(); ++i) {
            columns[i]->insert(fields[_column_indices[i]]);
        }
    }
    *eos = _next_index == _objects.size() && !_has_more;
    if (block->mem_reuse()) {
        block->set_columns(std::move(columns));
    } else {
        size_t index = 0;
        for (const auto* slot : _tuple_desc->slots()) {
            block->insert(
                    {std::move(columns[index++]), slot->get_data_type_ptr(), slot->col_name()});
        }
    }
    return Status::OK();
}

} // namespace doris
