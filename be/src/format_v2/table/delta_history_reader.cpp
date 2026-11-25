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

#include "format_v2/table/delta_history_reader.h"

#include <cctz/time_zone.h>

#include <algorithm>
#include <string>
#include <utility>

#include "core/block/block.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/value/timestamptz_value.h"
#include "format_v2/column_mapper.h"
#include "format_v2/materialized_reader_util.h"

namespace doris::format::deltalake {
namespace {

enum HistoryColumn {
    VERSION,
    TIMESTAMP,
    USER_ID,
    USER_NAME,
    OPERATION,
    OPERATION_PARAMETERS,
    JOB,
    NOTEBOOK,
    CLUSTER_ID,
    READ_VERSION,
    ISOLATION_LEVEL,
    IS_BLIND_APPEND,
    OPERATION_METRICS,
    USER_METADATA,
    ENGINE_INFO,
};

ColumnDefinition history_column(std::string name, DataTypePtr type, int32_t id,
                                std::vector<ColumnDefinition> children = {}) {
    return {.identifier = Field::create_field<TYPE_INT>(id),
            .local_id = id,
            .name = std::move(name),
            .type = make_nullable(std::move(type)),
            .children = std::move(children)};
}

ColumnDefinition history_map(std::string name, const DataTypePtr& string, int32_t id) {
    return history_column(std::move(name), std::make_shared<DataTypeMap>(string, string), id,
                          {history_column("key", string, 0), history_column("value", string, 1)});
}

ColumnDefinition history_struct(std::string name, const DataTypePtr& string, int32_t id,
                                const Strings& names) {
    std::vector<ColumnDefinition> children;
    for (const auto& child : names) {
        children.push_back(history_column(child, string, cast_set<int32_t>(children.size())));
    }
    return history_column(std::move(name),
                          std::make_shared<DataTypeStruct>(DataTypes(names.size(), string), names),
                          id, std::move(children));
}

Field history_string(bool present, const std::string& text) {
    return present ? Field::create_field<TYPE_STRING>(text) : Field {};
}

Field history_map_value(const std::vector<TDeltaLakeHistoryMapEntry>& entries) {
    Array keys;
    Array values;
    keys.reserve(entries.size());
    values.reserve(entries.size());
    for (const auto& entry : entries) {
        DORIS_CHECK(entry.__isset.key);
        keys.push_back(history_string(true, entry.key));
        values.push_back(history_string(entry.__isset.value, entry.value));
    }
    Map map;
    map.push_back(Field::create_field<TYPE_ARRAY>(std::move(keys)));
    map.push_back(Field::create_field<TYPE_ARRAY>(std::move(values)));
    return Field::create_field<TYPE_MAP>(std::move(map));
}

Field history_job_value(const TDeltaLakeHistoryJob& job) {
    Struct fields;
    fields.push_back(history_string(job.__isset.job_id, job.job_id));
    fields.push_back(history_string(job.__isset.job_name, job.job_name));
    fields.push_back(history_string(job.__isset.job_run_id, job.job_run_id));
    fields.push_back(history_string(job.__isset.run_id, job.run_id));
    fields.push_back(history_string(job.__isset.job_owner_id, job.job_owner_id));
    fields.push_back(history_string(job.__isset.trigger_type, job.trigger_type));
    return Field::create_field<TYPE_STRUCT>(std::move(fields));
}

Field history_notebook_value(const TDeltaLakeHistoryNotebook& notebook) {
    Struct fields;
    fields.push_back(history_string(notebook.__isset.notebook_id, notebook.notebook_id));
    return Field::create_field<TYPE_STRUCT>(std::move(fields));
}

Status history_timestamp(int64_t millis, Field* value) {
    // C++ rounds integer division towards zero. Normalize the remainder so -1 ms is
    // 1969-12-31 23:59:59.999000 UTC rather than a negative fractional second.
    int64_t seconds = millis / 1000;
    int64_t remainder = millis % 1000;
    if (remainder < 0) {
        --seconds;
        remainder += 1000;
    }
    const cctz::time_point<cctz::seconds> instant {cctz::seconds(seconds)};
    const auto civil = cctz::convert(instant, cctz::utc_time_zone());
    if (civil.year() < 0 || civil.year() > 9999) {
        return Status::DataQualityError("Delta history timestamp {} is out of range", millis);
    }
    TimestampTzValue utc;
    utc.unchecked_set_time(cast_set<uint16_t>(civil.year()), cast_set<uint8_t>(civil.month()),
                           cast_set<uint8_t>(civil.day()), cast_set<uint8_t>(civil.hour()),
                           cast_set<uint8_t>(civil.minute()), cast_set<uint8_t>(civil.second()),
                           cast_set<uint32_t>(remainder * 1000));
    *value = Field::create_field<TYPE_TIMESTAMPTZ>(utc);
    return Status::OK();
}

Status history_value(const TDeltaLakeHistoryRow& row, int32_t id, Field* value) {
    switch (id) {
    case VERSION:
        DORIS_CHECK(row.__isset.version);
        *value = Field::create_field<TYPE_BIGINT>(row.version);
        break;
    case TIMESTAMP:
        DORIS_CHECK(row.__isset.timestamp_millis);
        return history_timestamp(row.timestamp_millis, value);
    case USER_ID:
        *value = history_string(row.__isset.user_id, row.user_id);
        break;
    case USER_NAME:
        *value = history_string(row.__isset.user_name, row.user_name);
        break;
    case OPERATION:
        *value = history_string(row.__isset.operation, row.operation);
        break;
    case OPERATION_PARAMETERS:
        if (row.__isset.operation_parameters) {
            *value = history_map_value(row.operation_parameters);
        }
        break;
    case JOB:
        if (row.__isset.job) {
            *value = history_job_value(row.job);
        }
        break;
    case NOTEBOOK:
        if (row.__isset.notebook) {
            *value = history_notebook_value(row.notebook);
        }
        break;
    case CLUSTER_ID:
        *value = history_string(row.__isset.cluster_id, row.cluster_id);
        break;
    case READ_VERSION:
        if (row.__isset.read_version) {
            *value = Field::create_field<TYPE_BIGINT>(row.read_version);
        }
        break;
    case ISOLATION_LEVEL:
        *value = history_string(row.__isset.isolation_level, row.isolation_level);
        break;
    case IS_BLIND_APPEND:
        if (row.__isset.is_blind_append) {
            *value = Field::create_field<TYPE_BOOLEAN>(row.is_blind_append);
        }
        break;
    case OPERATION_METRICS:
        if (row.__isset.operation_metrics) {
            *value = history_map_value(row.operation_metrics);
        }
        break;
    case USER_METADATA:
        *value = history_string(row.__isset.user_metadata, row.user_metadata);
        break;
    case ENGINE_INFO:
        *value = history_string(row.__isset.engine_info, row.engine_info);
        break;
    default:
        return Status::InternalError("Unknown Delta history column ID {}", id);
    }
    return Status::OK();
}

} // namespace

DeltaHistoryFileReader::DeltaHistoryFileReader(
        std::shared_ptr<io::FileSystemProperties>& system_properties,
        std::unique_ptr<io::FileDescription>& file_description,
        std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
        std::span<const TDeltaLakeHistoryRow> rows)
        : FileReader(system_properties, file_description, std::move(io_ctx), profile),
          _rows(rows) {}

Status DeltaHistoryFileReader::init(RuntimeState*) {
    // FileReader::init opens a filesystem reader. This split already contains its input.
    return Status::OK();
}

Status DeltaHistoryFileReader::get_schema(std::vector<ColumnDefinition>* schema) const {
    const auto string = make_nullable(std::make_shared<DataTypeString>());
    const auto bigint = std::make_shared<DataTypeInt64>();
    *schema = {
            history_column("version", bigint, VERSION),
            history_column("timestamp", std::make_shared<DataTypeTimeStampTz>(6), TIMESTAMP),
            history_column("userid", string, USER_ID),
            history_column("username", string, USER_NAME),
            history_column("operation", string, OPERATION),
            history_map("operationparameters", string, OPERATION_PARAMETERS),
            history_struct("job", string, JOB,
                           {"jobid", "jobname", "jobrunid", "runid", "jobownerid", "triggertype"}),
            history_struct("notebook", string, NOTEBOOK, {"notebookid"}),
            history_column("clusterid", string, CLUSTER_ID),
            history_column("readversion", bigint, READ_VERSION),
            history_column("isolationlevel", string, ISOLATION_LEVEL),
            history_column("isblindappend", std::make_shared<DataTypeUInt8>(), IS_BLIND_APPEND),
            history_map("operationmetrics", string, OPERATION_METRICS),
            history_column("usermetadata", string, USER_METADATA),
            history_column("engineinfo", string, ENGINE_INFO),
    };
    return Status::OK();
}

std::unique_ptr<TableColumnMapper> DeltaHistoryFileReader::create_column_mapper(
        TableColumnMapperOptions options) const {
    return std::make_unique<MaterializedColumnMapper>(std::move(options));
}

void DeltaHistoryFileReader::set_batch_size(size_t size) {
    DORIS_CHECK(size > 0);
    _batch_size = size;
}

Status DeltaHistoryFileReader::get_block(Block* block, size_t* rows, bool* eof) {
    DORIS_CHECK(_request != nullptr);
    *rows = std::min(_batch_size, _rows.size() - _offset);
    for (const auto& [id, position] : _request->local_positions) {
        auto& data = block->get_by_position(position.value()).column;
        auto column = IColumn::mutate(std::move(data));
        if (_request->is_count_star_placeholder(id)) {
            column->insert_many_defaults(*rows);
        } else {
            for (const auto& row : _rows.subspan(_offset, *rows)) {
                Field value;
                RETURN_IF_ERROR(history_value(row, id.value(), &value));
                column->insert(value);
            }
        }
        data = std::move(column);
    }
    _offset += *rows;
    *eof = _eof = _offset == _rows.size();
    _record_scan_rows(cast_set<int64_t>(*rows));
    return apply_materialized_reader_filters(_request.get(), _io_ctx.get(), block, rows);
}

} // namespace doris::format::deltalake
