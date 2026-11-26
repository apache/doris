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

#include "format_v2/table/delta_sys_table_reader.h"

#include <cctz/time_zone.h>

#include <algorithm>
#include <utility>

#include "core/block/block.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/value/timestamptz_value.h"
#include "format_v2/column_mapper.h"
#include "format_v2/materialized_reader_util.h"
#include "util/string_util.h"

namespace doris::format::deltalake {
namespace {

Status timestamp_field(int64_t millis, Field* value) {
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
        return Status::DataQualityError("Delta system table timestamp {} is out of range", millis);
    }
    TimestampTzValue utc;
    utc.unchecked_set_time(cast_set<uint16_t>(civil.year()), cast_set<uint8_t>(civil.month()),
                           cast_set<uint8_t>(civil.day()), cast_set<uint8_t>(civil.hour()),
                           cast_set<uint8_t>(civil.minute()), cast_set<uint8_t>(civil.second()),
                           cast_set<uint32_t>(remainder * 1000));
    *value = Field::create_field<TYPE_TIMESTAMPTZ>(utc);
    return Status::OK();
}

Status decode_cell(const TDeltaLakeSysTableField& cell, const DataTypePtr& type, Field* value) {
    const auto& set = cell.__isset;
    const int branches = set.bool_val + set.int64_val + set.double_val + set.string_val +
                         set.timestamp_millis + set.array_val + set.map_val + set.struct_val;
    if (branches == 0 && type->is_nullable()) {
        *value = Field {};
        return Status::OK();
    }
    if (branches != 1) {
        return Status::DataQualityError("Delta system table cell has {} values for {}", branches,
                                        type->get_name());
    }
    const auto nested = remove_nullable(type);
    switch (nested->get_primitive_type()) {
    case TYPE_BOOLEAN:
        if (set.bool_val) {
            *value = Field::create_field<TYPE_BOOLEAN>(cell.bool_val);
            return Status::OK();
        }
        break;
    case TYPE_BIGINT:
        if (set.int64_val) {
            *value = Field::create_field<TYPE_BIGINT>(cell.int64_val);
            return Status::OK();
        }
        break;
    case TYPE_DOUBLE:
        if (set.double_val) {
            *value = Field::create_field<TYPE_DOUBLE>(cell.double_val);
            return Status::OK();
        }
        break;
    case TYPE_STRING:
        if (set.string_val) {
            *value = Field::create_field<TYPE_STRING>(cell.string_val);
            return Status::OK();
        }
        break;
    case TYPE_TIMESTAMPTZ:
        if (set.timestamp_millis) {
            return timestamp_field(cell.timestamp_millis, value);
        }
        break;
    case TYPE_ARRAY:
        if (set.array_val) {
            const auto& element = assert_cast<const DataTypeArray&>(*nested).get_nested_type();
            Array values(cell.array_val.size());
            for (size_t i = 0; i < values.size(); ++i) {
                RETURN_IF_ERROR(decode_cell(cell.array_val[i], element, &values[i]));
            }
            *value = Field::create_field<TYPE_ARRAY>(std::move(values));
            return Status::OK();
        }
        break;
    case TYPE_MAP:
        if (set.map_val) {
            const auto& map = assert_cast<const DataTypeMap&>(*nested);
            if (map.get_key_type()->get_primitive_type() != TYPE_STRING) {
                break;
            }
            Array keys;
            Array values(cell.map_val.size());
            keys.reserve(values.size());
            for (size_t i = 0; i < values.size(); ++i) {
                const auto& entry = cell.map_val[i];
                if (!entry.__isset.key || !entry.__isset.value) {
                    return Status::DataQualityError("Incomplete Delta system table map entry");
                }
                keys.push_back(Field::create_field<TYPE_STRING>(entry.key));
                RETURN_IF_ERROR(decode_cell(entry.value, map.get_value_type(), &values[i]));
            }
            Map fields;
            fields.push_back(Field::create_field<TYPE_ARRAY>(std::move(keys)));
            fields.push_back(Field::create_field<TYPE_ARRAY>(std::move(values)));
            *value = Field::create_field<TYPE_MAP>(std::move(fields));
            return Status::OK();
        }
        break;
    case TYPE_STRUCT:
        if (set.struct_val) {
            const auto& types = assert_cast<const DataTypeStruct&>(*nested).get_elements();
            if (types.size() != cell.struct_val.size()) {
                break;
            }
            Struct fields(types.size());
            for (size_t i = 0; i < fields.size(); ++i) {
                RETURN_IF_ERROR(decode_cell(cell.struct_val[i], types[i], &fields[i]));
            }
            *value = Field::create_field<TYPE_STRUCT>(std::move(fields));
            return Status::OK();
        }
        break;
    default:
        break;
    }
    return Status::DataQualityError("Delta system table cell does not match {}", type->get_name());
}

} // namespace

DeltaSysTableReader::DeltaSysTableReader(
        std::shared_ptr<io::FileSystemProperties>& system_properties,
        std::unique_ptr<io::FileDescription>& file_description,
        std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
        const TDeltaLakeSysTableBatch& batch,
        const std::vector<ColumnDefinition>& projected_columns)
        : FileReader(system_properties, file_description, std::move(io_ctx), profile),
          _batch(batch),
          _projected_columns(projected_columns) {}

Status DeltaSysTableReader::init(RuntimeState*) {
    if (!_batch.__isset.columns || !_batch.__isset.rows) {
        return Status::DataQualityError("Incomplete Delta system table batch");
    }
    for (const auto& row : _batch.rows) {
        if (!row.__isset.cells || row.cells.size() != _batch.columns.size()) {
            return Status::DataQualityError("Delta system table row has {} cells for {} columns",
                                            row.cells.size(), _batch.columns.size());
        }
    }
    return Status::OK();
}

Status DeltaSysTableReader::get_schema(std::vector<ColumnDefinition>* schema) const {
    schema->clear();
    for (size_t i = 0; i < _batch.columns.size(); ++i) {
        const auto& column = _batch.columns[i];
        if (!column.__isset.name || !column.__isset.schema_ordinal || column.schema_ordinal < 0) {
            return Status::DataQualityError("Incomplete Delta system table column at position {}",
                                            i);
        }
        auto target = std::ranges::find_if(_projected_columns, [&](const ColumnDefinition& def) {
            return iequal(def.name, column.name);
        });
        if (target != _projected_columns.end()) {
            schema->push_back({.identifier = Field::create_field<TYPE_INT>(column.schema_ordinal),
                               .local_id = cast_set<int32_t>(i),
                               .name = target->name,
                               .type = target->type,
                               .children = materialized_schema_children(target->type)});
        }
    }
    return Status::OK();
}

std::unique_ptr<TableColumnMapper> DeltaSysTableReader::create_column_mapper(
        TableColumnMapperOptions options) const {
    return std::make_unique<MaterializedColumnMapper>(std::move(options));
}

void DeltaSysTableReader::set_batch_size(size_t size) {
    DORIS_CHECK(size > 0);
    _batch_size = size;
}

Status DeltaSysTableReader::get_block(Block* block, size_t* rows, bool* eof) {
    DORIS_CHECK(_request != nullptr);
    *rows = std::min(_batch_size, _batch.rows.size() - _offset);
    for (const auto& [id, position] : _request->local_positions) {
        auto& slot = block->get_by_position(position.value());
        auto& data = slot.column;
        auto column = IColumn::mutate(std::move(data));
        if (_request->is_count_star_placeholder(id)) {
            column->insert_many_defaults(*rows);
        } else {
            for (size_t i = _offset; i < _offset + *rows; ++i) {
                Field value;
                RETURN_IF_ERROR(decode_cell(_batch.rows[i].cells[id.value()], slot.type, &value));
                column->insert(value);
            }
        }
        data = std::move(column);
    }
    _offset += *rows;
    *eof = _eof = _offset == _batch.rows.size();
    _record_scan_rows(cast_set<int64_t>(*rows));
    return apply_materialized_reader_filters(_request.get(), _io_ctx.get(), block, rows);
}

} // namespace doris::format::deltalake
