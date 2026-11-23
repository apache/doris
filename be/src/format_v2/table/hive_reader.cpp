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

#include "format_v2/table/hive_reader.h"

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <algorithm>
#include <cctype>
#include <string_view>

#include "common/consts.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "format_v2/column_mapper.h"
#include "format_v2/file_reader.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/defer_op.h"

namespace doris::format::hive {
namespace {

constexpr size_t MAX_HIVE_AVRO_SCHEMA_BYTES = 1024 * 1024;

struct SchemaSizeStream {
    using Ch = char;
    size_t size = 0;
    void Put(char) {
        if (size <= MAX_HIVE_AVRO_SCHEMA_BYTES) {
            ++size;
        }
    }
    void Flush() {}
};

const char* hive_avro_primitive(PrimitiveType type) {
    switch (type) {
    case TYPE_BOOLEAN:
        return "boolean";
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
        return "int";
    case TYPE_BIGINT:
        return "long";
    case TYPE_FLOAT:
        return "float";
    case TYPE_DOUBLE:
        return "double";
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:
        return "string";
    case TYPE_VARBINARY:
        return "bytes";
    default:
        return nullptr;
    }
}

template <typename Writer>
Status hive_avro_type(const DataTypePtr& input, Writer* output, size_t* record_id,
                      size_t depth = 0);

template <typename Writer>
Status hive_avro_record_type(const DataTypeStruct& structure, Writer* output, size_t* record_id,
                             size_t depth) {
    output->StartObject();
    output->Key("type");
    output->String("record");
    output->Key("name");
    output->String(("hive_record_" + std::to_string((*record_id)++)).c_str());
    output->Key("fields");
    output->StartArray();
    for (size_t i = 0; i < structure.get_elements().size(); ++i) {
        output->StartObject();
        output->Key("name");
        output->String(structure.get_element_names()[i].c_str());
        output->Key("type");
        RETURN_IF_ERROR(hive_avro_type(structure.get_elements()[i], output, record_id, depth + 1));
        output->Key("default");
        output->Null();
        output->EndObject();
    }
    output->EndArray();
    output->EndObject();
    return Status::OK();
}

template <typename Writer>
Status hive_avro_type(const DataTypePtr& input, Writer* output, size_t* record_id, size_t depth) {
    if (depth >= 64) {
        return Status::NotSupported("Hive Avro table type nesting is unsupported");
    }
    const auto type = remove_nullable(input);
    output->StartArray();
    output->String("null");
    const auto primitive = type->get_primitive_type();
    auto logical = [&](const char* physical, const char* name) {
        output->StartObject();
        output->Key("type");
        output->String(physical);
        output->Key("logicalType");
        output->String(name);
    };
    if (const auto* name = hive_avro_primitive(primitive)) {
        output->String(name);
        output->EndArray();
        return Status::OK();
    }
    switch (primitive) {
    case TYPE_DATE:
    case TYPE_DATEV2:
        logical("int", "date");
        output->EndObject();
        break;
    case TYPE_DATETIME:
    case TYPE_DATETIMEV2:
        // Hive's implicit Avro schema and deserializer interpret timestamp longs as millis.
        logical("long", "timestamp-millis");
        output->EndObject();
        break;
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
    case TYPE_DECIMAL256:
        logical("bytes", "decimal");
        output->Key("precision");
        output->Uint(type->get_precision());
        output->Key("scale");
        output->Uint(type->get_scale());
        output->EndObject();
        break;
    case TYPE_ARRAY:
        output->StartObject();
        output->Key("type");
        output->String("array");
        output->Key("items");
        RETURN_IF_ERROR(hive_avro_type(assert_cast<const DataTypeArray&>(*type).get_nested_type(),
                                       output, record_id, depth + 1));
        output->EndObject();
        break;
    case TYPE_MAP: {
        const auto& map = assert_cast<const DataTypeMap&>(*type);
        const auto key = remove_nullable(map.get_key_type())->get_primitive_type();
        if (key != TYPE_STRING && key != TYPE_VARCHAR && key != TYPE_CHAR) {
            return Status::NotSupported("Hive Avro maps require string keys");
        }
        output->StartObject();
        output->Key("type");
        output->String("map");
        output->Key("values");
        RETURN_IF_ERROR(hive_avro_type(map.get_value_type(), output, record_id, depth + 1));
        output->EndObject();
        break;
    }
    case TYPE_STRUCT:
        RETURN_IF_ERROR(hive_avro_record_type(assert_cast<const DataTypeStruct&>(*type), output,
                                              record_id, depth));
        break;
    default:
        return Status::NotSupported("Unsupported Hive Avro table type {}", type->get_name());
    }
    output->EndArray();
    return Status::OK();
}

TFileFormatType::type format_type_from_context(const format::ProjectedColumnBuildContext& context) {
    DORIS_CHECK(context.scan_params != nullptr);
    if (context.range != nullptr && context.range->__isset.format_type) {
        return context.range->format_type;
    }
    return context.scan_params->format_type;
}

bool use_column_position_mapping(const format::ProjectedColumnBuildContext& context) {
    if (context.runtime_state == nullptr || context.scan_params == nullptr) {
        return false;
    }
    switch (format_type_from_context(context)) {
    case TFileFormatType::FORMAT_PARQUET:
        return !context.runtime_state->query_options().hive_parquet_use_column_names;
    case TFileFormatType::FORMAT_ORC:
        return !context.runtime_state->query_options().hive_orc_use_column_names;
    default:
        return false;
    }
}

bool is_file_column_position_slot(const TFileScanSlotInfo& slot_info,
                                  const std::string& column_name) {
    if (column_name.starts_with(BeConsts::GLOBAL_ROWID_COL) ||
        column_name == BeConsts::ICEBERG_ROWID_COL) {
        return false;
    }
    if (slot_info.__isset.is_file_slot) {
        return slot_info.is_file_slot;
    }
    return !slot_info.__isset.category || slot_info.category != TColumnCategory::PARTITION_KEY;
}

bool is_hive1_orc_column_name(std::string_view name) {
    if (name.size() <= 4 || name.substr(0, 4) != "_col") {
        return false;
    }
    return std::ranges::all_of(name.substr(4), [](unsigned char c) { return std::isdigit(c); });
}

bool is_hive1_orc_file_schema(const std::vector<format::ColumnDefinition>& file_schema) {
    bool has_data_column = false;
    for (const auto& field : file_schema) {
        if (field.column_type != format::ColumnType::DATA_COLUMN) {
            continue;
        }
        has_data_column = true;
        if (!is_hive1_orc_column_name(field.name)) {
            return false;
        }
    }
    return has_data_column;
}

bool is_hive_data_column(const format::ColumnDefinition& column) {
    if (column.column_type != format::ColumnType::DATA_COLUMN || column.is_partition_key) {
        return false;
    }
    return !column.name.starts_with(BeConsts::GLOBAL_ROWID_COL) &&
           column.name != BeConsts::ICEBERG_ROWID_COL;
}

template <typename Stream>
Status write_hive_avro_schema(const std::vector<ColumnDefinition>& columns, Stream* stream) {
    rapidjson::Writer<Stream> output(*stream);
    output.StartObject();
    output.Key("type");
    output.String("record");
    output.Key("name");
    output.String("hive_table");
    output.Key("fields");
    output.StartArray();
    size_t record_id = 0;
    for (const auto& column : columns) {
        if (!is_hive_data_column(column)) {
            continue;
        }
        output.StartObject();
        output.Key("name");
        output.String(column.name.c_str());
        output.Key("type");
        RETURN_IF_ERROR(hive_avro_type(column.type, &output, &record_id));
        output.Key("default");
        output.Null();
        output.EndObject();
    }
    output.EndArray();
    output.EndObject();
    return Status::OK();
}

Status reserve_hive_schema_memory(size_t bytes, ReservedMemoryToken* token) {
    auto* manager = thread_context()->thread_mem_tracker_mgr.get();
    auto previous = manager->take_reserved_memory();
    Defer restore([&] { manager->adopt_reserved_memory(std::move(previous)); });
    RETURN_IF_ERROR(manager->try_reserve(static_cast<int64_t>(bytes)));
    *token = manager->take_reserved_memory();
    return Status::OK();
}

void add_name_mapping(std::vector<std::string>* name_mapping, const std::string& alias) {
    DORIS_CHECK(name_mapping != nullptr);
    if (std::ranges::find(*name_mapping, alias) == name_mapping->end()) {
        name_mapping->push_back(alias);
    }
}

} // namespace

Status HiveReader::prepare_split(const format::SplitReadOptions& options) {
    {
        // Keep derived validation visible without overlapping the base scopes on the same timers.
        SCOPED_TIMER(_profile.total_timer);
        SCOPED_TIMER(_profile.prepare_split_timer);
        if (options.current_split_format != _format) {
            return Status::InternalError(
                    "Hive scan expects all splits to use the same file format, "
                    "initialized_format={}, current_split_format={}",
                    static_cast<int>(_format), static_cast<int>(options.current_split_format));
        }
    }
    return format::TableReader::prepare_split(options);
}

format::TableColumnMappingMode HiveReader::mapping_mode() const {
    // Hive-specific behavior: choose the column matching mode based on file format and the
    // matching session variable.
    //   - hive_orc_use_column_names / hive_parquet_use_column_names == true
    //     => BY_NAME (modern Hive default, match by column name)
    //   - those options == false
    //     => BY_INDEX (mainly for Hive1 ORC `_col0` / `_col1`, match by top-level position;
    //                  Parquet exposes the same switch for consistency)
    // TableReader updates `_format` in prepare_split(), so this is evaluated per split.
    DORIS_CHECK(_runtime_state != nullptr);
    const auto& query_options = _runtime_state->query_options();
    bool use_column_names = true;
    if (_format == format::FileFormat::ORC) {
        use_column_names = query_options.hive_orc_use_column_names;
    } else if (_format == format::FileFormat::PARQUET) {
        use_column_names = query_options.hive_parquet_use_column_names;
    } else if (_format == format::FileFormat::AVRO) {
        // Avro carries field names even when Hive's ORC/Parquet positional switches are disabled.
        use_column_names = true;
    } else if (_format == format::FileFormat::CSV || _format == format::FileFormat::TEXT ||
               _format == format::FileFormat::JSON) {
        // Hive CSV/TEXT/JSON readers synthesize a file-local schema from FE-provided file slots
        // because these formats do not carry embedded column names or field ids. The scan params'
        // format-specific attributes still tell the physical reader how to read values, while the
        // table-level mapper can safely match the synthesized file schema by table column name.
        use_column_names = true;
    } else {
        DORIS_CHECK(false) << "HiveReader does not support this file reader format";
    }

    return use_column_names ? format::TableColumnMappingMode::BY_NAME
                            : format::TableColumnMappingMode::BY_INDEX;
}

Status HiveReader::annotate_projected_column(const TFileScanSlotInfo& slot_info,
                                             format::ProjectedColumnBuildContext* context,
                                             format::ColumnDefinition* column) const {
    RETURN_IF_ERROR(format::TableReader::annotate_projected_column(slot_info, context, column));
    DORIS_CHECK(context != nullptr);
    DORIS_CHECK(column != nullptr);
    if (!use_column_position_mapping(*context) ||
        !is_file_column_position_slot(slot_info, column->name)) {
        return Status::OK();
    }
    const auto* scan_params = context->scan_params;
    DORIS_CHECK(scan_params != nullptr);
    if (!scan_params->__isset.column_idxs ||
        context->next_file_column_idx >= scan_params->column_idxs.size()) {
        return Status::InvalidArgument(
                "Hive positional column mapping is missing file index for column '{}', "
                "required file slot ordinal={}, column_idxs_size={}",
                column->name, context->next_file_column_idx,
                scan_params->__isset.column_idxs ? scan_params->column_idxs.size() : 0);
    }
    const auto file_index = scan_params->column_idxs[context->next_file_column_idx];
    if (file_index < 0) {
        return Status::InvalidArgument(
                "Hive positional column mapping has negative file index {} for column '{}'",
                file_index, column->name);
    }
    column->identifier = Field::create_field<TYPE_INT>(file_index);
    ++context->next_file_column_idx;
    return Status::OK();
}

Status HiveReader::validate_projected_columns(
        const format::ProjectedColumnBuildContext& context) const {
    if (!use_column_position_mapping(context)) {
        return Status::OK();
    }
    DORIS_CHECK(context.scan_params != nullptr);
    if (context.scan_params->__isset.column_idxs &&
        context.next_file_column_idx != context.scan_params->column_idxs.size()) {
        return Status::InvalidArgument(
                "Hive positional column mapping has unused file indexes: consumed={}, "
                "column_idxs_size={}",
                context.next_file_column_idx, context.scan_params->column_idxs.size());
    }
    return Status::OK();
}

Status HiveReader::annotate_file_schema(std::vector<format::ColumnDefinition>* file_schema) {
    DORIS_CHECK(file_schema != nullptr);
    if (_format == format::FileFormat::AVRO) {
        // HMS defines an implicit Avro reader even without a literal. Validate before the
        // mapper localizes filters or performs SQL casts that differ from Avro resolution.
        // A missing table field can make this declaration much larger than the writer schema.
        // Count escaped JSON without allocating its buffer, then admit buffer growth and copies.
        SchemaSizeStream size;
        RETURN_IF_ERROR(write_hive_avro_schema(_projected_columns, &size));
        if (size.size > MAX_HIVE_AVRO_SCHEMA_BYTES) {
            return Status::NotSupported("Hive Avro projected schema exceeds supported size");
        }
        ReservedMemoryToken schema_memory;
        RETURN_IF_ERROR(reserve_hive_schema_memory(size.size * 4 + 64 * 1024, &schema_memory));
        rapidjson::StringBuffer buffer;
        RETURN_IF_ERROR(write_hive_avro_schema(_projected_columns, &buffer));
        const std::string implicit_schema(buffer.GetString(), buffer.GetSize());
        // HDFS transports location properties in hdfs_conf; object stores use properties.
        if (_scan_params->__isset.properties) {
            const auto literal = _scan_params->properties.find("avro.schema.literal");
            if (literal != _scan_params->properties.end()) {
                return _data_reader.reader->validate_read_schema(literal->second, true,
                                                                 implicit_schema);
            }
        }
        if (_scan_params->__isset.hdfs_params) {
            for (const auto& conf : _scan_params->hdfs_params.hdfs_conf) {
                if (conf.key == "avro.schema.literal") {
                    return _data_reader.reader->validate_read_schema(conf.value, true,
                                                                     implicit_schema);
                }
            }
        }
        return _data_reader.reader->validate_read_schema(implicit_schema, false);
    }
    if (_format != format::FileFormat::ORC || _runtime_state == nullptr ||
        !_runtime_state->query_options().hive_orc_use_column_names ||
        !is_hive1_orc_file_schema(*file_schema)) {
        return Status::OK();
    }

    DORIS_CHECK(_scan_params != nullptr);
    if (!_scan_params->__isset.column_idxs) {
        return Status::InvalidArgument(
                "Hive ORC Hive1-style name mapping is missing positional column indexes");
    }

    size_t next_file_column_idx = 0;
    for (const auto& table_column : _projected_columns) {
        if (!is_hive_data_column(table_column)) {
            continue;
        }
        if (next_file_column_idx >= _scan_params->column_idxs.size()) {
            return Status::InvalidArgument(
                    "Hive ORC Hive1-style name mapping is missing file index for column '{}', "
                    "required file slot ordinal={}, column_idxs_size={}",
                    table_column.name, next_file_column_idx, _scan_params->column_idxs.size());
        }
        const auto file_index = _scan_params->column_idxs[next_file_column_idx];
        if (file_index < 0) {
            return Status::InvalidArgument(
                    "Hive ORC Hive1-style name mapping has negative file index {} for column '{}'",
                    file_index, table_column.name);
        }
        ++next_file_column_idx;
        if (static_cast<size_t>(file_index) >= file_schema->size()) {
            continue;
        }

        auto& file_column = (*file_schema)[static_cast<size_t>(file_index)];
        add_name_mapping(&file_column.name_mapping, table_column.name);
        if (table_column.has_identifier_name()) {
            add_name_mapping(&file_column.name_mapping, table_column.get_identifier_name());
        }
        for (const auto& alias : table_column.name_mapping) {
            add_name_mapping(&file_column.name_mapping, alias);
        }
    }
    if (next_file_column_idx != _scan_params->column_idxs.size()) {
        return Status::InvalidArgument(
                "Hive ORC Hive1-style name mapping has unused file indexes: consumed={}, "
                "column_idxs_size={}",
                next_file_column_idx, _scan_params->column_idxs.size());
    }
    return Status::OK();
}

} // namespace doris::format::hive
