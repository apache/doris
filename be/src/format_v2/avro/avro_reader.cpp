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

#include "format_v2/avro/avro_reader.h"

#include <algorithm>
#include <array>
#include <avro/Generic.hh>
#include <avro/NodeImpl.hh>
#include <charconv>
#include <limits>
#include <mutex>
#include <string_view>
#include <type_traits>
#include <unordered_set>
#include <utility>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_time.h"
#include "core/data_type/data_type_timestamp_ns.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type/data_type_uuid.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/value/timestamp_ns_value.h"
#include "core/value/timestamptz_value.h"
#include "format_v2/avro/avro_container_reader.h"
#include "format_v2/column_mapper.h"
#include "format_v2/materialized_reader_util.h"
#include "io/fs/file_reader.h"
#include "io/io_common.h"
#include "runtime/file_scan_profile.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/memory/thread_mem_tracker_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "storage/utils.h"
#include "util/timezone_utils.h"

namespace doris::format::avro {
namespace {

::avro::NodePtr resolved_node(const ::avro::NodePtr& node) {
    return node->type() == ::avro::AVRO_SYMBOLIC ? ::avro::resolveSymbol(node) : node;
}

struct AvroTemporalPolicy {
    cctz::time_zone timezone = cctz::utc_time_zone();
    bool legacy_calendar = false;
    bool legacy_timezone = false;
};

bool parse_writer_timezone(const std::string& name, cctz::time_zone* zone) {
    std::string_view offset(name);
    bool prefixed = false;
    if (offset.starts_with("GMT") || offset.starts_with("UTC")) {
        offset.remove_prefix(3);
        prefixed = true;
    } else if (offset.starts_with("UT")) {
        offset.remove_prefix(2);
        prefixed = true;
    }
    if ((prefixed && offset.empty()) || name == "Z") {
        *zone = cctz::utc_time_zone();
        return true;
    }
    if (offset.empty()) {
        return false;
    }
    if (offset.front() != '+' && offset.front() != '-') {
        // Hive emits normalized Java ZoneIds. Use named TZDB rules without SQL-only aliases.
        return cctz::load_time_zone(name, zone);
    }
    const bool negative = offset.front() == '-';
    offset.remove_prefix(1);
    auto parse = [](std::string_view part, int* value) {
        if (part.empty() || part.size() > 2 ||
            !std::ranges::all_of(part, [](char c) { return c >= '0' && c <= '9'; })) {
            return false;
        }
        const auto result = std::from_chars(part.data(), part.data() + part.size(), *value);
        return result.ec == std::errc() && result.ptr == part.data() + part.size();
    };
    int hours = 0, minutes = 0, seconds = 0;
    const auto colon = offset.find(':');
    if (!parse(offset.substr(0, colon), &hours)) {
        return false;
    }
    if (colon != std::string_view::npos) {
        offset.remove_prefix(colon + 1);
        const auto second_colon = offset.find(':');
        const auto minute = offset.substr(0, second_colon);
        if (minute.size() != 2 || !parse(minute, &minutes)) {
            return false;
        }
        if (second_colon != std::string_view::npos) {
            const auto second = offset.substr(second_colon + 1);
            if (second.size() != 2 || !parse(second, &seconds)) {
                return false;
            }
        }
    }
    // Java ZoneOffset permits +/-18h, unlike the SQL session's -12h/+14h setting.
    if (hours > 18 || minutes > 59 || seconds > 59 ||
        (hours == 18 && (minutes != 0 || seconds != 0))) {
        return false;
    }
    const int total = (negative ? -1 : 1) * (hours * 3600 + minutes * 60 + seconds);
    *zone = TimezoneUtils::create_fixed_offset_time_zone(total);
    return true;
}

size_t expanded_schema_bytes(const ::avro::NodePtr& input,
                             std::unordered_map<const ::avro::Node*, size_t>* sizes,
                             std::unordered_set<const ::avro::Node*>* ancestors) {
    const auto node = resolved_node(input);
    if (ancestors->size() >= 64 || !ancestors->insert(node.get()).second) {
        throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                        "Recursive or excessively nested Avro schemas are unsupported");
    }
    Defer pop([&] { ancestors->erase(node.get()); });
    if (const auto found = sizes->find(node.get()); found != sizes->end()) {
        return found->second;
    }
    // Named schemas share Avro nodes, but Doris expands each use into a distinct column tree.
    // Memoize the expanded cost: N_i={a:N_(i-1),b:N_(i-1)} doubles without a deep schema.
    constexpr size_t max_schema_bytes = 64 * 1024 * 1024;
    size_t bytes = 1024;
    for (size_t i = 0; i < node->leaves(); ++i) {
        const auto child = expanded_schema_bytes(node->leafAt(i), sizes, ancestors);
        if (child > max_schema_bytes || bytes > max_schema_bytes - child) {
            throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                            "Expanded Avro schema size is unsupported");
        }
        bytes += child;
    }
    sizes->emplace(node.get(), bytes);
    return bytes;
}

std::string lowercase(std::string name) {
    for (auto& c : name) {
        if (c >= 'A' && c <= 'Z') {
            c += 'a' - 'A';
        }
    }
    return name;
}

Status set_identifier(const ::avro::NodePtr& node, size_t attributes_index, const char* attribute,
                      ColumnDefinition* column) {
    if (attributes_index >= node->customAttributes()) {
        return Status::OK();
    }
    const auto value = node->customAttributesAt(attributes_index).getAttribute(attribute);
    if (!value.has_value()) {
        return Status::OK();
    }
    int32_t id = 0;
    const auto parsed = std::from_chars(value->data(), value->data() + value->size(), id);
    if (parsed.ec != std::errc() || parsed.ptr != value->data() + value->size() || id <= 0) {
        return Status::InvalidArgument("Invalid Avro field identifier");
    }
    column->identifier = Field::create_field<TYPE_INT>(id);
    return Status::OK();
}

bool is_logical_map(const ::avro::NodePtr& node) {
    const auto logical = node->logicalType();
    return node->type() == ::avro::AVRO_ARRAY && logical.type() == ::avro::LogicalType::CUSTOM &&
           logical.customLogicalType()->name() == "map";
}

// Keep logical and physical alternatives in one visitor so nullable handling is shared.
// NOLINTNEXTLINE(readability-function-size,readability-function-cognitive-complexity)
Status build_schema(const ::avro::NodePtr& input, const std::string& name, int32_t ordinal,
                    bool timestamp_tz, bool varbinary,
                    std::unordered_set<const ::avro::Node*>* ancestors, ColumnDefinition* column) {
    const auto node = resolved_node(input);
    // A recursive Avro record cannot be represented by a finite Doris STRUCT type.
    if (ancestors->size() >= 64 || !ancestors->insert(node.get()).second) {
        return Status::NotSupported("Recursive or excessively nested Avro schemas are unsupported");
    }
    struct Pop {
        std::unordered_set<const ::avro::Node*>* nodes;
        const ::avro::Node* node;
        ~Pop() { nodes->erase(node); }
    } pop {.nodes = ancestors, .node = node.get()};
    if (node->leaves() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        return Status::NotSupported("Avro schema has too many fields");
    }
    column->name = lowercase(name);
    column->local_id = ordinal;
    column->identifier = Field::create_field<TYPE_STRING>(column->name);
    if (node->type() == ::avro::AVRO_UNION) {
        ::avro::NodePtr value;
        for (size_t i = 0; i < node->leaves(); ++i) {
            if (node->leafAt(i)->type() != ::avro::AVRO_NULL) {
                if (value != nullptr) {
                    return Status::NotSupported(
                            "Avro unions with multiple non-null types are unsupported");
                }
                value = node->leafAt(i);
            }
        }
        if (value != nullptr) {
            return build_schema(value, name, ordinal, timestamp_tz, varbinary, ancestors, column);
        }
        column->type = make_nullable(std::make_shared<DataTypeString>());
        return Status::OK();
    }

    DataTypePtr type;
    const auto logical = node->logicalType();
    using Logical = ::avro::LogicalType;
    switch (logical.type()) {
    case Logical::DECIMAL:
        if (logical.precision() > 76) {
            return Status::NotSupported("Avro decimal precision exceeds 76");
        }
        type = create_decimal(logical.precision(), logical.scale(), false);
        break;
    case Logical::UUID:
        type = std::make_shared<DataTypeUUID>();
        break;
    case Logical::DATE:
        type = std::make_shared<DataTypeDateV2>();
        break;
    case Logical::TIME_MILLIS:
    case Logical::TIME_MICROS:
        type = std::make_shared<DataTypeTimeV2>(logical.type() == Logical::TIME_MILLIS ? 3 : 6);
        break;
    case Logical::TIMESTAMP_NANOS:
        // TIMESTAMP_NS is a civil clock and TIMESTAMPTZ retains only microseconds. Neither can
        // represent a nanosecond UTC instant without losing timezone semantics or precision.
        return Status::NotSupported("Avro timestamp-nanos is unsupported");
    case Logical::LOCAL_TIMESTAMP_NANOS:
        type = std::make_shared<DataTypeTimeStampNs>();
        column->timestamp_is_adjusted_to_utc = logical.type() == Logical::TIMESTAMP_NANOS;
        break;
    case Logical::TIMESTAMP_MILLIS:
    case Logical::TIMESTAMP_MICROS:
    case Logical::LOCAL_TIMESTAMP_MILLIS:
    case Logical::LOCAL_TIMESTAMP_MICROS: {
        const bool adjusted = logical.type() == Logical::TIMESTAMP_MILLIS ||
                              logical.type() == Logical::TIMESTAMP_MICROS;
        const int scale = logical.type() == Logical::TIMESTAMP_MILLIS ||
                                          logical.type() == Logical::LOCAL_TIMESTAMP_MILLIS
                                  ? 3
                                  : 6;
        type = timestamp_tz && adjusted ? DataTypePtr(std::make_shared<DataTypeTimeStampTz>(scale))
                                        : DataTypePtr(std::make_shared<DataTypeDateTimeV2>(scale));
        column->timestamp_is_adjusted_to_utc = adjusted;
        break;
    }
    default:
        break;
    }
    if (type == nullptr) {
        switch (node->type()) {
        case ::avro::AVRO_NULL:
            type = std::make_shared<DataTypeString>();
            break;
        case ::avro::AVRO_BOOL:
            type = std::make_shared<DataTypeUInt8>();
            break;
        case ::avro::AVRO_INT:
            type = std::make_shared<DataTypeInt32>();
            break;
        case ::avro::AVRO_LONG:
            type = std::make_shared<DataTypeInt64>();
            break;
        case ::avro::AVRO_FLOAT:
            type = std::make_shared<DataTypeFloat32>();
            break;
        case ::avro::AVRO_DOUBLE:
            type = std::make_shared<DataTypeFloat64>();
            break;
        case ::avro::AVRO_BYTES:
        case ::avro::AVRO_FIXED:
            type = varbinary ? DataTypePtr(std::make_shared<DataTypeVarbinary>())
                             : DataTypePtr(std::make_shared<DataTypeString>());
            break;
        case ::avro::AVRO_STRING:
        case ::avro::AVRO_ENUM:
            type = std::make_shared<DataTypeString>();
            break;
        case ::avro::AVRO_RECORD: {
            DataTypes types;
            Strings names;
            for (size_t i = 0; i < node->leaves(); ++i) {
                ColumnDefinition child;
                RETURN_IF_ERROR(build_schema(node->leafAt(i), node->nameAt(i),
                                             static_cast<int32_t>(i), timestamp_tz, varbinary,
                                             ancestors, &child));
                RETURN_IF_ERROR(set_identifier(node, i, "field-id", &child));
                types.push_back(child.type);
                names.push_back(child.name);
                column->children.push_back(std::move(child));
            }
            if (types.empty()) {
                return Status::NotSupported("Empty Avro records are unsupported");
            }
            type = std::make_shared<DataTypeStruct>(types, names);
            break;
        }
        case ::avro::AVRO_ARRAY:
        case ::avro::AVRO_MAP: {
            const bool map = node->type() == ::avro::AVRO_MAP || is_logical_map(node);
            if (is_logical_map(node)) {
                const auto entry = resolved_node(node->leafAt(0));
                if (entry->type() != ::avro::AVRO_RECORD || entry->leaves() != 2 ||
                    entry->nameAt(0) != "key" || entry->nameAt(1) != "value") {
                    return Status::InvalidArgument("Invalid Avro logical map entry schema");
                }
                ColumnDefinition entries;
                RETURN_IF_ERROR(build_schema(entry, "entry", 0, timestamp_tz, varbinary, ancestors,
                                             &entries));
                column->children = std::move(entries.children);
            } else {
                for (size_t i = 0; std::cmp_less(i, (map ? 2 : 1)); ++i) {
                    ColumnDefinition child;
                    RETURN_IF_ERROR(build_schema(
                            node->leafAt(i), map ? (i == 0 ? "key" : "value") : "element",
                            static_cast<int32_t>(i), timestamp_tz, varbinary, ancestors, &child));
                    RETURN_IF_ERROR(set_identifier(
                            node, 0, map ? (i == 0 ? "key-id" : "value-id") : "element-id",
                            &child));
                    column->children.push_back(std::move(child));
                }
            }
            type = map ? DataTypePtr(std::make_shared<DataTypeMap>(column->children[0].type,
                                                                   column->children[1].type))
                       : DataTypePtr(std::make_shared<DataTypeArray>(column->children[0].type));
            break;
        }
        default:
            return Status::NotSupported("Unsupported Avro schema type");
        }
    }
    column->type = make_nullable(type);
    return Status::OK();
}

::avro::NodePtr clone_projected_schema(
        const ::avro::NodePtr& input,
        std::unordered_map<const ::avro::Node*, ::avro::NodePtr>* copies,
        const std::unordered_set<int32_t>* selected_roots = nullptr) {
    const auto source = resolved_node(input);
    if (const auto found = copies->find(source.get()); found != copies->end()) {
        return found->second;
    }
    ::avro::NodePtr result;
    switch (source->type()) {
    case ::avro::AVRO_RECORD:
        result = std::make_shared<::avro::NodeRecord>();
        break;
    case ::avro::AVRO_ENUM:
        result = std::make_shared<::avro::NodeEnum>();
        break;
    case ::avro::AVRO_FIXED:
        result = std::make_shared<::avro::NodeFixed>();
        break;
    case ::avro::AVRO_ARRAY:
        result = std::make_shared<::avro::NodeArray>();
        break;
    case ::avro::AVRO_MAP:
        result = std::make_shared<::avro::NodeMap>();
        break;
    case ::avro::AVRO_UNION:
        result = std::make_shared<::avro::NodeUnion>();
        break;
    default:
        result = std::make_shared<::avro::NodePrimitive>(source->type());
        break;
    }
    copies->emplace(source.get(), result);
    if (source->hasName()) {
        result->setName(source->name());
    }
    if (source->type() == ::avro::AVRO_FIXED) {
        result->setFixedSize(source->fixedSize());
    }
    for (size_t i = source->type() == ::avro::AVRO_MAP ? 1 : 0; i < source->leaves(); ++i) {
        if (selected_roots != nullptr && !selected_roots->contains(static_cast<int32_t>(i))) {
            continue;
        }
        // Null-first datum schemas avoid constructing a large default value merely to read
        // null from a writer union such as [fixed, null]. The decoder remaps writer indices.
        const bool swap_union = source->type() == ::avro::AVRO_UNION && source->leaves() == 2 &&
                                source->leafAt(1)->type() == ::avro::AVRO_NULL;
        const auto child = swap_union ? 1 - i : i;
        result->addLeaf(clone_projected_schema(source->leafAt(child), copies));
        if (source->type() == ::avro::AVRO_RECORD) {
            result->addName(source->nameAt(i));
        }
    }
    if (source->type() == ::avro::AVRO_ENUM) {
        for (size_t i = 0; i < source->names(); ++i) {
            result->addName(source->nameAt(i));
        }
    }
    // Rebuild named definitions instead of retaining symbolic references to omitted roots.
    // The ValidSchema validator replaces repeated definitions with links within this schema.
    result->setLogicalType(source->logicalType());
    return result;
}

void apply_timestamp_projection(ColumnDefinition* column, const LocalColumnIndex& projection) {
    if (projection.timestamp_is_adjusted_to_utc.has_value()) {
        column->timestamp_is_adjusted_to_utc = projection.timestamp_is_adjusted_to_utc;
    }
    for (const auto& child_projection : projection.children) {
        for (auto& child : column->children) {
            if (child.local_id == child_projection.local_id()) {
                apply_timestamp_projection(&child, child_projection);
                break;
            }
        }
    }
}

Status append_decimal(const std::vector<uint8_t>& bytes, const DataTypePtr& type, IColumn* column) {
    // Avro decimals are signed, big-endian two's-complement integers, including FIXED values.
    size_t start = 0;
    // FIXED may be wider than its precision requires; discard only redundant sign extension.
    while (start + 1 < bytes.size() && ((bytes[start] == 0 && (bytes[start + 1] & 0x80) == 0) ||
                                        (bytes[start] == 255 && (bytes[start + 1] & 0x80) != 0))) {
        ++start;
    }
    if (bytes.empty() || bytes.size() - start > 32) {
        return Status::InvalidArgument("Invalid Avro decimal width");
    }
    Int256 value = (bytes[start] & 0x80) != 0 ? -1 : 0;
    for (; start < bytes.size(); ++start) {
        value = value * 256 + bytes[start];
    }
    Int256 limit = 1;
    for (unsigned i = 0; i < type->get_precision(); ++i) {
        limit *= 10;
    }
    if (value <= -limit || value >= limit) {
        return Status::InvalidArgument("Avro decimal exceeds its precision");
    }
    switch (type->get_primitive_type()) {
    case TYPE_DECIMAL32:
        column->insert(Field::create_field<TYPE_DECIMAL32>(Decimal32(static_cast<int32_t>(value))));
        break;
    case TYPE_DECIMAL64:
        column->insert(Field::create_field<TYPE_DECIMAL64>(Decimal64(static_cast<int64_t>(value))));
        break;
    case TYPE_DECIMAL128I:
        column->insert(
                Field::create_field<TYPE_DECIMAL128I>(Decimal128V3(static_cast<int128_t>(value))));
        break;
    case TYPE_DECIMAL256:
        column->insert(Field::create_field<TYPE_DECIMAL256>(Decimal256(value)));
        break;
    default:
        return Status::InternalError("Invalid Avro decimal output type");
    }
    return Status::OK();
}

Status rebase_hybrid_day(int64_t* day) {
    if (*day >= -141427) {
        return Status::OK();
    }
    // Hive's hybrid 1582-10-04 is epoch day -141428. Reinterpret its Julian civil date;
    // subtracting ten days would be wrong for older centuries and Julian leap years.
    if (*day < -719530) {
        return Status::InvalidArgument("Avro date is out of range");
    }
    const int64_t c = *day + 2440588 + 32082;
    const int64_t d = (4 * c + 3) / 1461;
    const int64_t e = c - 1461 * d / 4;
    const int64_t m = (5 * e + 2) / 153;
    const int64_t year = d - 4800 + m / 10;
    const int64_t month = m + 3 - 12 * (m / 10);
    const int64_t dom = e - (153 * m + 2) / 5 + 1;
    DateV2Value<DateV2ValueType> first;
    if (!first.check_range_and_set_time(static_cast<uint16_t>(year), static_cast<uint8_t>(month), 1,
                                        0, 0, 0, 0)) {
        return Status::InvalidArgument("Avro date is out of range");
    }
    // Hive normalizes Julian-only leap days through a lenient Gregorian calendar.
    *day = first.daynr() + dom - 1 - 719528;
    return Status::OK();
}

// One visitor preserves the same null/child-offset rules for all Avro value alternatives.
// NOLINTNEXTLINE(readability-function-size,readability-function-cognitive-complexity)
Status append_value(const ::avro::GenericDatum& datum, const ColumnDefinition& schema,
                    IColumn* output, const DataTypePtr& output_type,
                    const AvroTemporalPolicy& temporal) {
    if (datum.type() == ::avro::AVRO_NULL) {
        output->insert_default();
        return Status::OK();
    }
    auto& nullable = assert_cast<ColumnNullable&>(*output);
    auto* column = &nullable.get_nested_column();
    // A load and an Iceberg scan may carry the same adjusted marker but use different physical
    // types. Insert the representation selected by the actual file block, including nested leaves.
    const auto type = remove_nullable(output_type);
    const auto& timezone = temporal.timezone;
    const bool legacy_calendar = temporal.legacy_calendar;
    using Logical = ::avro::LogicalType;
    const auto logical = datum.logicalType().type();
    if (logical == Logical::DECIMAL) {
        const auto& bytes = datum.type() == ::avro::AVRO_FIXED
                                    ? datum.value<::avro::GenericFixed>().value()
                                    : datum.value<std::vector<uint8_t>>();
        RETURN_IF_ERROR(append_decimal(bytes, type, column));
    } else if (logical == Logical::UUID) {
        UUIDValueType value;
        if (datum.type() == ::avro::AVRO_FIXED) {
            const auto& bytes = datum.value<::avro::GenericFixed>().value();
            if (bytes.size() != UUIDValue::BINARY_LENGTH) {
                return Status::InvalidArgument("Invalid Avro UUID width");
            }
            value = UUIDValue::from_big_endian(bytes.data());
        } else if (!UUIDValue::from_string(value, datum.value<std::string>())) {
            return Status::InvalidArgument("Invalid Avro UUID value");
        }
        column->insert(Field::create_field<TYPE_UUID>(value));
    } else if (logical == Logical::DATE) {
        DateV2Value<DateV2ValueType> date;
        int64_t day = datum.value<int32_t>();
        if (legacy_calendar) {
            RETURN_IF_ERROR(rebase_hybrid_day(&day));
        }
        day += 719528;
        if (day < 0 || !date.get_date_from_daynr(day)) {
            return Status::InvalidArgument("Avro date is out of range");
        }
        column->insert(Field::create_field<TYPE_DATEV2>(date));
    } else if (logical == Logical::TIME_MILLIS || logical == Logical::TIME_MICROS) {
        const int64_t micros = logical == Logical::TIME_MILLIS
                                       ? static_cast<int64_t>(datum.value<int32_t>()) * 1000
                                       : datum.value<int64_t>();
        if (micros < 0 || micros >= 86400000000LL) {
            return Status::InvalidArgument("Avro time is out of range");
        }
        column->insert(Field::create_field<TYPE_TIMEV2>(static_cast<double>(micros)));
    } else if (logical == Logical::TIMESTAMP_NANOS || logical == Logical::LOCAL_TIMESTAMP_NANOS) {
        column->insert(
                Field::create_field<TYPE_TIMESTAMP_NS>(TimeStampNsValue(datum.value<int64_t>())));
    } else if (logical == Logical::TIMESTAMP_MILLIS || logical == Logical::TIMESTAMP_MICROS ||
               logical == Logical::LOCAL_TIMESTAMP_MILLIS ||
               logical == Logical::LOCAL_TIMESTAMP_MICROS) {
        const int64_t units =
                logical == Logical::TIMESTAMP_MILLIS || logical == Logical::LOCAL_TIMESTAMP_MILLIS
                        ? 1000
                        : 1000000;
        const auto raw = datum.value<int64_t>();
        int64_t seconds = raw / units;
        int64_t fraction = raw % units;
        if (fraction < 0) {
            --seconds;
            fraction += units;
        }
        const bool adjusted = schema.timestamp_is_adjusted_to_utc.value_or(
                logical == Logical::TIMESTAMP_MILLIS || logical == Logical::TIMESTAMP_MICROS);
        if (temporal.legacy_timezone && adjusted && seconds < -2208988800LL) {
            // Metadata-free Hive 2 timestamps use Java's pre-1900 raw-offset rules; applying
            // historical tzdb LMT would silently change the civil clock (e.g. by 238s in New York).
            return Status::NotSupported(
                    "Hive Avro timestamps before 1900 without writer timezone are unsupported");
        }
        if (seconds < -62167305600LL || seconds > 253402387199LL) {
            return Status::InvalidArgument("Avro timestamp is out of range");
        }
        DateV2Value<DateTimeV2ValueType> value;
        // Local timestamps encode a civil clock. UTC is only its arithmetic epoch anchor;
        // applying the session zone would change that clock (for example 00:00 into 08:00).
        const auto zone = adjusted && type->get_primitive_type() != TYPE_TIMESTAMPTZ
                                  ? timezone
                                  : cctz::utc_time_zone();
        // DateTimeV2's fractional argument is nanoseconds, even when the output scale is six.
        value.from_unixtime(seconds, static_cast<int32_t>(fraction * (1000000000 / units)), zone,
                            6);
        if (!value.is_valid_date()) {
            return Status::InvalidArgument("Avro timestamp is out of range");
        }
        if (legacy_calendar) {
            // Restore the writer-zone civil clock first, then rebase its calendar date.
            int64_t day = value.daynr() - 719528;
            RETURN_IF_ERROR(rebase_hybrid_day(&day));
            DateV2Value<DateV2ValueType> date;
            if (!date.get_date_from_daynr(day + 719528)) {
                return Status::InvalidArgument("Avro timestamp is out of range");
            }
            value.unchecked_set_time(date.year(), date.month(), date.day(), value.hour(),
                                     value.minute(), value.second(), value.microsecond());
        }
        if (type->get_primitive_type() == TYPE_TIMESTAMPTZ) {
            column->insert(Field::create_field<TYPE_TIMESTAMPTZ>(TimestampTzValue(value)));
        } else {
            column->insert(Field::create_field<TYPE_DATETIMEV2>(value));
        }
    } else {
        switch (datum.type()) {
        case ::avro::AVRO_BOOL:
            column->insert(Field::create_field<TYPE_BOOLEAN>(datum.value<bool>()));
            break;
        case ::avro::AVRO_INT:
            column->insert(Field::create_field<TYPE_INT>(datum.value<int32_t>()));
            break;
        case ::avro::AVRO_LONG:
            column->insert(Field::create_field<TYPE_BIGINT>(datum.value<int64_t>()));
            break;
        case ::avro::AVRO_FLOAT:
            column->insert(Field::create_field<TYPE_FLOAT>(datum.value<float>()));
            break;
        case ::avro::AVRO_DOUBLE:
            column->insert(Field::create_field<TYPE_DOUBLE>(datum.value<double>()));
            break;
        case ::avro::AVRO_STRING: {
            const auto& value = datum.value<std::string>();
            column->insert_data(value.data(), value.size());
            break;
        }
        case ::avro::AVRO_ENUM: {
            const auto& value = datum.value<::avro::GenericEnum>().symbol();
            column->insert_data(value.data(), value.size());
            break;
        }
        case ::avro::AVRO_BYTES:
        case ::avro::AVRO_FIXED: {
            const auto& value = datum.type() == ::avro::AVRO_FIXED
                                        ? datum.value<::avro::GenericFixed>().value()
                                        : datum.value<std::vector<uint8_t>>();
            column->insert_data(reinterpret_cast<const char*>(value.data()), value.size());
            break;
        }
        case ::avro::AVRO_RECORD: {
            const auto& record = datum.value<::avro::GenericRecord>();
            auto& target = assert_cast<ColumnStruct&>(*column);
            for (size_t i = 0; i < schema.children.size(); ++i) {
                RETURN_IF_ERROR(append_value(
                        record.fieldAt(i), schema.children[i], &target.get_column(i),
                        assert_cast<const DataTypeStruct&>(*type).get_elements()[i], temporal));
            }
            break;
        }
        case ::avro::AVRO_ARRAY: {
            const auto& values = datum.value<::avro::GenericArray>().value();
            if (type->get_primitive_type() == TYPE_MAP) {
                auto& target = assert_cast<ColumnMap&>(*column);
                for (const auto& entry : values) {
                    const auto& record = entry.value<::avro::GenericRecord>();
                    RETURN_IF_ERROR(append_value(
                            record.fieldAt(0), schema.children[0], &target.get_keys(),
                            assert_cast<const DataTypeMap&>(*type).get_key_type(), temporal));
                    RETURN_IF_ERROR(append_value(
                            record.fieldAt(1), schema.children[1], &target.get_values(),
                            assert_cast<const DataTypeMap&>(*type).get_value_type(), temporal));
                }
                target.get_offsets().push_back(target.get_keys().size());
            } else {
                auto& target = assert_cast<ColumnArray&>(*column);
                for (const auto& value : values) {
                    RETURN_IF_ERROR(append_value(
                            value, schema.children[0], &target.get_data(),
                            assert_cast<const DataTypeArray&>(*type).get_nested_type(), temporal));
                }
                target.get_offsets().push_back(target.get_data().size());
            }
            break;
        }
        case ::avro::AVRO_MAP: {
            auto& target = assert_cast<ColumnMap&>(*column);
            for (const auto& [key, value] : datum.value<::avro::GenericMap>().value()) {
                auto& keys = assert_cast<ColumnNullable&>(target.get_keys());
                keys.get_nested_column().insert_data(key.data(), key.size());
                keys.get_null_map_data().push_back(0);
                RETURN_IF_ERROR(append_value(
                        value, schema.children[1], &target.get_values(),
                        assert_cast<const DataTypeMap&>(*type).get_value_type(), temporal));
            }
            target.get_offsets().push_back(target.get_keys().size());
            break;
        }
        default:
            return Status::NotSupported("Unsupported Avro value type");
        }
    }
    nullable.get_null_map_data().push_back(0);
    return Status::OK();
}

size_t datum_scratch_bytes(const ::avro::GenericDatum& datum) {
    // type()/value() unwrap a union, but its std::any-owned wrapper remains allocated.
    const size_t wrapper = datum.isUnion() ? sizeof(::avro::GenericUnion) : 0;
    switch (datum.type()) {
    case ::avro::AVRO_STRING:
        return wrapper + sizeof(std::string) +
               (datum.value<std::string>().capacity() > std::string().capacity()
                        ? datum.value<std::string>().capacity() + 1
                        : 0);
    case ::avro::AVRO_BYTES:
        return wrapper + sizeof(std::vector<uint8_t>) +
               datum.value<std::vector<uint8_t>>().capacity();
    case ::avro::AVRO_FIXED:
        return wrapper + sizeof(::avro::GenericFixed) +
               datum.value<::avro::GenericFixed>().value().capacity();
    case ::avro::AVRO_RECORD: {
        const auto& record = datum.value<::avro::GenericRecord>();
        size_t bytes =
                wrapper + sizeof(record) + record.fieldCount() * sizeof(::avro::GenericDatum);
        for (size_t i = 0; i < record.fieldCount(); ++i) {
            bytes += datum_scratch_bytes(record.fieldAt(i));
        }
        return bytes;
    }
    case ::avro::AVRO_ARRAY: {
        const auto& array = datum.value<::avro::GenericArray>().value();
        size_t bytes = wrapper + sizeof(::avro::GenericArray) +
                       array.capacity() * sizeof(::avro::GenericDatum);
        for (const auto& value : array) {
            bytes += datum_scratch_bytes(value);
        }
        return bytes;
    }
    case ::avro::AVRO_MAP: {
        const auto& map = datum.value<::avro::GenericMap>().value();
        size_t bytes = wrapper + sizeof(::avro::GenericMap) +
                       map.capacity() * sizeof(std::remove_reference_t<decltype(map)>::value_type);
        for (const auto& [key, value] : map) {
            bytes += key.capacity() + datum_scratch_bytes(value);
        }
        return bytes;
    }
    case ::avro::AVRO_ENUM:
        return wrapper + sizeof(::avro::GenericEnum);
    default:
        return wrapper;
    }
}

} // namespace

struct AvroReader::Impl {
    std::shared_ptr<MemTrackerLimiter> memory_tracker;
    DorisVector<ReservedMemoryToken> reservations;
    int64_t accounted_scratch = 0;
    size_t schema_scratch = 0;
    RuntimeProfile::Counter* scratch_accounting_passes = nullptr;
    RuntimeProfile::Counter* total_time = nullptr;
    RuntimeProfile::Counter* decode_materialize_time = nullptr;
    RuntimeProfile::Counter* filter_time = nullptr;
    size_t small_reservation_available = 0;
    size_t collection_scratch_before_reset = 0;

    static void reserve_callback(void* context, size_t bytes) {
        auto* self = static_cast<Impl*>(context);
        if (bytes > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
            throw Exception(ErrorCode::MEM_LIMIT_EXCEEDED, "Avro allocation exceeds Int64");
        }
        if (bytes <= self->small_reservation_available) {
            self->small_reservation_available -= bytes;
            return;
        }
        // Batch tiny std::any allocations so large nullable arrays do not acquire one token per item.
        const size_t reservation = std::max(bytes, size_t(64 * 1024));
        auto* manager = thread_context()->thread_mem_tracker_mgr.get();
        // Preserve the scanner's existing reservation; each Avro growth owns an independent token.
        auto previous = manager->take_reserved_memory();
        Defer restore([&] { manager->adopt_reserved_memory(std::move(previous)); });
        const auto status = manager->try_reserve(static_cast<int64_t>(reservation));
        if (!status.ok()) {
            throw Exception(status);
        }
        self->reservations.push_back(manager->take_reserved_memory());
        if (bytes < reservation) {
            self->small_reservation_available = reservation - bytes;
        }
    }

    static std::shared_ptr<void> reserve_workspace(size_t bytes) {
        if (bytes > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
            throw Exception(ErrorCode::MEM_LIMIT_EXCEEDED, "Avro codec workspace exceeds Int64");
        }
        auto* manager = thread_context()->thread_mem_tracker_mgr.get();
        auto previous = manager->take_reserved_memory();
        Defer restore([&] { manager->adopt_reserved_memory(std::move(previous)); });
        const auto status = manager->try_reserve(static_cast<int64_t>(bytes));
        if (!status.ok()) {
            throw Exception(status);
        }
        // Codec contexts die at block/frame boundaries, including when no row is produced.
        return std::make_shared<ReservedMemoryToken>(manager->take_reserved_memory());
    }

    static void checkpoint_callback(void* context, const ::avro::GenericDatum& collection,
                                    bool before) {
        auto* self = static_cast<Impl*>(context);
        const size_t retained = datum_scratch_bytes(collection);
        if (before) {
            self->collection_scratch_before_reset = retained;
        } else {
            // Collection resets and switches to null release previously-accounted children.
            // The decoder suppresses union checkpoints for newly constructed branches.
            const auto released =
                    static_cast<int64_t>(self->collection_scratch_before_reset - retained);
            DCHECK_LE(released, self->accounted_scratch);
            self->memory_tracker->release(released);
            self->accounted_scratch -= released;
        }
    }

    void reconcile_scratch() {
        if (scratch_accounting_passes != nullptr) {
            COUNTER_UPDATE(scratch_accounting_passes, 1);
        }
        const size_t retained =
                schema_scratch + (reader ? reader->scratchBytes() : 0) +
                (datum ? sizeof(::avro::GenericDatum) + datum_scratch_bytes(*datum) : 0);
        // Replace conservative transient-growth reservations with retained capacities after every
        // record. Retained codec and datum buffers remain charged between batches until close.
        memory_tracker->consume(static_cast<int64_t>(retained) - accounted_scratch);
        accounted_scratch = static_cast<int64_t>(retained);
        reservations.clear();
        small_reservation_available = 0;
    }

    bool hive_compatible = false;
    bool timestamp_tz;
    bool varbinary;
    AvroTemporalPolicy temporal;
    size_t batch_size = 4096;
    size_t max_block_bytes = std::numeric_limits<size_t>::max();
    std::unordered_set<int32_t> count_placeholders;
    std::unordered_map<int32_t, size_t> projected_ordinals;
    std::unique_ptr<AvroContainerReader> reader;
    std::unique_ptr<::avro::GenericDatum> datum;
    std::vector<ColumnDefinition> schema;
    int64_t header_end = 0;
    int64_t range_end = std::numeric_limits<int64_t>::max();
    int64_t row_position = 0;
    size_t next_requested_row = 0;
    std::optional<GlobalRowIdContext> global_rowid_context;
};

AvroReader::AvroReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                       std::unique_ptr<io::FileDescription>& file_description,
                       std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
                       bool enable_mapping_timestamp_tz, bool enable_mapping_varbinary,
                       std::optional<GlobalRowIdContext> global_rowid_context, bool hive_compatible)
        : FileReader(system_properties, file_description, std::move(io_ctx), profile),
          _impl(std::make_unique<Impl>()) {
    _impl->hive_compatible = hive_compatible;
    _impl->timestamp_tz = enable_mapping_timestamp_tz && !hive_compatible;
    _impl->varbinary = enable_mapping_varbinary;
    _impl->global_rowid_context = global_rowid_context;
}

AvroReader::~AvroReader() {
    (void)close();
}

Status AvroReader::init(RuntimeState* state) {
    _impl->memory_tracker = thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker_sptr();
    RETURN_IF_ERROR(FileReader::init(state));
    if (_profile != nullptr) {
        file_scan_profile::ensure_hierarchy(_profile);
        _impl->total_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "AvroReader",
                                                       file_scan_profile::FILE_READER, 1);
        _impl->decode_materialize_time = ADD_CHILD_TIMER_WITH_LEVEL(
                _profile, "AvroDecodeAndMaterializeTime", "AvroReader", 1);
        _impl->filter_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "AvroFilterTime", "AvroReader", 1);
        _impl->scratch_accounting_passes = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "AvroScratchAccountingPasses", TUnit::UNIT, "AvroReader", 1);
    }
    SCOPED_TIMER(_impl->total_time);
    if (state != nullptr) {
        _impl->temporal.timezone = state->timezone_obj();
        _impl->max_block_bytes = state->preferred_block_size_bytes();
    }
    static std::once_flag register_map;
    std::call_once(register_map, [] {
        // Iceberg's non-string-key maps are arrays of key/value records with logicalType=map.
        ::avro::CustomLogicalTypeRegistry::instance().registerType("map", [](const std::string&) {
            return std::make_shared<::avro::CustomLogicalType>("map");
        });
    });
    try {
        Impl::reserve_callback(_impl.get(), 128 * 1024);
        _impl->reader = std::make_unique<AvroContainerReader>(
                _tracing_file_reader, _io_ctx,
                [this](size_t bytes) { Impl::reserve_callback(_impl.get(), bytes); },
                [this](const ::avro::GenericDatum& datum, bool before) {
                    Impl::checkpoint_callback(_impl.get(), datum, before);
                },
                &Impl::reserve_workspace, _profile);
        if (_impl->hive_compatible) {
            const auto zone = _impl->reader->metadata("writer.time.zone");
            if (zone && !parse_writer_timezone(*zone, &_impl->temporal.timezone)) {
                return Status::InvalidArgument("Invalid Avro writer timezone");
            }
            _impl->temporal.legacy_timezone = !zone.has_value();
            const auto proleptic = _impl->reader->metadata("writer.proleptic");
            if (proleptic && *proleptic != "true" && *proleptic != "false") {
                return Status::InvalidArgument("Invalid Avro writer calendar");
            }
            // Hive 2 has neither key and writes a hybrid calendar. Hive 3 adds its writer zone
            // and writes proleptic dates; Hive 4 records the calendar choice explicitly.
            _impl->temporal.legacy_calendar = proleptic ? *proleptic == "false" : !zone.has_value();
            if (_impl->reader->metadata("writer.zone.conversion.legacy") == "true") {
                return Status::NotSupported("Legacy Hive Avro timezone conversion is unsupported");
            }
        }
        const auto root = _impl->reader->dataSchema().root();
        if (root->type() != ::avro::AVRO_RECORD) {
            return Status::NotSupported("Avro container root must be a record");
        }
        _impl->header_end = _impl->reader->position();
        std::unordered_set<const ::avro::Node*> ancestors;
        std::unordered_map<const ::avro::Node*, size_t> schema_sizes;
        _impl->schema_scratch = expanded_schema_bytes(root, &schema_sizes, &ancestors);
        Impl::reserve_callback(_impl.get(), _impl->schema_scratch);
        ColumnDefinition record;
        RETURN_IF_ERROR(build_schema(root, "row", 0, _impl->timestamp_tz, _impl->varbinary,
                                     &ancestors, &record));
        _impl->schema = std::move(record.children);
        _impl->reconcile_scratch();
        return Status::OK();
    } catch (const doris::Exception& e) {
        return e.to_status();
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("Avro allocation failed");
    } catch (const std::exception& e) {
        return Status::InvalidArgument("Cannot read Avro schema: {}", e.what());
    }
}

Status AvroReader::get_schema(std::vector<ColumnDefinition>* file_schema) const {
    *file_schema = _impl->schema;
    if (_impl->global_rowid_context.has_value()) {
        auto rowid = global_rowid_column_definition();
        rowid.type = make_nullable(rowid.type);
        file_schema->push_back(std::move(rowid));
    }
    return Status::OK();
}

std::unique_ptr<TableColumnMapper> AvroReader::create_column_mapper(
        TableColumnMapperOptions options) const {
    return std::make_unique<MaterializedColumnMapper>(options);
}

// Range ownership and initialization update the same absolute row-position state.
// NOLINTNEXTLINE(readability-function-size,readability-function-cognitive-complexity)
Status AvroReader::open(std::shared_ptr<FileScanRequest> request) {
    SCOPED_TIMER(_impl->total_time);
    RETURN_IF_ERROR(FileReader::open(std::move(request)));
    try {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_impl->memory_tracker);
        // The mapper communicates per-leaf physical decoding semantics through local projections.
        // Apply them before writing into the file block, whose types already carry these markers.
        auto apply_projection = [&](const LocalColumnIndex& projection) {
            const auto id = projection.local_id();
            if (id >= 0 && static_cast<size_t>(id) < _impl->schema.size()) {
                apply_timestamp_projection(&_impl->schema[id], projection);
            }
        };
        for (const auto& projection : _request->predicate_columns) {
            apply_projection(projection);
        }
        for (const auto& projection : _request->non_predicate_columns) {
            apply_projection(projection);
        }
        // Equality-delete dependencies may promote a COUNT carrier after it was marked.
        // Such columns still need their actual values for the predicate to remove rows.
        for (const auto id : _request->count_star_placeholder_columns) {
            if (std::ranges::none_of(_request->predicate_columns, [&](const auto& projection) {
                    return projection.column_id() == id;
                })) {
                _impl->count_placeholders.insert(id.value());
            }
        }
        std::unordered_set<int32_t> selected;
        for (const auto& [id, position] : _request->local_positions) {
            if (id.value() >= 0 && !_impl->count_placeholders.contains(id.value())) {
                if (static_cast<size_t>(id.value()) >= _impl->schema.size()) {
                    return Status::InvalidArgument("Unknown Avro file-local column");
                }
                selected.insert(id.value());
            }
        }
        size_t projected = 0;
        for (size_t i = 0; i < _impl->schema.size(); ++i) {
            if (selected.contains(static_cast<int32_t>(i))) {
                _impl->projected_ordinals.emplace(static_cast<int32_t>(i), projected++);
            }
        }
        const int64_t start = _file_description->range_start_offset;
        const int64_t size = _file_description->range_size;
        if (start < 0 || size < -1 ||
            (size >= 0 && start > std::numeric_limits<int64_t>::max() - size)) {
            return Status::InvalidArgument("Invalid Avro file range");
        }
        _impl->range_end = size < 0 ? std::numeric_limits<int64_t>::max() : start + size;
        // Position deletes and virtual row ids need absolute coordinates. Ordinary scans can
        // align directly to a sync marker instead of enumerating all preceding block headers.
        const bool absolute_positions =
                _request->row_ids.has_value() ||
                _request->local_positions.contains(LocalColumnId(ROW_POSITION_COLUMN_ID)) ||
                _request->local_positions.contains(LocalColumnId(GLOBAL_ROWID_COLUMN_ID));
        _impl->row_position = _impl->reader->seek_range(start, absolute_positions);
        const auto offset = _impl->reader->position();
        _eof = std::cmp_greater_equal(offset, _impl->range_end) ||
               static_cast<uint64_t>(offset) >= _tracing_file_reader->size();
        if (!_eof) {
            // Decode only requested roots. The resolving decoder skips the other writer fields,
            // avoiding GenericDatum allocations for wide unprojected strings and collections.
            std::unordered_map<const ::avro::Node*, ::avro::NodePtr> copies;
            ::avro::ValidSchema projected_schema(
                    clone_projected_schema(_impl->reader->dataSchema().root(), &copies, &selected));
            _impl->reader->init(projected_schema, offset);
        }
        _impl->reconcile_scratch();
        return Status::OK();
    } catch (const doris::Exception& e) {
        return e.to_status();
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("Avro allocation failed");
    } catch (const std::exception& e) {
        return Status::InvalidArgument("Cannot open Avro data: {}", e.what());
    }
}

void AvroReader::set_batch_size(size_t batch_size) {
    _impl->batch_size = std::max(size_t(1), batch_size);
}

// Keep selected-row advancement, block ownership and final filtering in one batch transition.
// NOLINTNEXTLINE(readability-function-size,readability-function-cognitive-complexity)
Status AvroReader::get_block(Block* file_block, size_t* rows, bool* eof) {
    SCOPED_TIMER(_impl->total_time);
    *rows = 0;
    *eof = _eof;
    if (_eof) {
        return Status::OK();
    }
    try {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_impl->memory_tracker);
        {
            // Decode and append share one row traversal; time the batch without per-row clocks.
            SCOPED_TIMER(_impl->decode_materialize_time);
            while (*rows < _impl->batch_size &&
                   (*rows == 0 || file_block->bytes() < _impl->max_block_bytes)) {
                if (_io_ctx != nullptr && _io_ctx->should_stop) {
                    _eof = true;
                    break;
                }
                if (_request->row_ids.has_value() &&
                    _impl->next_requested_row == _request->row_ids->size()) {
                    _eof = true;
                    break;
                }
                if (!_impl->reader->hasMoreBefore(_impl->range_end)) {
                    _eof = true;
                    break;
                }
                if (_impl->datum == nullptr) {
                    _impl->datum = _impl->reader->make_datum();
                    // Publish the admitted skeleton before reset callbacks can release old children.
                    _impl->reconcile_scratch();
                }
                _impl->reader->read(*_impl->datum);
                _impl->reconcile_scratch();
                if (_request->row_ids.has_value()) {
                    if (_impl->row_position != (*_request->row_ids)[_impl->next_requested_row]) {
                        ++_impl->row_position;
                        continue;
                    }
                    ++_impl->next_requested_row;
                }
                const auto& record = _impl->datum->value<::avro::GenericRecord>();
                for (const auto& [id, position] : _request->local_positions) {
                    auto& column = file_block->get_by_position(position.value())
                                           .column->assert_mutable_ref();
                    if (id.value() == ROW_POSITION_COLUMN_ID) {
                        column.insert(Field::create_field<TYPE_BIGINT>(_impl->row_position));
                    } else if (id.value() == GLOBAL_ROWID_COLUMN_ID) {
                        if (!_impl->global_rowid_context.has_value()) {
                            return Status::InvalidArgument(
                                    "Avro global row id requires row id context");
                        }
                        if (std::cmp_greater(_impl->row_position,
                                             std::numeric_limits<uint32_t>::max())) {
                            return Status::NotSupported("Avro global row position exceeds UInt32");
                        }
                        const auto& context = *_impl->global_rowid_context;
                        const GlobalRowLoacationV2 location(
                                context.version, context.backend_id, context.file_id,
                                static_cast<uint32_t>(_impl->row_position));
                        auto* output = &column;
                        if (auto* nullable = check_and_get_column<ColumnNullable>(column)) {
                            nullable->get_null_map_data().push_back(0);
                            output = &nullable->get_nested_column();
                        }
                        output->insert_data(reinterpret_cast<const char*>(&location),
                                            sizeof(location));
                    } else if (_impl->count_placeholders.contains(id.value())) {
                        // COUNT(*) needs the physical cardinality, not a potentially lossy value cast.
                        column.insert_default();
                    } else {
                        if (id.value() < 0 ||
                            static_cast<size_t>(id.value()) >= _impl->schema.size()) {
                            return Status::InvalidArgument("Unknown Avro file-local column");
                        }
                        RETURN_IF_ERROR(append_value(
                                record.fieldAt(_impl->projected_ordinals.at(id.value())),
                                _impl->schema[id.value()], &column,
                                file_block->get_by_position(position.value()).type,
                                _impl->temporal));
                    }
                }
                ++_impl->row_position;
                ++*rows;
            }
        }
        _record_scan_rows(*rows);
        SCOPED_TIMER(_impl->filter_time);
        RETURN_IF_ERROR(
                apply_materialized_reader_filters(_request.get(), _io_ctx.get(), file_block, rows));
        // A nonempty last batch must be consumed before EOF, including after delete filtering.
        *eof = _eof && *rows == 0;
        return Status::OK();
    } catch (const doris::Exception& e) {
        return e.to_status();
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("Avro allocation failed");
    } catch (const std::exception& e) {
        return Status::InvalidArgument("Cannot decode Avro data: {}", e.what());
    }
}

Status AvroReader::close() {
    if (_impl->memory_tracker != nullptr) {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_impl->memory_tracker);
        _impl->datum.reset();
        _impl->reader.reset();
        std::vector<ColumnDefinition>().swap(_impl->schema);
        _impl->schema_scratch = 0;
        _impl->memory_tracker->release(_impl->accounted_scratch);
        _impl->accounted_scratch = 0;
        DorisVector<ReservedMemoryToken>().swap(_impl->reservations);
    }
    return FileReader::close();
}

} // namespace doris::format::avro
