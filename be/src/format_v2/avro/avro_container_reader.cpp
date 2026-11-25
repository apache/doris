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

#include "format_v2/avro/avro_container_reader.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <snappy.h>
#include <zlib.h>
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

#include <algorithm>
#include <array>
#include <avro/Compiler.hh>
#include <avro/Generic.hh>
#include <avro/NodeImpl.hh>
#include <bit>
#include <cstring>
#include <limits>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "io/fs/file_reader.h"
#include "io/io_common.h"
#include "runtime/runtime_profile.h"

namespace doris::format::avro {
namespace {

void require(bool condition, const char* message) {
    if (!condition) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, message);
    }
}

std::string folded_name(std::string name) {
    for (auto& c : name) {
        if (c >= 'A' && c <= 'Z') {
            c += 'a' - 'A';
        }
    }
    return name;
}

// Writer defaults never appear on the wire. Avro C++ eagerly builds their GenericDatum values
// and assumes union branch zero, rejecting legal null defaults on later branches. Strip only
// record-field defaults before compiling a physical schema; retain all types and logical metadata.
void strip_field_defaults(rapidjson::Value& value, bool hive_validation) {
    if (value.IsArray()) {
        for (auto& child : value.GetArray()) {
            strip_field_defaults(child, hive_validation);
        }
    } else if (value.IsObject()) {
        if (hive_validation && value.HasMember("logicalType") && value["logicalType"].IsString()) {
            const std::string_view logical(value["logicalType"].GetString(),
                                           value["logicalType"].GetStringLength());
            // Hive compares decimal/char/varchar with Java equalsIgnoreCase. Reject non-ASCII
            // spellings too, whose Unicode case equivalence the native compiler does not share.
            require(std::ranges::all_of(logical, [](unsigned char c) { return c < 128; }),
                    "Non-ASCII Hive Avro logical annotation is unsupported");
            const auto folded = folded_name(std::string(logical));
            require(folded != "char" && folded != "varchar",
                    "Hive Avro char/varchar coercion is unsupported");
            require(folded != "decimal" || logical == "decimal",
                    "Noncanonical Hive Avro decimal annotation is unsupported");
        }
        if (value.HasMember("fields") && value["fields"].IsArray()) {
            for (auto& field : value["fields"].GetArray()) {
                if (field.IsObject()) {
                    field.RemoveMember("default");
                    if (field.HasMember("type")) {
                        strip_field_defaults(field["type"], hive_validation);
                    }
                }
            }
        }
        for (const auto* key : {"type", "items", "values"}) {
            if (value.HasMember(key)) {
                strip_field_defaults(value[key], hive_validation);
            }
        }
    }
}

void validate_json_keys(const rapidjson::Value& value) {
    if (value.IsObject()) {
        std::unordered_set<std::string_view> keys;
        for (const auto& member : value.GetObject()) {
            const std::string_view key(member.name.GetString(), member.name.GetStringLength());
            // RapidJSON, Gson and Avro C++ choose different duplicate members. Reject them
            // before removing defaults, so no surviving default can reach eager SDK construction.
            require(keys.insert(key).second, "Duplicate Avro schema JSON key");
            validate_json_keys(member.value);
        }
    } else if (value.IsArray()) {
        for (const auto& child : value.GetArray()) {
            validate_json_keys(child);
        }
    }
}

std::string uncomment_hive_schema(const std::string& json) {
    // Hive accepts Java comments, including CR-only // lines. Replace them with whitespace
    // before the depth scan: a comment such as /* [ " */ is not schema structure.
    std::string result = json;
    bool quoted = false, escaped = false;
    for (size_t i = 0; i < json.size(); ++i) {
        const char c = json[i];
        if (quoted) {
            if (escaped) {
                escaped = false;
            } else if (c == '\\') {
                escaped = true;
            } else if (c == '"') {
                quoted = false;
            }
        } else if (c == '"') {
            quoted = true;
        } else if (c == '/' && i + 1 < json.size()) {
            size_t end = i;
            if (json[i + 1] == '/') {
                end = json.find_first_of("\r\n", i + 2);
                end = end == std::string::npos ? json.size() : end;
            } else if (json[i + 1] == '*') {
                end = json.find("*/", i + 2);
                require(end != std::string::npos, "Invalid Avro schema JSON");
                end += 2;
            }
            if (end != i) {
                // Do not concatenate tokens or alter quoted slash/escape bytes.
                std::fill(result.begin() + i, result.begin() + end, ' ');
                i = end - 1;
            }
        }
    }
    return result;
}

rapidjson::Document physical_schema_document(const std::string& json, bool hive_validation) {
    // The caller's schema workspace already covers preprocessing copies. Writer schemas
    // remain strict JSON; only Hive's validated reader literal admits comments.
    const auto uncommented = hive_validation ? uncomment_hive_schema(json) : std::string();
    const auto& source = hive_validation ? uncommented : json;
    // Bound recursion before either JSON parser runs, including header-only files.
    size_t depth = 0;
    bool quoted = false, escaped = false;
    for (const char c : source) {
        if (quoted) {
            if (escaped) {
                escaped = false;
            } else if (c == '\\') {
                escaped = true;
            } else if (c == '"') {
                quoted = false;
            }
        } else if (c == '"') {
            quoted = true;
        } else if (c == '{' || c == '[') {
            require(++depth <= 128, "Avro schema nesting is unsupported");
        } else if (c == '}' || c == ']') {
            require(depth != 0, "Invalid Avro schema JSON");
            --depth;
        }
    }
    require(depth == 0 && !quoted, "Invalid Avro schema JSON");
    rapidjson::Document document;
    document.Parse(source.data(), source.size());
    require(!document.HasParseError(), "Invalid Avro schema JSON");
    validate_json_keys(document);
    strip_field_defaults(document, hive_validation);
    return document;
}

void normalize_iceberg_timestamps(rapidjson::Value& schema) {
    // Visit schema positions only: a union/field/item/value may define a timestamp, whereas
    // defaults and custom metadata objects are not schemas and must retain their own contents.
    if (schema.IsArray()) {
        for (auto& branch : schema.GetArray()) {
            normalize_iceberg_timestamps(branch);
        }
        return;
    }
    if (!schema.IsObject()) {
        return;
    }
    auto type = schema.FindMember("type");
    if (type == schema.MemberEnd()) {
        return;
    }
    if (!type->value.IsString()) {
        normalize_iceberg_timestamps(type->value);
        return;
    }
    const std::string_view physical_type(type->value.GetString(), type->value.GetStringLength());
    if (physical_type == "long") {
        const auto adjusted = schema.FindMember("adjust-to-utc");
        auto logical = schema.FindMember("logicalType");
        // Iceberg 1.12 writes civil timestamps as timestamp-* plus Boolean false. Avro C++
        // drops that custom property; preserve it before compilation or epoch 0 becomes 08:00
        // in a +08 session, and civil nanos is rejected as an unsupported instant.
        if (adjusted != schema.MemberEnd() && adjusted->value.IsBool() &&
            !adjusted->value.GetBool() && logical != schema.MemberEnd() &&
            logical->value.IsString()) {
            const std::string_view name(logical->value.GetString(),
                                        logical->value.GetStringLength());
            if (name == "timestamp-micros") {
                logical->value.SetString(rapidjson::StringRef("local-timestamp-micros"));
            } else if (name == "timestamp-nanos") {
                logical->value.SetString(rapidjson::StringRef("local-timestamp-nanos"));
            }
        }
    } else if (physical_type == "record") {
        const auto fields = schema.FindMember("fields");
        if (fields != schema.MemberEnd() && fields->value.IsArray()) {
            for (auto& field : fields->value.GetArray()) {
                // Malformed record fields remain the SDK compiler's explicit error.
                if (field.IsObject()) {
                    auto field_type = field.FindMember("type");
                    if (field_type != field.MemberEnd()) {
                        normalize_iceberg_timestamps(field_type->value);
                    }
                }
            }
        }
    } else if (physical_type == "array" || physical_type == "map") {
        const auto child = schema.FindMember(physical_type == "array" ? "items" : "values");
        if (child != schema.MemberEnd()) {
            normalize_iceberg_timestamps(child->value);
        }
    }
}

::avro::ValidSchema compile_physical_schema(const std::string& json, bool hive_validation = false,
                                            bool iceberg_compatible = false) {
    auto document = physical_schema_document(json, hive_validation);
    // The Iceberg extension must not change ordinary Avro or Hive instant semantics.
    if (iceberg_compatible) {
        normalize_iceberg_timestamps(document);
    }
    rapidjson::StringBuffer output;
    rapidjson::Writer<rapidjson::StringBuffer> writer(output);
    require(document.Accept(writer), "Cannot serialize Avro physical schema");
    return ::avro::compileJsonSchemaFromString(std::string(output.GetString(), output.GetSize()));
}

::avro::NodePtr physical_node(const ::avro::NodePtr& input) {
    return input->type() == ::avro::AVRO_SYMBOLIC ? ::avro::resolveSymbol(input) : input;
}

::avro::NodePtr non_null_branch(const ::avro::NodePtr& input, bool* nullable) {
    auto node = physical_node(input);
    *nullable = node->type() == ::avro::AVRO_NULL;
    if (node->type() != ::avro::AVRO_UNION) {
        return node;
    }
    ::avro::NodePtr value;
    for (size_t i = 0; i < node->leaves(); ++i) {
        auto child = physical_node(node->leafAt(i));
        if (child->type() == ::avro::AVRO_NULL) {
            *nullable = true;
        } else {
            require(!value, "Reader schema requires unsupported Avro union resolution");
            value = std::move(child);
        }
    }
    return value;
}

void validate_schema_pair(const ::avro::NodePtr& writer, const ::avro::NodePtr& reader,
                          bool match_record_names, size_t depth = 0,
                          bool exact_value_types = false);

void validate_record_pair(const ::avro::NodePtr& from, const ::avro::NodePtr& to,
                          bool match_record_names, size_t depth, bool exact_value_types) {
    require(!match_record_names || from->name() == to->name(),
            "Avro reader record name resolution is unsupported");
    std::unordered_set<std::string> reader_names;
    std::unordered_set<std::string> writer_names;
    bool indexed_writer_names = false;
    for (size_t i = 0; i < to->leaves(); ++i) {
        auto folded = folded_name(to->nameAt(i));
        require(reader_names.insert(folded).second,
                "Avro reader has ambiguous case-insensitive field names");
        size_t writer_index = 0;
        if (from->nameIndex(to->nameAt(i), writer_index)) {
            validate_schema_pair(from->leafAt(writer_index), to->leafAt(i), match_record_names,
                                 depth + 1, exact_value_types);
            continue;
        }
        // The mapper is case-insensitive, whereas Avro resolution is exact. Do not let a
        // missing nullable reader field bind to a differently-cased writer field later.
        if (!indexed_writer_names) {
            for (size_t j = 0; j < from->leaves(); ++j) {
                writer_names.insert(folded_name(from->nameAt(j)));
            }
            indexed_writer_names = true;
        }
        require(!writer_names.contains(folded), "Avro reader field differs only by case");
        require(!exact_value_types, "Projected field is absent from Avro reader schema");
        bool nullable = false;
        (void)non_null_branch(to->leafAt(i), &nullable);
        require(nullable, "Missing Avro reader field without a nullable default");
    }
}

void validate_schema_pair(const ::avro::NodePtr& writer, const ::avro::NodePtr& reader,
                          bool match_record_names, size_t depth, bool exact_value_types) {
    require(depth < 64, "Avro reader schema nesting is unsupported");
    bool writer_null = false, reader_null = false;
    const auto from = non_null_branch(writer, &writer_null);
    const auto to = non_null_branch(reader, &reader_null);
    require(!writer_null || reader_null, "Avro reader schema cannot resolve nullable writer");
    if (!from || from->type() == ::avro::AVRO_NULL) {
        return;
    }
    require(to && to->type() != ::avro::AVRO_NULL, "Incompatible Avro reader schema");
    const auto& from_logical = from->logicalType();
    const auto& to_logical = to->logicalType();
    // A SQL cast after writer-logical decoding is not Avro resolution (date int 1 becomes
    // 19700102). Require identical logical interpretation before projection/predicate mapping.
    require(from_logical.type() == to_logical.type(), "Incompatible Avro reader logical type");
    if (from_logical.type() == ::avro::LogicalType::DECIMAL) {
        // Decimal parameter evolution is not implemented by this physical compatibility gate.
        require(from_logical.precision() == to_logical.precision() &&
                        from_logical.scale() == to_logical.scale(),
                "Avro decimal reader precision/scale evolution is unsupported");
    }
    const auto source = from->type(), target = to->type();
    const bool promoted = (source == ::avro::AVRO_INT &&
                           (target == ::avro::AVRO_LONG || target == ::avro::AVRO_FLOAT ||
                            target == ::avro::AVRO_DOUBLE)) ||
                          (source == ::avro::AVRO_LONG &&
                           (target == ::avro::AVRO_FLOAT || target == ::avro::AVRO_DOUBLE)) ||
                          (source == ::avro::AVRO_FLOAT && target == ::avro::AVRO_DOUBLE) ||
                          (source == ::avro::AVRO_STRING && target == ::avro::AVRO_BYTES) ||
                          (source == ::avro::AVRO_BYTES && target == ::avro::AVRO_STRING);
    // Java resolves bytes to STRING with UTF-8 replacement; raw byte insertion is not equivalent.
    require(source != ::avro::AVRO_BYTES || target != ::avro::AVRO_STRING,
            "Hive Avro bytes-to-string resolution is unsupported");
    const bool identical_string_storage =
            source == ::avro::AVRO_STRING && target == ::avro::AVRO_BYTES;
    require(source == target || (promoted && (!exact_value_types || identical_string_storage) &&
                                 from_logical.type() == ::avro::LogicalType::NONE),
            "Incompatible Avro reader physical type");
    if (source == ::avro::AVRO_RECORD) {
        validate_record_pair(from, to, match_record_names, depth, exact_value_types);
    } else if (source == ::avro::AVRO_ARRAY || source == ::avro::AVRO_MAP) {
        validate_schema_pair(from->leafAt(source == ::avro::AVRO_MAP ? 1 : 0),
                             to->leafAt(target == ::avro::AVRO_MAP ? 1 : 0), match_record_names,
                             depth + 1, exact_value_types);
    } else if (source == ::avro::AVRO_FIXED) {
        require(from->name() == to->name() && from->fixedSize() == to->fixedSize(),
                "Incompatible Avro fixed reader schema");
    } else if (source == ::avro::AVRO_ENUM) {
        require(from->name() == to->name(), "Incompatible Avro enum reader schema");
        for (size_t i = 0; i < from->names(); ++i) {
            size_t index = 0;
            require(to->nameIndex(from->nameAt(i), index), "Incompatible Avro enum symbols");
        }
    }
}

template <typename Cursor>
int64_t read_long(Cursor& cursor) {
    uint64_t value = 0;
    for (unsigned shift = 0; shift < 64; shift += 7) {
        const uint8_t byte = cursor.byte();
        // The tenth byte has only one available bit in a 64-bit Avro varint.
        require(shift != 63 || byte <= 1, "Avro varint overflows");
        value |= static_cast<uint64_t>(byte & 127) << shift;
        if ((byte & 128) == 0) {
            return static_cast<int64_t>(value >> 1) ^ -static_cast<int64_t>(value & 1);
        }
    }
    throw Exception(ErrorCode::INVALID_ARGUMENT, "Invalid Avro varint");
}

struct MemoryCursor {
    const uint8_t* data = nullptr;
    size_t length = 0;
    size_t position = 0;
    size_t remaining() const { return length - position; }
    uint8_t byte() {
        require(remaining() != 0, "Truncated Avro value");
        return data[position++];
    }
    void read(void* output, size_t size) {
        require(size <= remaining(), "Truncated Avro value");
        if (size != 0) {
            memcpy(output, data + position, size);
        }
        position += size;
    }
    void skip(size_t size) {
        require(size <= remaining(), "Truncated Avro value");
        position += size;
    }
};

class FileCursor {
public:
    FileCursor(io::FileReaderSPtr file, std::shared_ptr<io::IOContext> context)
            : _file(std::move(file)), _context(std::move(context)) {}
    size_t position() const { return _position; }
    size_t remaining() const { return _file->size() - _position; }
    size_t scratch_bytes() const { return _prefix_buffer.capacity(); }
    void coalesce_prefix(const AvroContainerReader::Reserve& reserve) {
        reserve(1024 * 1024);
        _prefix_buffer.resize(1024 * 1024);
        _buffer_size = 0;
    }
    void seek(size_t position) {
        require(position <= _file->size(), "Invalid Avro file offset");
        _position = position;
    }
    uint8_t byte() {
        uint8_t value;
        read(&value, 1);
        return value;
    }
    void read(void* output, size_t size) {
        require(size <= remaining(), "Truncated Avro container");
        auto* destination = static_cast<uint8_t*>(output);
        while (size != 0) {
            if (_context != nullptr && _context->should_stop) {
                throw Exception(ErrorCode::CANCELLED, "Avro scan cancelled");
            }
            if (_position < _buffer_start || _position >= _buffer_start + _buffer_size) {
                size_t read = 0;
                auto* buffer = _prefix_buffer.empty() ? _buffer.data() : _prefix_buffer.data();
                const auto capacity =
                        _prefix_buffer.empty() ? _buffer.size() : _prefix_buffer.size();
                auto status =
                        _file->read_at(_position, Slice(buffer, std::min(capacity, remaining())),
                                       &read, _context.get());
                if (!status.ok()) {
                    throw Exception(status);
                }
                require(read != 0, "Truncated Avro container");
                _buffer_start = _position;
                _buffer_size = read;
            }
            const size_t count = std::min(size, _buffer_start + _buffer_size - _position);
            const auto* buffer = _prefix_buffer.empty() ? _buffer.data() : _prefix_buffer.data();
            memcpy(destination, buffer + _position - _buffer_start, count);
            destination += count;
            size -= count;
            _position += count;
        }
    }

private:
    io::FileReaderSPtr _file;
    std::shared_ptr<io::IOContext> _context;
    std::array<char, 64 * 1024> _buffer;
    std::vector<char> _prefix_buffer;
    size_t _position = 0;
    size_t _buffer_start = 0;
    size_t _buffer_size = 0;
};

struct MetadataCursor {
    FileCursor& file;
    size_t end;
    size_t remaining() const { return end - file.position(); }
    uint8_t byte() {
        require(remaining() != 0, "Truncated Avro metadata block");
        return file.byte();
    }
    void read(void* output, size_t size) {
        require(size <= remaining(), "Truncated Avro metadata block");
        file.read(output, size);
    }
};

::avro::NodePtr resolved(const ::avro::NodePtr& node) {
    return node->type() == ::avro::AVRO_SYMBOLIC ? ::avro::resolveSymbol(node) : node;
}

} // namespace

struct AvroContainerReader::Impl {
    FileCursor file;
    Reserve reserve;
    Checkpoint checkpoint;
    Workspace workspace;
    RuntimeProfile::Counter* decompress_time = nullptr;
    ::avro::ValidSchema schema;
    ::avro::ValidSchema projected_schema;
    std::unordered_map<std::string, std::string> metadata;
    std::array<uint8_t, 16> sync;
    std::vector<uint8_t> compressed;
    std::vector<uint8_t> uncompressed;
    std::unordered_map<const ::avro::Node*, size_t> datum_sizes;
    std::unordered_map<size_t, size_t> selected;
    MemoryCursor data;
    size_t header_end = 0;
    int64_t remaining_rows = 0;
    int64_t total_rows = 0;
    size_t schema_charge = 0;
    bool loaded = false;

    Impl(io::FileReaderSPtr input, std::shared_ptr<io::IOContext> context, Reserve allocation,
         Checkpoint reset, Workspace codec_workspace, RuntimeProfile* profile,
         bool iceberg_compatible)
            : file(std::move(input), std::move(context)),
              reserve(std::move(allocation)),
              checkpoint(std::move(reset)),
              workspace(std::move(codec_workspace)) {
        if (profile != nullptr) {
            decompress_time = ADD_CHILD_TIMER_WITH_LEVEL(profile, "AvroDecompressTime",
                                                         "AvroDecodeAndMaterializeTime", 1);
        }
        std::array<uint8_t, 4> magic;
        file.read(magic.data(), magic.size());
        require(magic == std::array<uint8_t, 4> {'O', 'b', 'j', 1}, "Invalid Avro magic");
        for (int64_t count = read_long(file); count != 0; count = read_long(file)) {
            std::optional<size_t> block_end;
            if (count < 0) {
                require(count != std::numeric_limits<int64_t>::min(),
                        "Invalid Avro metadata count");
                count = -count;
                const auto bytes = read_long(file);
                require(bytes >= 0 && static_cast<uint64_t>(bytes) <= file.remaining(),
                        "Invalid Avro metadata block");
                block_end = file.position() + static_cast<size_t>(bytes);
            }
            // A negative map block cannot borrow bytes from its successor or the sync marker.
            MetadataCursor block_input {
                    .file = file, .end = block_end.value_or(file.position() + file.remaining())};
            growth(0, count, 128);
            require(static_cast<uint64_t>(count) <= block_input.remaining() / 2,
                    "Truncated Avro metadata");
            for (int64_t i = 0; i < count; ++i) {
                auto key = read_string(block_input);
                auto value = read_string(block_input);
                metadata.insert_or_assign(std::move(key), std::move(value));
            }
            require(!block_end || file.position() == *block_end,
                    "Avro metadata block size mismatch");
        }
        file.read(sync.data(), sync.size());
        header_end = file.position();
        const auto found = metadata.find("avro.schema");
        require(found != metadata.end(), "Avro schema metadata is missing");
        // Cover the preprocessing DOM/output and compiler graph before allocating any of them.
        growth(0, found->second.size(), 32);
        schema_charge = found->second.size() * 32;
        schema = compile_physical_schema(found->second, false, iceberg_compatible);
    }

    void growth(size_t capacity, size_t count, size_t item_size = 1) const {
        require(count <= std::numeric_limits<int64_t>::max() / 2 / item_size,
                "Avro allocation size overflows");
        if (count > capacity) {
            reserve(std::max(count, capacity * 2) * item_size);
        }
    }

    void grow_buffer(std::vector<uint8_t>& buffer, size_t size) const {
        const auto maximum =
                std::min(buffer.max_size(), size_t(std::numeric_limits<int64_t>::max()) / 2);
        require(size <= maximum, "Avro allocation size overflows");
        if (size > buffer.capacity()) {
            // Chunked output must not retain every superseded capacity (64+128+... KiB).
            // Admit only the delta; a 2->4 MiB reallocation still needs a scoped 2 MiB
            // lease so its actual 6 MiB old/new overlap is checked before allocating.
            const auto target = std::max(
                    size,
                    buffer.capacity() + std::min(buffer.capacity(), maximum - buffer.capacity()));
            auto old_buffer = buffer.capacity() == 0 ? nullptr : workspace(buffer.capacity());
            reserve(target - buffer.capacity());
            buffer.reserve(target);
        }
    }

    template <typename Cursor>
    std::string read_string(Cursor& input) {
        const auto length = read_long(input);
        require(length >= 0, "Invalid Avro string length");
        // Check the current block's boundary before admitting or allocating the string.
        require(static_cast<uint64_t>(length) <= input.remaining(), "Truncated Avro string");
        growth(0, length);
        std::string value(static_cast<size_t>(length), '\0');
        input.read(value.data(), value.size());
        return value;
    }

    size_t datum_size(const ::avro::NodePtr& input) {
        const auto node = resolved(input);
        if (const auto found = datum_sizes.find(node.get()); found != datum_sizes.end()) {
            return found->second;
        }
        size_t size = 0;
        switch (node->type()) {
        case ::avro::AVRO_STRING:
            size = sizeof(std::string);
            break;
        case ::avro::AVRO_BYTES:
            size = sizeof(std::vector<uint8_t>);
            break;
        case ::avro::AVRO_FIXED:
            size = sizeof(::avro::GenericFixed) + node->fixedSize();
            break;
        case ::avro::AVRO_ARRAY:
            size = sizeof(::avro::GenericArray);
            break;
        case ::avro::AVRO_MAP:
            size = sizeof(::avro::GenericMap);
            break;
        case ::avro::AVRO_ENUM:
            size = sizeof(::avro::GenericEnum);
            break;
        case ::avro::AVRO_UNION: {
            // Estimate the canonical null-first skeleton even when this is a writer node used
            // to admit a nested branch. An unused large FIXED alternative must not consume budget.
            const size_t first =
                    node->leaves() == 2 && node->leafAt(1)->type() == ::avro::AVRO_NULL ? 1 : 0;
            size = sizeof(::avro::GenericUnion) + datum_size(node->leafAt(first));
            break;
        }
        case ::avro::AVRO_RECORD:
            size = sizeof(::avro::GenericRecord) + node->leaves() * sizeof(::avro::GenericDatum);
            for (size_t i = 0; i < node->leaves(); ++i) {
                const auto child = datum_size(node->leafAt(i));
                require(child <= std::numeric_limits<int64_t>::max() - size,
                        "Avro datum size overflows");
                size += child;
            }
            break;
        default:
            break;
        }
        datum_sizes.emplace(node.get(), size);
        return size;
    }

    bool zero_width(const ::avro::NodePtr& input) {
        const auto node = resolved(input);
        if (node->type() == ::avro::AVRO_NULL) {
            return true;
        }
        if (node->type() == ::avro::AVRO_FIXED) {
            return node->fixedSize() == 0;
        }
        if (node->type() != ::avro::AVRO_RECORD) {
            return false;
        }
        for (size_t i = 0; i < node->leaves(); ++i) {
            if (!zero_width(node->leafAt(i))) {
                return false;
            }
        }
        return true;
    }

    int64_t block_header(size_t* payload_end) {
        const int64_t count = read_long(file);
        const int64_t bytes = read_long(file);
        require(count >= 0 && bytes >= 0 && file.remaining() >= sync.size() &&
                        static_cast<uint64_t>(bytes) <= file.remaining() - sync.size(),
                "Invalid Avro data block bounds");
        *payload_end = file.position() + bytes;
        return count;
    }

    void verify_sync() {
        std::array<uint8_t, 16> marker;
        file.read(marker.data(), marker.size());
        require(marker == sync, "Avro sync mismatch");
    }

    void decompress_snappy() {
        require(compressed.size() >= 4, "Truncated Avro Snappy block");
        const size_t length = compressed.size() - 4;
        size_t output_size = 0;
        require(snappy::GetUncompressedLength(reinterpret_cast<const char*>(compressed.data()),
                                              length, &output_size),
                "Invalid Avro Snappy block");
        grow_buffer(uncompressed, output_size);
        uncompressed.resize(output_size);
        require(snappy::RawUncompress(reinterpret_cast<const char*>(compressed.data()), length,
                                      reinterpret_cast<char*>(uncompressed.data())),
                "Avro Snappy decompression failed");
        uint32_t expected = 0;
        for (size_t i = length; i < compressed.size(); ++i) {
            expected = (expected << 8) | compressed[i];
        }
        auto actual = crc32(0, nullptr, 0);
        for (size_t offset = 0; offset < uncompressed.size();) {
            const auto size = std::min(size_t(64 * 1024), uncompressed.size() - offset);
            actual = crc32(actual, uncompressed.data() + offset, static_cast<uInt>(size));
            offset += size;
        }
        require(expected == actual, "Avro Snappy checksum mismatch");
    }

    void decompress_zstandard() {
        // Even a zero-byte Avro datum needs a real compressed frame when a block owns rows.
        require(!compressed.empty(), "Missing Avro Zstandard frame");
        size_t offset = 0;
        while (offset < compressed.size()) {
            const auto* frame = compressed.data() + offset;
            const auto frame_size = ZSTD_findFrameCompressedSize(frame, compressed.size() - offset);
            require(!ZSTD_isError(frame_size), "Invalid Avro Zstandard frame");
            const auto workspace_bytes = ZSTD_estimateDStreamSize_fromFrame(frame, frame_size);
            require(!ZSTD_isError(workspace_bytes), "Invalid Avro Zstandard window");
            auto workspace_lease = workspace(workspace_bytes);
            std::unique_ptr<ZSTD_DStream, decltype(&ZSTD_freeDStream)> stream(ZSTD_createDStream(),
                                                                              &ZSTD_freeDStream);
            require(stream != nullptr && !ZSTD_isError(ZSTD_initDStream(stream.get())),
                    "Cannot initialize Avro Zstandard decoder");
            ZSTD_inBuffer input {frame, frame_size, 0};
            std::array<uint8_t, 64 * 1024> chunk;
            size_t remaining = 1;
            while (input.pos < input.size || remaining != 0) {
                ZSTD_outBuffer output {chunk.data(), chunk.size(), 0};
                remaining = ZSTD_decompressStream(stream.get(), &output, &input);
                require(!ZSTD_isError(remaining), "Avro Zstandard decompression failed");
                append(chunk.data(), output.pos);
                require(input.pos != input.size || remaining == 0 || output.pos != 0,
                        "Truncated Avro Zstandard frame");
            }
            offset += frame_size;
        }
    }

    void decompress_deflate() {
        if (compressed.empty()) {
            // Avro C++ omits the deflate frame for zero-byte records, even with a positive count.
            // Check the writer schema: an empty COUNT projection cannot legitimize missing data.
            require(remaining_rows == 0 || zero_width(schema.root()),
                    "Truncated Avro deflate block");
            return;
        }
        auto workspace_lease = workspace(256 * 1024);
        z_stream stream {};
        require(inflateInit2(&stream, -15) == Z_OK, "Cannot initialize Avro deflate decoder");
        std::unique_ptr<z_stream, decltype(&inflateEnd)> cleanup(&stream, &inflateEnd);
        std::array<uint8_t, 64 * 1024> chunk;
        size_t offset = 0;
        int result = Z_OK;
        while (result != Z_STREAM_END) {
            if (stream.avail_in == 0) {
                const size_t length = std::min<size_t>(std::numeric_limits<uInt>::max(),
                                                       compressed.size() - offset);
                stream.next_in = compressed.data() + offset;
                stream.avail_in = static_cast<uInt>(length);
                offset += length;
            }
            stream.next_out = chunk.data();
            stream.avail_out = chunk.size();
            const auto before = stream.avail_in;
            result = inflate(&stream, Z_NO_FLUSH);
            require(result == Z_OK || result == Z_STREAM_END || result == Z_BUF_ERROR,
                    "Invalid Avro deflate block");
            const auto produced = chunk.size() - stream.avail_out;
            append(chunk.data(), produced);
            // zlib can consume the last input byte while a MATCH still has pending output.
            // Only completion or a no-progress iteration can terminate the drain.
            require(result == Z_STREAM_END || before != stream.avail_in || produced != 0,
                    "Truncated Avro deflate block");
        }
        require(result == Z_STREAM_END && stream.avail_in == 0 && offset == compressed.size(),
                "Truncated Avro deflate block");
    }

    void decompress() {
        SCOPED_TIMER(decompress_time);
        const auto found = metadata.find("avro.codec");
        const auto codec = found == metadata.end() ? "null" : found->second;
        uncompressed.clear();
        if (codec == "null") {
            compressed.swap(uncompressed);
        } else if (codec == "snappy") {
            decompress_snappy();
        } else if (codec == "zstandard") {
            decompress_zstandard();
        } else if (codec == "deflate") {
            decompress_deflate();
        } else {
            throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "Unsupported Avro codec: {}", codec);
        }
        data = {.data = uncompressed.data(), .length = uncompressed.size(), .position = 0};
    }

    void append(const uint8_t* bytes, size_t count) {
        require(count <= std::numeric_limits<size_t>::max() - uncompressed.size(),
                "Avro decompression size overflows");
        grow_buffer(uncompressed, uncompressed.size() + count);
        uncompressed.insert(uncompressed.end(), bytes, bytes + count);
    }

    // Decode only selected values; skipping a writer field never constructs GenericDatum trees.
    // NOLINTNEXTLINE(readability-function-size,readability-function-cognitive-complexity)
    void read_value(const ::avro::NodePtr& input, ::avro::GenericDatum* datum,
                    bool retained = true) {
        const auto node = resolved(input);
        switch (node->type()) {
        case ::avro::AVRO_NULL:
            break;
        case ::avro::AVRO_BOOL: {
            const auto value = data.byte();
            require(value <= 1, "Invalid Avro boolean");
            if (datum) {
                datum->value<bool>() = value;
            }
            break;
        }
        case ::avro::AVRO_INT: {
            const auto value = read_long(data);
            require(value >= std::numeric_limits<int32_t>::min() &&
                            value <= std::numeric_limits<int32_t>::max(),
                    "Avro int overflows");
            if (datum) {
                datum->value<int32_t>() = static_cast<int32_t>(value);
            }
            break;
        }
        case ::avro::AVRO_LONG: {
            const auto value = read_long(data);
            if (datum) {
                datum->value<int64_t>() = value;
            }
            break;
        }
        case ::avro::AVRO_FLOAT:
        case ::avro::AVRO_DOUBLE: {
            uint64_t bits = 0;
            const size_t length = node->type() == ::avro::AVRO_FLOAT ? 4 : 8;
            for (size_t i = 0; i < length; ++i) {
                bits |= uint64_t(data.byte()) << (8 * i);
            }
            if (datum) {
                if (length == 4) {
                    datum->value<float>() = std::bit_cast<float>(static_cast<uint32_t>(bits));
                } else {
                    datum->value<double>() = std::bit_cast<double>(bits);
                }
            }
            break;
        }
        case ::avro::AVRO_STRING:
        case ::avro::AVRO_BYTES:
        case ::avro::AVRO_FIXED: {
            const int64_t length = node->type() == ::avro::AVRO_FIXED
                                           ? static_cast<int64_t>(node->fixedSize())
                                           : read_long(data);
            require(length >= 0, "Invalid Avro value length");
            if (datum == nullptr) {
                data.skip(length);
            } else if (node->type() == ::avro::AVRO_STRING) {
                auto& value = datum->value<std::string>();
                growth(value.capacity(), length);
                require(static_cast<uint64_t>(length) <= data.remaining(), "Truncated Avro string");
                value.resize(length);
                data.read(value.data(), length);
            } else {
                auto& value = node->type() == ::avro::AVRO_FIXED
                                      ? datum->value<::avro::GenericFixed>().value()
                                      : datum->value<std::vector<uint8_t>>();
                growth(value.capacity(), length);
                require(static_cast<uint64_t>(length) <= data.remaining(), "Truncated Avro bytes");
                value.resize(length);
                data.read(value.data(), length);
            }
            break;
        }
        case ::avro::AVRO_ENUM: {
            const auto index = read_long(data);
            require(index >= 0 && static_cast<uint64_t>(index) < node->names(),
                    "Invalid Avro enum");
            if (datum) {
                datum->value<::avro::GenericEnum>().set(index);
            }
            break;
        }
        case ::avro::AVRO_UNION: {
            const auto index = read_long(data);
            require(index >= 0 && static_cast<uint64_t>(index) < node->leaves(),
                    "Invalid Avro union");
            // Projected datum unions are null-first; encoded indices always belong to the
            // writer schema, including for nested collection elements and named records.
            const auto datum_index =
                    node->leaves() == 2 && node->leafAt(1)->type() == ::avro::AVRO_NULL ? 1 - index
                                                                                        : index;
            if (datum && datum->unionBranch() != static_cast<size_t>(datum_index)) {
                const bool releases_old =
                        retained && node->leafAt(index)->type() == ::avro::AVRO_NULL;
                if (releases_old) {
                    checkpoint(*datum, true);
                }
                reserve(datum_size(node->leafAt(index)));
                datum->selectBranch(datum_index);
                if (releases_old) {
                    checkpoint(*datum, false);
                }
                retained = false;
            }
            read_value(node->leafAt(index), datum, retained);
            break;
        }
        case ::avro::AVRO_RECORD:
            for (size_t i = 0; i < node->leaves(); ++i) {
                read_value(node->leafAt(i),
                           datum ? &datum->value<::avro::GenericRecord>().fieldAt(i) : nullptr,
                           retained);
            }
            break;
        case ::avro::AVRO_ARRAY:
        case ::avro::AVRO_MAP: {
            const bool map = node->type() == ::avro::AVRO_MAP;
            const auto child = node->leafAt(map ? 1 : 0);
            auto projected_child = child;
            if (datum != nullptr) {
                projected_child = map ? datum->value<::avro::GenericMap>().schema()->leafAt(1)
                                      : datum->value<::avro::GenericArray>().schema()->leafAt(0);
            }
            if (datum) {
                checkpoint(*datum, true);
                if (map) {
                    datum->value<::avro::GenericMap>().value().clear();
                } else {
                    datum->value<::avro::GenericArray>().value().clear();
                }
                checkpoint(*datum, false);
            }
            for (int64_t count = read_long(data); count != 0; count = read_long(data)) {
                std::optional<size_t> block_end;
                if (count < 0) {
                    require(count != std::numeric_limits<int64_t>::min(),
                            "Invalid Avro collection count");
                    count = -count;
                    const auto bytes = read_long(data);
                    require(bytes >= 0 && static_cast<uint64_t>(bytes) <= data.remaining(),
                            "Invalid Avro collection block size");
                    block_end = data.position + bytes;
                    if (datum == nullptr) {
                        data.skip(bytes);
                        continue;
                    }
                }
                // A positive block of nulls has no payload bytes. Skip its cardinality in one
                // step rather than looping over a potentially enormous unprojected collection.
                if (datum == nullptr && !map && zero_width(child)) {
                    continue;
                }
                if (datum == nullptr) {
                    require(static_cast<uint64_t>(count) <= data.remaining(),
                            "Truncated Avro collection");
                }
                if (datum) {
                    growth(0, count, std::max(size_t(1), datum_size(projected_child)));
                    auto admit = [&](auto& values) {
                        using Value =
                                typename std::remove_reference_t<decltype(values)>::value_type;
                        static_assert(std::is_nothrow_move_constructible_v<Value>);
                        const auto maximum = std::min(
                                values.max_size(),
                                size_t(std::numeric_limits<int64_t>::max()) / 2 / sizeof(Value));
                        require(static_cast<uint64_t>(count) <= maximum - values.size(),
                                "Avro collection count overflows");
                        const auto size = values.size() + count;
                        // Thousands of one-item blocks must not copy and admit the entire prefix
                        // on each append. Retain only the capacity delta; the temporary old-buffer
                        // lease covers overlap during reallocation and expires when it is freed.
                        if (size > values.capacity()) {
                            const auto capacity =
                                    values.capacity() +
                                    std::min(values.capacity(), maximum - values.capacity());
                            const auto target = std::max(size, capacity);
                            const auto old_bytes = values.capacity() * sizeof(Value);
                            auto old_buffer = old_bytes == 0 ? nullptr : workspace(old_bytes);
                            reserve((target - values.capacity()) * sizeof(Value));
                            values.reserve(target);
                        }
                    };
                    if (map) {
                        admit(datum->value<::avro::GenericMap>().value());
                    } else {
                        admit(datum->value<::avro::GenericArray>().value());
                    }
                }
                for (int64_t i = 0; i < count; ++i) {
                    if (map) {
                        if (datum) {
                            auto& values = datum->value<::avro::GenericMap>().value();
                            auto key = read_string(data);
                            values.emplace_back(std::move(key),
                                                ::avro::GenericDatum(projected_child));
                            read_value(child, &values.back().second, false);
                        } else {
                            const auto length = read_long(data);
                            require(length >= 0, "Invalid Avro map key length");
                            data.skip(length);
                            read_value(child, nullptr);
                        }
                    } else if (datum) {
                        auto& values = datum->value<::avro::GenericArray>().value();
                        values.emplace_back(projected_child);
                        read_value(child, &values.back(), false);
                    } else {
                        read_value(child, nullptr);
                    }
                }
                require(!block_end || *block_end == data.position,
                        "Avro collection block size mismatch");
            }
            break;
        }
        default:
            throw Exception(ErrorCode::INVALID_ARGUMENT, "Unsupported Avro value");
        }
    }
};

AvroContainerReader::AvroContainerReader(io::FileReaderSPtr file,
                                         std::shared_ptr<io::IOContext> context, Reserve reserve,
                                         Checkpoint checkpoint, Workspace workspace,
                                         RuntimeProfile* profile, bool iceberg_compatible)
        : _impl(std::make_unique<Impl>(std::move(file), std::move(context), std::move(reserve),
                                       std::move(checkpoint), std::move(workspace), profile,
                                       iceberg_compatible)) {}
AvroContainerReader::~AvroContainerReader() = default;

const ::avro::ValidSchema& AvroContainerReader::dataSchema() const {
    return _impl->schema;
}

void AvroContainerReader::validate_read_schema(const std::string& json, bool match_record_names,
                                               const std::string& projected_schema) const {
    const auto limit = (std::numeric_limits<size_t>::max() - _impl->schema_charge) / 32;
    require(json.size() <= limit && projected_schema.size() <= limit - json.size(),
            "Avro reader schema allocation overflows");
    // Hold admission through compilation AND comparison, including temporary field-name indexes.
    auto workspace =
            _impl->workspace((json.size() + projected_schema.size()) * 32 + _impl->schema_charge);
    // Inspect the original writer JSON before the C++ compiler drops unknown Hive annotations.
    (void)physical_schema_document(_impl->metadata.at("avro.schema"), true);
    const auto reader_schema = compile_physical_schema(json, true);
    validate_schema_pair(_impl->schema.root(), reader_schema.root(), match_record_names);
    if (!projected_schema.empty()) {
        const auto projected = compile_physical_schema(projected_schema, true);
        // Skipping a reader INT->FLOAT promotion before a DOUBLE output would skip rounding.
        // Every projected field must exist with the same value interpretation in the reader.
        validate_schema_pair(reader_schema.root(), projected.root(), false, 0, true);
    }
}

std::optional<std::string> AvroContainerReader::metadata(const std::string& key) const {
    const auto found = _impl->metadata.find(key);
    return found == _impl->metadata.end() ? std::nullopt : std::optional(found->second);
}

size_t AvroContainerReader::scratchBytes() const {
    size_t bytes = sizeof(Impl) + _impl->compressed.capacity() + _impl->uncompressed.capacity() +
                   _impl->schema_charge + _impl->datum_sizes.size() * 64 +
                   _impl->file.scratch_bytes();
    for (const auto& [key, value] : _impl->metadata) {
        bytes += 128 + key.capacity() + value.capacity();
    }
    return bytes;
}

size_t AvroContainerReader::position() const {
    return _impl->file.position();
}

int64_t AvroContainerReader::seek_range(int64_t start) {
    auto& s = *_impl;
    s.file.seek(s.header_end);
    int64_t rows = 0;
    if (std::cmp_greater(start, s.header_end)) {
        // Coalesce prefix headers so remote scans do not issue one GET per small OCF block.
        s.file.coalesce_prefix(s.reserve);
    }
    // A bytes value can contain <sync><valid header><valid datum><sync>. Only block lengths
    // followed from the container header prove boundaries, even without absolute row ids.
    while (s.file.position() < static_cast<uint64_t>(start) && s.file.remaining() != 0) {
        size_t end;
        const auto count = s.block_header(&end);
        require(count <= std::numeric_limits<int64_t>::max() - rows, "Avro row count overflows");
        rows += count;
        s.file.seek(end);
        s.verify_sync();
    }
    s.total_rows = rows;
    return rows;
}

void AvroContainerReader::init(const ::avro::ValidSchema& schema, int64_t offset) {
    auto& s = *_impl;
    // The selected schema contains whole roots in writer order; omitted roots are skipped in place.
    const auto source = s.schema.root();
    const auto target = schema.root();
    size_t ordinal = 0;
    for (size_t i = 0; i < source->leaves(); ++i) {
        if (ordinal < target->leaves() && source->nameAt(i) == target->nameAt(ordinal)) {
            s.selected.emplace(i, ordinal++);
        }
    }
    s.projected_schema = schema;
    s.file.seek(offset);
    size_t end;
    s.block_header(&end);
    s.file.seek(offset);
}

bool AvroContainerReader::hasMoreBefore(int64_t end) {
    auto& s = *_impl;
    while (s.remaining_rows == 0) {
        require(!s.loaded || s.data.remaining() == 0, "Avro block has trailing values");
        if (s.file.remaining() == 0 || s.file.position() >= static_cast<uint64_t>(end)) {
            return false;
        }
        size_t payload_end;
        s.remaining_rows = s.block_header(&payload_end);
        require(s.remaining_rows <= std::numeric_limits<int64_t>::max() - s.total_rows,
                "Avro row count overflows");
        s.total_rows += s.remaining_rows;
        const size_t bytes = payload_end - s.file.position();
        s.grow_buffer(s.compressed, bytes);
        s.compressed.resize(bytes);
        s.file.read(s.compressed.data(), bytes);
        s.verify_sync();
        s.decompress();
        s.loaded = true;
    }
    return true;
}

std::unique_ptr<::avro::GenericDatum> AvroContainerReader::make_datum() {
    auto& s = *_impl;
    // A zero-row block needs no value tree, even when the schema contains a huge nonnullable FIXED.
    s.reserve(sizeof(::avro::GenericDatum) + s.datum_size(s.projected_schema.root()));
    return std::make_unique<::avro::GenericDatum>(s.projected_schema);
}

void AvroContainerReader::read(::avro::GenericDatum& datum) {
    auto& s = *_impl;
    const auto root = s.schema.root();
    auto& record = datum.value<::avro::GenericRecord>();
    for (size_t i = 0; i < root->leaves(); ++i) {
        const auto found = s.selected.find(i);
        s.read_value(root->leafAt(i),
                     found == s.selected.end() ? nullptr : &record.fieldAt(found->second));
    }
    --s.remaining_rows;
}

} // namespace doris::format::avro
