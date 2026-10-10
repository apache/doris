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

// Independent fixture/oracle: uses vanilla ORC APIs, never Doris FILE code.
#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <orc/OrcFile.hh>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace {
void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

// Null, integer, bytes, or ordered children. MAP children are (key, value) pairs.
struct Value {
    using Children = std::vector<Value>;
    std::variant<std::monostate, int64_t, std::string, Children> data;

    Value() = default;
    explicit Value(int64_t value) : data(value) {}
    explicit Value(std::string value) : data(std::move(value)) {}
    explicit Value(Children value) : data(std::move(value)) {}
    bool is_null() const { return std::holds_alternative<std::monostate>(data); }
    int64_t integer() const { return std::get<int64_t>(data); }
    const std::string& bytes() const { return std::get<std::string>(data); }
    const Children& children() const { return std::get<Children>(data); }
    Children& children() { return std::get<Children>(data); }
};

Value values(std::initializer_list<Value> children) {
    return Value(Value::Children(children));
}

bool is_file(const orc::Type& type) {
    return type.hasAttributeKey("doris.struct-type") &&
           type.getAttributeValue("doris.struct-type") == "FILE";
}

const std::vector<int> ALL_FILE_FIELDS = {0, 1, 2, 3, 4, 5};

// Keep uri and canonical order. Interior omissions expose accidental positional mapping.
const std::map<std::string, std::vector<int>> SPARSE_FIELDS = {{"uri_only", {0}},
                                                               {"middle", {0, 2, 4, 5}},
                                                               {"inline", {0, 5}},
                                                               {"no_inline", {0, 1, 2, 3}},
                                                               {"missing_size", {0, 1, 5}}};
const std::vector<std::string> SPARSE_VALID_CASES = {"uri_only", "middle", "inline", "no_inline"};

std::unique_ptr<orc::Type> file_type(bool markers,
                                     const std::vector<int>& selected = ALL_FILE_FIELDS) {
    const std::vector<std::pair<std::string, orc::TypeKind>> canonical = {
            {"uri", orc::STRING},          {"offset", orc::LONG},     {"size", orc::LONG},
            {"content_type", orc::STRING}, {"checksum", orc::STRING}, {"inline", orc::BINARY}};
    auto type = orc::createStructType();
    for (int index : selected) {
        const auto& [name, kind] = canonical[index];
        type->addStructField(name, orc::createPrimitiveType(kind));
    }
    if (markers) {
        type->setAttribute("doris.struct-type", "FILE");
    }
    return type;
}

std::unique_ptr<orc::Type> fixture_schema(bool markers = true,
                                          const std::vector<int>& selected = ALL_FILE_FIELDS) {
    auto row = orc::createStructType();
    row->addStructField("id", orc::createPrimitiveType(orc::INT));
    row->addStructField("f", file_type(markers, selected));
    row->addStructField("files", orc::createListType(file_type(markers, selected)));
    auto holder = orc::createStructType();
    holder->addStructField("asset", file_type(markers, selected));
    holder->addStructField("attachments", orc::createListType(file_type(markers, selected)));
    row->addStructField("holder", std::move(holder));
    row->addStructField("lookup", orc::createMapType(orc::createPrimitiveType(orc::STRING),
                                                     file_type(markers, selected)));
    return row;
}

std::vector<Value> fixture_rows() {
    std::string payload;
    std::string alternate_payload;
    for (int i = 0; i < 4097; ++i) {
        payload.push_back(static_cast<char>(i % 256));
        alternate_payload.push_back(static_cast<char>(255 - i % 256));
    }
    // URI spelling is deliberately preserved, including escapes and dot segments.
    const auto blob = values({Value("s3://fixture/a/../b%2Fc%2fd%20e%252F?versionId=AbC%2bD"),
                              Value(7), Value(4097), Value("application/octet-stream"),
                              Value("ETAG:opaque-2"), Value(payload)});
    // Identical public metadata must not conceal substituting another value's bytes.
    auto alternate_blob = blob;
    alternate_blob.children()[5] = Value(alternate_payload);
    const auto empty = values({Value("urn:doris:file-fixture:empty"), Value(0), Value(0),
                               Value("application/octet-stream"), Value(), Value("")});
    const auto external = values(
            {Value("s3://fixture/external%252Fraw"), Value(), Value(), Value(), Value(), Value()});
    // Both exceed UINT32_MAX; their sum (12,884,901,912) fits in signed BIGINT.
    constexpr int64_t range_offset = 4'294'967'303;
    constexpr int64_t range_size = 8'589'934'609;
    const auto ranged = values({Value("hdfs://fixture:8020/range%20raw"), Value(range_offset),
                                Value(range_size), Value("text/plain"), Value("CRC32:0123abcd"),
                                Value(alternate_payload)});
    const auto nil = Value();
    const auto no_items = Value(Value::Children {});
    return {
            values({Value(1), blob, values({alternate_blob, empty, external, nil}),
                    values({blob, values({empty, nil})}),
                    values({values({Value("blob"), blob}), values({Value("empty"), empty}),
                            values({Value("external"), external}), values({Value("null"), nil})})}),
            values({Value(2), empty, no_items, values({empty, no_items}), no_items}),
            values({Value(3), external, values({external}), values({nil, values({external})}),
                    values({values({Value("external"), external})})}),
            values({Value(4), nil, nil, nil, nil}),
            values({Value(5), nil, values({nil}), values({nil, nil}),
                    values({values({Value("null"), nil})})}),
            values({Value(6), ranged, values({ranged, blob}), values({ranged, values({blob})}),
                    values({values({Value("range"), ranged}), values({Value("blob"), blob})})}),
    };
}

// Project fixture values physically for sparse ORC, or leave six children with NULL gaps
// for the independent post-read oracle. Ancestor NULLs are preserved in either form.
Value sparse_value(const Value& value, const orc::Type& field, const std::vector<int>& selected,
                   bool compact) {
    if (value.is_null()) {
        return value;
    }
    if (is_file(field)) {
        Value::Children children(compact ? 0 : 6);
        for (int index : selected) {
            if (compact) {
                children.push_back(value.children()[index]);
            } else {
                children[index] = value.children()[index];
            }
        }
        return Value(std::move(children));
    }
    auto result = value;
    if (field.getKind() == orc::STRUCT) {
        for (size_t i = 0; i < value.children().size(); ++i) {
            result.children()[i] =
                    sparse_value(value.children()[i], *field.getSubtype(i), selected, compact);
        }
    } else if (field.getKind() == orc::LIST) {
        for (auto& child : result.children()) {
            child = sparse_value(child, *field.getSubtype(0), selected, compact);
        }
    } else if (field.getKind() == orc::MAP) {
        for (auto& pair : result.children()) {
            pair.children()[1] =
                    sparse_value(pair.children()[1], *field.getSubtype(1), selected, compact);
        }
    }
    return result;
}

std::vector<Value> sparse_rows(const std::string& name, bool compact) {
    auto rows = fixture_rows();
    for (auto& row : rows) {
        row = sparse_value(row, *fixture_schema(), SPARSE_FIELDS.at(name), compact);
    }
    return rows;
}

void fill_orc(orc::ColumnVectorBatch& batch, const orc::Type& type,
              const std::vector<Value>& rows) {
    batch.resize(rows.size());
    batch.numElements = rows.size();
    batch.hasNulls = true;
    for (size_t i = 0; i < rows.size(); ++i) {
        batch.notNull[i] = !rows[i].is_null();
    }
    switch (type.getKind()) {
    case orc::INT:
    case orc::LONG: {
        auto& integers = static_cast<orc::LongVectorBatch&>(batch);
        for (size_t i = 0; i < rows.size(); ++i) {
            integers.data[i] = rows[i].is_null() ? 0 : rows[i].integer();
        }
        break;
    }
    case orc::STRING:
    case orc::BINARY: {
        auto& bytes = static_cast<orc::StringVectorBatch&>(batch);
        uint64_t length = 0;
        for (const auto& row : rows) {
            length += row.is_null() ? 0 : row.bytes().size();
        }
        // Own the bytes in the batch; flattened child vectors below are temporary.
        bytes.blob.resize(length + 1);
        uint64_t offset = 0;
        for (size_t i = 0; i < rows.size(); ++i) {
            bytes.length[i] = rows[i].is_null() ? 0 : rows[i].bytes().size();
            bytes.data[i] = bytes.blob.data() + offset;
            if (!rows[i].is_null()) {
                std::copy(rows[i].bytes().begin(), rows[i].bytes().end(), bytes.data[i]);
            }
            offset += bytes.length[i];
        }
        break;
    }
    case orc::STRUCT: {
        auto& object = static_cast<orc::StructVectorBatch&>(batch);
        for (uint64_t child = 0; child < type.getSubtypeCount(); ++child) {
            std::vector<Value> flattened;
            for (const auto& row : rows) {
                flattened.push_back(row.is_null() ? Value() : row.children()[child]);
            }
            fill_orc(*object.fields[child], *type.getSubtype(child), flattened);
        }
        break;
    }
    case orc::LIST: {
        auto& list = static_cast<orc::ListVectorBatch&>(batch);
        std::vector<Value> flattened;
        list.offsets[0] = 0;
        for (size_t i = 0; i < rows.size(); ++i) {
            if (!rows[i].is_null()) {
                flattened.insert(flattened.end(), rows[i].children().begin(),
                                 rows[i].children().end());
            }
            list.offsets[i + 1] = flattened.size();
        }
        fill_orc(*list.elements, *type.getSubtype(0), flattened);
        break;
    }
    case orc::MAP: {
        auto& map = static_cast<orc::MapVectorBatch&>(batch);
        std::vector<Value> keys;
        std::vector<Value> items;
        map.offsets[0] = 0;
        for (size_t i = 0; i < rows.size(); ++i) {
            if (!rows[i].is_null()) {
                for (const auto& pair : rows[i].children()) {
                    keys.push_back(pair.children()[0]);
                    items.push_back(pair.children()[1]);
                }
            }
            map.offsets[i + 1] = keys.size();
        }
        fill_orc(*map.keys, *type.getSubtype(0), keys);
        fill_orc(*map.elements, *type.getSubtype(1), items);
        break;
    }
    default:
        throw std::runtime_error("Unexpected ORC batch type");
    }
}

void validate_orc_schema(const orc::Type& actual, const orc::Type& expected,
                         const std::string& path) {
    require(actual.getKind() == expected.getKind(), path + ": ORC kind");
    const std::string marker = "doris.struct-type";
    require(actual.hasAttributeKey(marker) == expected.hasAttributeKey(marker),
            path + ": ORC FILE marker");
    if (expected.hasAttributeKey(marker)) {
        require(actual.getAttributeValue(marker) == "FILE", path + ": ORC FILE marker value");
    }
    require(actual.getSubtypeCount() == expected.getSubtypeCount(), path + ": ORC child count");
    for (uint64_t i = 0; i < expected.getSubtypeCount(); ++i) {
        auto child_path = path + "." + std::to_string(i);
        if (expected.getKind() == orc::STRUCT) {
            require(actual.getFieldName(i) == expected.getFieldName(i),
                    child_path + ": ORC child name");
            child_path = path + "." + expected.getFieldName(i);
        }
        validate_orc_schema(*actual.getSubtype(i), *expected.getSubtype(i), child_path);
    }
}

Value decode_orc(const orc::ColumnVectorBatch& batch, const orc::Type& type, uint64_t row) {
    if (batch.hasNulls && !batch.notNull[row]) {
        return {};
    }
    switch (type.getKind()) {
    case orc::INT:
    case orc::LONG:
        return Value(static_cast<const orc::LongVectorBatch&>(batch).data[row]);
    case orc::STRING:
    case orc::BINARY: {
        const auto& bytes = static_cast<const orc::StringVectorBatch&>(batch);
        return Value(bytes.length[row] == 0 ? std::string()
                                            : std::string(bytes.data[row], bytes.length[row]));
    }
    case orc::STRUCT: {
        const auto& object = static_cast<const orc::StructVectorBatch&>(batch);
        Value::Children children;
        for (uint64_t i = 0; i < type.getSubtypeCount(); ++i) {
            children.push_back(decode_orc(*object.fields[i], *type.getSubtype(i), row));
        }
        return Value(std::move(children));
    }
    case orc::LIST: {
        const auto& list = static_cast<const orc::ListVectorBatch&>(batch);
        Value::Children children;
        for (int64_t i = list.offsets[row]; i < list.offsets[row + 1]; ++i) {
            children.push_back(decode_orc(*list.elements, *type.getSubtype(0), i));
        }
        return Value(std::move(children));
    }
    case orc::MAP: {
        const auto& map = static_cast<const orc::MapVectorBatch&>(batch);
        Value::Children children;
        for (int64_t i = map.offsets[row]; i < map.offsets[row + 1]; ++i) {
            children.push_back(values({decode_orc(*map.keys, *type.getSubtype(0), i),
                                       decode_orc(*map.elements, *type.getSubtype(1), i)}));
        }
        return Value(std::move(children));
    }
    default:
        throw std::runtime_error("Unexpected ORC decoded type");
    }
}

void write_orc(const std::string& path, const orc::Type& schema, const std::vector<Value>& rows) {
    auto output = orc::writeLocalFile(path);
    orc::WriterOptions options;
    options.setCompression(orc::CompressionKind_NONE);
    auto writer = orc::createWriter(schema, output.get(), options);
    auto batch = writer->createRowBatch(rows.size());
    fill_orc(*batch, schema, rows);
    writer->add(*batch);
    writer->close();
    output->close();
}

// Compare decoded primitive fields, including binary length/content and validity.
// There is no Doris FILE equality expression or public five-field cast in this oracle.
void compare_value(const Value& actual, const Value& expected, const orc::Type& field,
                   const std::string& path, bool expect_null_inline) {
    require(actual.data.index() == expected.data.index(), path + ": NULL/type differs");
    if (expected.is_null()) {
        return;
    }
    switch (field.getKind()) {
    case orc::INT:
    case orc::LONG:
        require(actual.integer() == expected.integer(), path + ": integer differs");
        return;
    case orc::STRING:
    case orc::BINARY:
        require(actual.bytes() == expected.bytes(), path + ": bytes differ");
        return;
    default:
        break;
    }
    const auto& a = actual.children();
    const auto& e = expected.children();
    require(a.size() == e.size(), path + ": child count differs");
    if (field.getKind() == orc::MAP) {
        std::map<std::string, const Value*> entries;
        for (const auto& pair : a) {
            require(pair.children().size() == 2, path + ": invalid MAP entry");
            require(entries.emplace(pair.children()[0].bytes(), &pair.children()[1]).second,
                    path + ": duplicate MAP key");
        }
        for (const auto& pair : e) {
            const auto& key = pair.children()[0].bytes();
            require(entries.contains(key), path + ": missing MAP key " + key);
            compare_value(*entries.at(key), pair.children()[1], *field.getSubtype(1),
                          path + "[" + key + "]", expect_null_inline);
        }
    } else {
        for (size_t i = 0; i < e.size(); ++i) {
            const auto& child = *field.getSubtype(field.getKind() == orc::LIST ? 0 : i);
            const auto name = field.getKind() == orc::STRUCT ? field.getFieldName(i) : "element";
            const auto child_path = path + "." + name + "[" + std::to_string(i) + "]";
            if (expect_null_inline && is_file(field) && name == "inline") {
                require(a[i].is_null(), child_path + ": expected NULL inline after JSON re-import");
            } else {
                compare_value(a[i], e[i], child, child_path, expect_null_inline);
            }
        }
    }
}

void verify_rows(const std::vector<std::string>& paths, const orc::Type& schema,
                 const std::vector<Value>& expected, bool expect_null_inline = false) {
    std::set<int64_t> seen;
    auto consume = [&](const Value& row) {
        require(!row.is_null() && row.children().size() == 5, "Invalid row shape");
        const auto id = row.children()[0].integer();
        require(id >= 1 && id <= static_cast<int64_t>(expected.size()), "Unexpected row id");
        require(seen.insert(id).second, "Duplicate row id " + std::to_string(id));
        compare_value(row, expected[id - 1], schema, "row[" + std::to_string(id) + "]",
                      expect_null_inline);
    };
    for (const auto& path : paths) {
        auto reader = orc::createReader(orc::readLocalFile(path), orc::ReaderOptions());
        validate_orc_schema(reader->getType(), schema, "row");
        auto rows = reader->createRowReader();
        auto batch = rows->createRowBatch(2);
        while (rows->next(*batch)) {
            for (uint64_t i = 0; i < batch->numElements; ++i) {
                consume(decode_orc(*batch, reader->getType(), i));
            }
        }
    }
    require(seen.size() == expected.size(), "Missing rows: expected six distinct ids");
}

void verify(const std::string& format, const std::vector<std::string>& paths,
            bool expect_null_inline = false) {
    require(format == "orc", "Expected orc");
    verify_rows(paths, *fixture_schema(), fixture_rows(), expect_null_inline);
}

void verify_sparse(const std::string& name, const std::vector<std::string>& paths) {
    verify_rows(paths, *fixture_schema(), sparse_rows(name, false));
}

void generate_sparse(const std::filesystem::path& directory) {
    std::filesystem::create_directories(directory);
    // A schema-only error must become a query error even when there are no rows to decode.
    const auto bad_schema_path = (directory / "sparse_invalid_uri_type.orc").string();
    auto bad_schema = orc::createStructType();
    auto bad_file = orc::createStructType();
    bad_file->addStructField("uri", orc::createPrimitiveType(orc::LONG));
    bad_file->setAttribute("doris.struct-type", "FILE");
    bad_schema->addStructField("f", std::move(bad_file));
    auto output = orc::writeLocalFile(bad_schema_path);
    auto writer = orc::createWriter(*bad_schema, output.get(), orc::WriterOptions());
    writer->close();
    output->close();
    auto reader = orc::createReader(orc::readLocalFile(bad_schema_path), orc::ReaderOptions());
    validate_orc_schema(reader->getType(), *bad_schema, "row");
    require(reader->getNumberOfRows() == 0, "Invalid URI type fixture must contain no rows");
    for (const auto& name : SPARSE_VALID_CASES) {
        const auto path = (directory / ("sparse_" + name + ".orc")).string();
        const auto schema = fixture_schema(true, SPARSE_FIELDS.at(name));
        const auto rows = sparse_rows(name, true);
        write_orc(path, *schema, rows);
        verify_rows({path}, *schema, rows);
    }
    // All schemas retain uri. Bad values are live (not hidden beneath a NULL FILE).
    for (const std::string location : {"top", "nested"}) {
        for (const std::string mutation : {"null_uri", "empty_uri", "negative_size",
                                           "negative_offset", "missing_size", "overflow"}) {
            const std::string name = mutation == "missing_size" ? "missing_size"
                                     : mutation == "negative_offset" || mutation == "overflow"
                                             ? "no_inline"
                                             : "middle";
            auto rows = fixture_rows();
            auto& file =
                    location == "top" ? rows[0].children()[1] : rows[0].children()[3].children()[0];
            if (mutation == "null_uri") {
                file.children()[0] = Value();
            } else if (mutation == "empty_uri") {
                file.children()[0] = Value("");
            } else if (mutation == "negative_size") {
                file.children()[2] = Value(-1);
            } else if (mutation == "negative_offset") {
                file.children()[1] = Value(-1);
            } else if (mutation == "overflow") {
                file.children()[1] = Value(INT64_MAX);
            }
            // Only the target FILE is invalid when size is absent from the schema.
            if (mutation == "missing_size") {
                for (auto& row : rows) {
                    row = sparse_value(row, *fixture_schema(), {0, 2, 3, 4, 5}, false);
                }
                auto& target = location == "top" ? rows[0].children()[1]
                                                 : rows[0].children()[3].children()[0];
                target.children()[1] = Value(7);
            }
            for (auto& row : rows) {
                row = sparse_value(row, *fixture_schema(), SPARSE_FIELDS.at(name), true);
            }
            const auto schema = fixture_schema(true, SPARSE_FIELDS.at(name));
            const auto path =
                    (directory / ("sparse_invalid_" + location + "_" + mutation + ".orc")).string();
            write_orc(path, *schema, rows);
            // Readability is independent of Doris's validation of FILE values.
            verify_rows({path}, *schema, rows);
        }
    }
}

void sparse_self_test(const std::filesystem::path& directory) {
    generate_sparse(directory);
    for (const auto& name : SPARSE_VALID_CASES) {
        for (const std::string mutation :
             {"good", "scalar_inline", "array_inline", "struct_inline", "attachment_inline",
              "map_inline", "missing_optional", "ancestor_null", "missing_row"}) {
            auto rows = sparse_rows(name, false);
            auto& columns = rows[0].children();
            if (mutation == "scalar_inline") {
                columns[1].children()[5] = Value("wrong inline");
            } else if (mutation == "array_inline") {
                columns[2].children()[0].children()[5] = Value("wrong inline");
            } else if (mutation == "struct_inline") {
                columns[3].children()[0].children()[5] = Value("wrong inline");
            } else if (mutation == "attachment_inline") {
                columns[3].children()[1].children()[0].children()[5] = Value("wrong inline");
            } else if (mutation == "map_inline") {
                columns[4].children()[0].children()[1].children()[5] = Value("wrong inline");
            } else if (mutation == "missing_optional") {
                const auto& selected = SPARSE_FIELDS.at(name);
                const auto missing = *std::find_if(
                        ALL_FILE_FIELDS.begin(), ALL_FILE_FIELDS.end(), [&](int index) {
                            return std::find(selected.begin(), selected.end(), index) ==
                                   selected.end();
                        });
                columns[1].children()[missing] =
                        fixture_rows()[0].children()[1].children()[missing];
            } else if (mutation == "ancestor_null") {
                rows[3].children()[3] = values({Value(), Value()});
            } else if (mutation == "missing_row") {
                rows.pop_back();
            }
            const auto path = (directory / (name + "_" + mutation + ".orc")).string();
            write_orc(path, *fixture_schema(), rows);
            bool rejected = false;
            try {
                verify_sparse(name, {path});
            } catch (const std::exception& error) {
                rejected = true;
                std::cout << "sparse " << name << " " << mutation << ": " << error.what() << '\n';
            }
            require(rejected == (mutation != "good"),
                    "Sparse oracle self-test " + name + " " + mutation);
        }
    }
}

void self_test(const std::filesystem::path& directory) {
    std::filesystem::create_directories(directory);
    const std::string format = "orc";
    // Each negative fixture remains structurally readable by the upstream library.
    for (const std::string mutation :
         {"good", "bytes", "nested_bytes", "swap_inline", "swap_nested_inline",
          "swap_struct_inline", "swap_map_inline", "empty_to_null", "null_to_empty",
          "ancestor_null", "marker", "uri", "offset32", "nested_size32", "missing_row"}) {
        auto rows = fixture_rows();
        if (mutation == "bytes") {
            rows[0].children()[1].children()[5] = Value("wrong binary");
        } else if (mutation == "nested_bytes") {
            rows[0].children()[3].children()[1].children()[0].children()[5] =
                    Value("wrong nested binary");
        } else if (mutation == "swap_inline") {
            std::swap(rows[0].children()[1].children()[5], rows[5].children()[1].children()[5]);
        } else if (mutation == "swap_nested_inline") {
            std::swap(rows[0].children()[2].children()[0].children()[5],
                      rows[5].children()[2].children()[1].children()[5]);
        } else if (mutation == "swap_struct_inline") {
            std::swap(rows[5].children()[3].children()[0].children()[5],
                      rows[5].children()[3].children()[1].children()[0].children()[5]);
        } else if (mutation == "swap_map_inline") {
            std::swap(rows[5].children()[4].children()[0].children()[1].children()[5],
                      rows[5].children()[4].children()[1].children()[1].children()[5]);
        } else if (mutation == "empty_to_null") {
            rows[1].children()[1].children()[5] = Value();
        } else if (mutation == "null_to_empty") {
            rows[2].children()[1].children()[5] = Value("");
        } else if (mutation == "ancestor_null") {
            rows[3].children()[3] = values({Value(), Value()});
        } else if (mutation == "uri") {
            rows[0].children()[1].children()[0] = Value("s3://fixture/normalized");
        } else if (mutation == "offset32") {
            auto& offset = rows[5].children()[1].children()[1];
            offset = Value(static_cast<int64_t>(static_cast<uint32_t>(offset.integer())));
        } else if (mutation == "nested_size32") {
            auto& size = rows[5].children()[4].children()[0].children()[1].children()[2];
            size = Value(static_cast<int64_t>(static_cast<uint32_t>(size.integer())));
        } else if (mutation == "missing_row") {
            rows.pop_back();
        }
        const auto path = (directory / (mutation + "." + format)).string();
        write_orc(path, *fixture_schema(mutation != "marker"), rows);
        bool rejected = false;
        try {
            verify(format, {path});
        } catch (const std::exception& error) {
            rejected = true;
            std::cout << format << " " << mutation << ": " << error.what() << '\n';
        }
        require(rejected == (mutation != "good"), format + ": oracle self-test " + mutation);
    }
    auto rows = fixture_rows();
    // MAP order is immaterial; rows and split output files may also be reordered.
    for (auto& row : rows) {
        auto& map = row.children()[4];
        if (!map.is_null()) {
            std::reverse(map.children().begin(), map.children().end());
        }
    }
    const auto first = (directory / ("part1." + format)).string();
    const auto second = (directory / ("part2." + format)).string();
    write_orc(first, *fixture_schema(), std::vector<Value>(rows.begin(), rows.begin() + 3));
    write_orc(second, *fixture_schema(), std::vector<Value>(rows.begin() + 3, rows.end()));
    verify(format, {second, first});

    // JSON re-import loses inline at every FILE position, including empty bytes.
    auto json_rows = fixture_rows();
    auto clear_inline = [](Value& file) {
        if (!file.is_null()) {
            file.children()[5] = Value();
        }
    };
    for (auto& row : json_rows) {
        auto& columns = row.children();
        clear_inline(columns[1]);
        if (!columns[2].is_null()) {
            for (auto& file : columns[2].children()) {
                clear_inline(file);
            }
        }
        if (!columns[3].is_null()) {
            clear_inline(columns[3].children()[0]);
            auto& attachments = columns[3].children()[1];
            if (!attachments.is_null()) {
                for (auto& file : attachments.children()) {
                    clear_inline(file);
                }
            }
        }
        if (!columns[4].is_null()) {
            for (auto& pair : columns[4].children()) {
                clear_inline(pair.children()[1]);
            }
        }
    }
    for (const std::string mutation :
         {"good", "scalar_empty", "array_bytes", "struct_bytes", "attachment_bytes", "map_bytes",
          "uri", "ancestor_null", "missing_row"}) {
        auto restored = json_rows;
        auto& columns = restored[0].children();
        if (mutation == "scalar_empty") {
            restored[1].children()[1].children()[5] = Value("");
        } else if (mutation == "array_bytes") {
            columns[2].children()[0].children()[5] = Value("unexpected");
        } else if (mutation == "struct_bytes") {
            columns[3].children()[0].children()[5] = Value("unexpected");
        } else if (mutation == "attachment_bytes") {
            columns[3].children()[1].children()[0].children()[5] = Value("unexpected");
        } else if (mutation == "map_bytes") {
            columns[4].children()[0].children()[1].children()[5] = Value("unexpected");
        } else if (mutation == "uri") {
            columns[1].children()[0] = Value("s3://fixture/normalized");
        } else if (mutation == "ancestor_null") {
            restored[3].children()[3] = values({Value(), Value()});
        } else if (mutation == "missing_row") {
            restored.pop_back();
        }
        const auto path = (directory / ("json_" + mutation + "." + format)).string();
        write_orc(path, *fixture_schema(), restored);
        bool rejected = false;
        try {
            verify(format, {path}, true);
        } catch (const std::exception& error) {
            rejected = true;
            std::cout << format << " json_" << mutation << ": " << error.what() << '\n';
        }
        require(rejected == (mutation != "good"),
                format + ": NULL-inline oracle self-test " + mutation);
    }
    // Neither oracle mode may silently accept the other mode's valid file.
    for (const bool expect_null_inline : {false, true}) {
        const auto path =
                (directory / ((expect_null_inline ? "good." : "json_good.") + format)).string();
        bool rejected = false;
        try {
            verify(format, {path}, expect_null_inline);
        } catch (const std::exception& error) {
            rejected = true;
            std::cout << format << " wrong oracle mode: " << error.what() << '\n';
        }
        require(rejected, format + ": oracle modes must distinguish original and NULL inline");
    }
    sparse_self_test(directory / "sparse");
}
} // namespace

int main(int argc, char** argv) {
    try {
        require(argc >= 3,
                "Usage: file_type_fixture generate DIR | verify orc [--expect-null-inline] "
                "FILE... | generate-sparse DIR | verify-sparse CASE FILE... | self-test DIR");
        const std::string command = argv[1];
        if (command == "generate") {
            require(argc == 3, "generate requires one directory");
            const std::filesystem::path directory(argv[2]);
            std::filesystem::create_directories(directory);
            const auto path = (directory / "canonical.orc").string();
            write_orc(path, *fixture_schema(), fixture_rows());
            verify("orc", {path});
            std::cout << "Generated and verified " << path << '\n';
        } else if (command == "generate-sparse") {
            require(argc == 3, "generate-sparse requires one directory");
            generate_sparse(argv[2]);
            std::cout << "Generated and verified sparse ORC fixtures\n";
        } else if (command == "verify-sparse") {
            require(argc >= 4, "verify-sparse requires a case and ORC output files");
            verify_sparse(argv[2], std::vector<std::string>(argv + 3, argv + argc));
            std::cout << "Verified sparse FILE expansion, all six fields and inline bytes\n";
        } else if (command == "verify") {
            require(argc >= 4, "verify requires a format and one or more files");
            const bool expect_null_inline = std::string(argv[3]) == "--expect-null-inline";
            const int first_path = expect_null_inline ? 4 : 3;
            require(argc > first_path, "verify requires one or more files");
            verify(argv[2], std::vector<std::string>(argv + first_path, argv + argc),
                   expect_null_inline);
            std::cout << "Verified six rows, five FILE schema positions, "
                      << (expect_null_inline ? "all public fields and recursively NULL inline\n"
                                             : "all six fields and inline bytes\n");
        } else if (command == "self-test") {
            require(argc == 3, "self-test requires one scratch directory");
            self_test(argv[2]);
            std::cout << "Oracle self-tests passed\n";
        } else {
            throw std::runtime_error("Unknown command " + command);
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
