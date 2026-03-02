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

#include <crc32c/crc32c.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <gflags/gflags.h>

#include <cctype>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <set>
#include <sstream>
#include <string>

#include "common/status.h"
#include "cpp/private_member_accessor.hpp"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "json2pb/pb_to_json.h"
#include "olap/data_dir.h"
#include "olap/decimal12.h"
#include "olap/olap_common.h"
#include "olap/options.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/encoding_info.h"
#include "olap/storage_engine.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "runtime/exec_env.h"
#include "runtime/large_int_value.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "util/coding.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_factory.hpp"

using doris::DataDir;
using doris::StorageEngine;
using doris::Status;
using doris::TabletMeta;
using doris::TabletMetaManager;
using doris::Slice;
using doris::segment_v2::SegmentFooterPB;
using doris::io::FileReaderSPtr;

using namespace doris::segment_v2;
using namespace doris::vectorized;
using namespace doris;

DEFINE_string(root_path, "", "storage root path");
DEFINE_string(operation, "get_meta",
              "valid operation: get_meta, flag, load_meta, delete_meta, show_meta, "
              "show_segment_footer, show_segment_data");
DEFINE_int64(tablet_id, 0, "tablet_id for tablet meta");
DEFINE_int32(schema_hash, 0, "schema_hash for tablet meta");
DEFINE_string(json_meta_path, "", "absolute json meta file path");
DEFINE_string(pb_meta_path, "", "pb meta file path");
DEFINE_string(tablet_file, "", "file to save a set of tablets");
DEFINE_string(file, "", "segment file path");

std::string get_usage(const std::string& progname) {
    std::stringstream ss;
    ss << progname << " is the Doris BE Meta tool.\n";
    ss << "Stop BE first before use this tool.\n";
    ss << "Usage:\n";
    ss << "./meta_tool --operation=get_meta --root_path=/path/to/storage/path "
          "--tablet_id=tabletid --schema_hash=schemahash\n";
    ss << "./meta_tool --operation=load_meta --root_path=/path/to/storage/path "
          "--json_meta_path=path\n";
    ss << "./meta_tool --operation=delete_meta "
          "--root_path=/path/to/storage/path --tablet_id=tabletid "
          "--schema_hash=schemahash\n";
    ss << "./meta_tool --operation=delete_meta --tablet_file=file_path\n";
    ss << "./meta_tool --operation=show_meta --pb_meta_path=path\n";
    ss << "./meta_tool --operation=show_segment_footer --file=/path/to/segment/file\n";
    ss << "./meta_tool --operation=show_segment_data --file=/path/to/segment/file\n";
    return ss.str();
}

void show_meta() {
    TabletMeta tablet_meta;
    Status s = tablet_meta.create_from_file(FLAGS_pb_meta_path);
    if (!s.ok()) {
        std::cout << "load pb meta file:" << FLAGS_pb_meta_path << " failed"
                  << ", status:" << s << std::endl;
        return;
    }
    std::string json_meta;
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    doris::TabletMetaPB tablet_meta_pb;
    tablet_meta.to_meta_pb(&tablet_meta_pb, false);
    json2pb::ProtoMessageToJson(tablet_meta_pb, &json_meta, json_options);
    std::cout << json_meta << std::endl;
}

void get_meta(DataDir* data_dir) {
    std::string value;
    Status s =
            TabletMetaManager::get_json_meta(data_dir, FLAGS_tablet_id, FLAGS_schema_hash, &value);
    if (s.is<doris::ErrorCode::META_KEY_NOT_FOUND>()) {
        std::cout << "no tablet meta for tablet_id:" << FLAGS_tablet_id
                  << ", schema_hash:" << FLAGS_schema_hash << std::endl;
        return;
    }
    std::cout << value << std::endl;
}

void load_meta(DataDir* data_dir) {
    // load json tablet meta into meta
    Status s = TabletMetaManager::load_json_meta(data_dir, FLAGS_json_meta_path);
    if (!s.ok()) {
        std::cout << "load meta failed, status:" << s << std::endl;
        return;
    }
    std::cout << "load meta successfully" << std::endl;
}

void delete_meta(DataDir* data_dir) {
    Status s = TabletMetaManager::remove(data_dir, FLAGS_tablet_id, FLAGS_schema_hash);
    if (!s.ok()) {
        std::cout << "delete tablet meta failed for tablet_id:" << FLAGS_tablet_id
                  << ", schema_hash:" << FLAGS_schema_hash << ", status:" << s << std::endl;
        return;
    }
    std::cout << "delete meta successfully" << std::endl;
}

Status init_data_dir(StorageEngine& engine, const std::string& dir, std::unique_ptr<DataDir>* ret) {
    std::string root_path;
    RETURN_IF_ERROR(doris::io::global_local_filesystem()->canonicalize(dir, &root_path));
    doris::StorePath path;
    auto res = parse_root_path(root_path, &path);
    if (!res.ok()) {
        std::cout << "parse root path failed:" << root_path << std::endl;
        return Status::InternalError("parse root path failed");
    }

    auto p = std::make_unique<DataDir>(engine, path.path, path.capacity_bytes, path.storage_medium);
    if (p == nullptr) {
        std::cout << "new data dir failed" << std::endl;
        return Status::InternalError("new data dir failed");
    }
    res = p->init();
    if (!res.ok()) {
        std::cout << "data_dir load failed" << std::endl;
        return Status::InternalError("data_dir load failed");
    }

    p.swap(*ret);
    return Status::OK();
}

void batch_delete_meta(const std::string& tablet_file) {
    // each line in tablet file indicate a tablet to delete, format is:
    //      data_dir,tablet_id,schema_hash
    // eg:
    //      /data1/palo.HDD,100010,11212389324
    //      /data2/palo.HDD,100010,23049230234
    std::ifstream infile(tablet_file);
    std::string line = "";
    int err_num = 0;
    int delete_num = 0;
    int total_num = 0;
    StorageEngine engine(doris::EngineOptions {});
    std::unordered_map<std::string, std::unique_ptr<DataDir>> dir_map;
    while (std::getline(infile, line)) {
        total_num++;
        std::vector<std::string> v = absl::StrSplit(line, ",");
        if (v.size() != 3) {
            std::cout << "invalid line in tablet_file: " << line << std::endl;
            err_num++;
            continue;
        }
        // 1. get dir
        std::string dir;
        Status st = doris::io::global_local_filesystem()->canonicalize(v[0], &dir);
        if (!st.ok()) {
            std::cout << "invalid root dir in tablet_file: " << line << std::endl;
            err_num++;
            continue;
        }

        if (dir_map.find(dir) == dir_map.end()) {
            // new data dir, init it
            std::unique_ptr<DataDir> data_dir_p;
            st = init_data_dir(engine, dir, &data_dir_p);
            if (!st.ok()) {
                std::cout << "invalid root path:" << FLAGS_root_path
                          << ", error: " << st.to_string() << std::endl;
                err_num++;
                continue;
            }
            dir_map[dir] = std::move(data_dir_p);
            std::cout << "get a new data dir: " << dir << std::endl;
        }
        DataDir* data_dir = dir_map[dir].get();
        if (data_dir == nullptr) {
            std::cout << "failed to get data dir: " << line << std::endl;
            err_num++;
            continue;
        }

        // 2. get tablet id/schema_hash
        int64_t tablet_id;
        if (!absl::SimpleAtoi(v[1], &tablet_id)) {
            std::cout << "invalid tablet id: " << line << std::endl;
            err_num++;
            continue;
        }
        int64_t schema_hash;
        if (!absl::SimpleAtoi(v[2], &schema_hash)) {
            std::cout << "invalid schema hash: " << line << std::endl;
            err_num++;
            continue;
        }

        Status s = TabletMetaManager::remove(data_dir, tablet_id, schema_hash);
        if (!s.ok()) {
            std::cout << "delete tablet meta failed for tablet_id:" << tablet_id
                      << ", schema_hash:" << schema_hash << ", status:" << s << std::endl;
            err_num++;
            continue;
        }

        delete_num++;
    }

    std::cout << "total: " << total_num << ", delete: " << delete_num << ", error: " << err_num
              << std::endl;
    return;
}

Status get_segment_footer(doris::io::FileReader* file_reader, SegmentFooterPB* footer) {
    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    std::string file_name = file_reader->path();
    uint64_t file_size = file_reader->size();
    if (file_size < 12) {
        return Status::Corruption("Bad segment file {}: file size {} < 12", file_name, file_size);
    }

    size_t bytes_read = 0;
    uint8_t fixed_buf[12];
    Slice slice(fixed_buf, 12);
    RETURN_IF_ERROR(file_reader->read_at(file_size - 12, slice, &bytes_read));

    // validate magic number
    const char* k_segment_magic = "D0R1";
    const uint32_t k_segment_magic_length = 4;
    if (memcmp(fixed_buf + 8, k_segment_magic, k_segment_magic_length) != 0) {
        return Status::Corruption("Bad segment file {}: magic number not match", file_name);
    }

    // read footer PB
    uint32_t footer_length = doris::decode_fixed32_le(fixed_buf);
    if (file_size < 12 + footer_length) {
        return Status::Corruption("Bad segment file {}: file size {} < {}", file_name, file_size,
                                  12 + footer_length);
    }
    std::string footer_buf;
    footer_buf.resize(footer_length);
    Slice slice2(footer_buf);
    RETURN_IF_ERROR(file_reader->read_at(file_size - 12 - footer_length, slice2, &bytes_read));

    // validate footer PB's checksum
    uint32_t expect_checksum = doris::decode_fixed32_le(fixed_buf + 4);
    uint32_t actual_checksum = crc32c::Crc32c(footer_buf.data(), footer_buf.size());
    if (actual_checksum != expect_checksum) {
        return Status::Corruption(
                "Bad segment file {}: footer checksum not match, actual={} vs expect={}", file_name,
                actual_checksum, expect_checksum);
    }

    // deserialize footer PB
    if (!footer->ParseFromString(footer_buf)) {
        return Status::Corruption("Bad segment file {}: failed to parse SegmentFooterPB",
                                  file_name);
    }
    return Status::OK();
}

void show_segment_footer(const std::string& file_name) {
    doris::io::FileReaderSPtr file_reader;
    Status status = doris::io::global_local_filesystem()->open_file(file_name, &file_reader);
    if (!status.ok()) {
        std::cout << "open file failed: " << status << std::endl;
        return;
    }
    SegmentFooterPB footer;
    status = get_segment_footer(file_reader.get(), &footer);
    if (!status.ok()) {
        std::cout << "get footer failed: " << status.to_string() << std::endl;
        return;
    }
    std::string json_footer;
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    bool ret = json2pb::ProtoMessageToJson(footer, &json_footer, json_options);
    if (!ret) {
        std::cout << "Convert PB to json failed" << std::endl;
        return;
    }
    std::cout << json_footer << std::endl;
    return;
}

// Helper function to get field type string
std::string get_field_type_string(doris::FieldType type) {
    switch (type) {
    case doris::FieldType::OLAP_FIELD_TYPE_TINYINT:
        return "TINYINT";
    case doris::FieldType::OLAP_FIELD_TYPE_SMALLINT:
        return "SMALLINT";
    case doris::FieldType::OLAP_FIELD_TYPE_INT:
        return "INT";
    case doris::FieldType::OLAP_FIELD_TYPE_BIGINT:
        return "BIGINT";
    case doris::FieldType::OLAP_FIELD_TYPE_LARGEINT:
        return "LARGEINT";
    case doris::FieldType::OLAP_FIELD_TYPE_FLOAT:
        return "FLOAT";
    case doris::FieldType::OLAP_FIELD_TYPE_DOUBLE:
        return "DOUBLE";
    case doris::FieldType::OLAP_FIELD_TYPE_DECIMAL:
        return "DECIMAL";
    case doris::FieldType::OLAP_FIELD_TYPE_DECIMAL32:
        return "DECIMAL32";
    case doris::FieldType::OLAP_FIELD_TYPE_DECIMAL64:
        return "DECIMAL64";
    case doris::FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
        return "DECIMAL128I";
    case doris::FieldType::OLAP_FIELD_TYPE_CHAR:
        return "CHAR";
    case doris::FieldType::OLAP_FIELD_TYPE_VARCHAR:
        return "VARCHAR";
    case doris::FieldType::OLAP_FIELD_TYPE_STRING:
        return "STRING";
    case doris::FieldType::OLAP_FIELD_TYPE_DATE:
        return "DATE";
    case doris::FieldType::OLAP_FIELD_TYPE_DATETIME:
        return "DATETIME";
    case doris::FieldType::OLAP_FIELD_TYPE_DATEV2:
        return "DATEV2";
    case doris::FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
        return "DATETIMEV2";
    case doris::FieldType::OLAP_FIELD_TYPE_BOOL:
        return "BOOLEAN";
    case doris::FieldType::OLAP_FIELD_TYPE_STRUCT:
        return "STRUCT";
    case doris::FieldType::OLAP_FIELD_TYPE_ARRAY:
        return "ARRAY";
    case doris::FieldType::OLAP_FIELD_TYPE_MAP:
        return "MAP";
    case doris::FieldType::OLAP_FIELD_TYPE_JSONB:
        return "JSONB";
    case doris::FieldType::OLAP_FIELD_TYPE_HLL:
        return "HLL";
    case doris::FieldType::OLAP_FIELD_TYPE_BITMAP:
        return "BITMAP";
    case doris::FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE:
        return "QUANTILE_STATE";
    case doris::FieldType::OLAP_FIELD_TYPE_AGG_STATE:
        return "AGG_STATE";
    case doris::FieldType::OLAP_FIELD_TYPE_VARIANT:
        return "VARIANT";
    default:
        return "UNKNOWN";
    }
}

// Helper function to get encoding type string
std::string get_encoding_string(doris::segment_v2::EncodingTypePB encoding) {
    switch (encoding) {
    case doris::segment_v2::PLAIN_ENCODING:
        return "PLAIN";
    case doris::segment_v2::PREFIX_ENCODING:
        return "PREFIX";
    case doris::segment_v2::RLE:
        return "RLE";
    case doris::segment_v2::DICT_ENCODING:
        return "DICT_ENCODING";
    case doris::segment_v2::BIT_SHUFFLE:
        return "BIT_SHUFFLE";
    case doris::segment_v2::FOR_ENCODING:
        return "FOR_ENCODING";
    case doris::segment_v2::PLAIN_ENCODING_V2:
        return "PLAIN_ENCODING_V2";
    default:
        return "UNKNOWN";
    }
}

// Helper function to get compression type string
std::string get_compression_string(doris::segment_v2::CompressionTypePB compression) {
    switch (compression) {
    case doris::segment_v2::NO_COMPRESSION:
        return "NONE";
    case doris::segment_v2::SNAPPY:
        return "SNAPPY";
    case doris::segment_v2::LZ4:
        return "LZ4";
    case doris::segment_v2::LZ4F:
        return "LZ4F";
    case doris::segment_v2::ZLIB:
        return "ZLIB";
    case doris::segment_v2::ZSTD:
        return "ZSTD";
    case doris::segment_v2::LZ4HC:
        return "LZ4HC";
    default:
        return "UNKNOWN";
    }
}

// Helper function to format a single value from a column
std::string format_column_value(const doris::vectorized::IColumn& column, size_t row,
                                doris::FieldType field_type) {
    try {
        switch (field_type) {
        case FieldType::OLAP_FIELD_TYPE_BOOL: {
            return column.get_bool(row) ? "true" : "false";
        }
        case FieldType::OLAP_FIELD_TYPE_TINYINT:
        case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        case FieldType::OLAP_FIELD_TYPE_INT:
        case FieldType::OLAP_FIELD_TYPE_BIGINT: {
            return std::to_string(column.get_int(row));
        }
        case FieldType::OLAP_FIELD_TYPE_LARGEINT: {
            // LargeInt is stored as Int128
            const StringRef& data = column.get_data_at(row);
            if (data.size == sizeof(__int128)) {
                __int128 val = *reinterpret_cast<const __int128*>(data.data);
                return doris::LargeIntValue::to_string(val);
            }
            return "<invalid largeint>";
        }
        case FieldType::OLAP_FIELD_TYPE_FLOAT: {
            const StringRef& data = column.get_data_at(row);
            if (data.size == sizeof(float)) {
                float val = *reinterpret_cast<const float*>(data.data);
                return std::to_string(val);
            }
            return "<invalid float>";
        }
        case FieldType::OLAP_FIELD_TYPE_DOUBLE: {
            const StringRef& data = column.get_data_at(row);
            if (data.size == sizeof(double)) {
                double val = *reinterpret_cast<const double*>(data.data);
                return std::to_string(val);
            }
            return "<invalid double>";
        }
        case FieldType::OLAP_FIELD_TYPE_DATE:
        case FieldType::OLAP_FIELD_TYPE_DATEV2: {
            const StringRef& data = column.get_data_at(row);
            if (data.size == sizeof(uint32_t)) {
                uint32_t val = *reinterpret_cast<const uint32_t*>(data.data);
                return std::to_string(val);
            }
            return "<invalid date>";
        }
        case FieldType::OLAP_FIELD_TYPE_DATETIME:
        case FieldType::OLAP_FIELD_TYPE_DATETIMEV2: {
            const StringRef& data = column.get_data_at(row);
            if (data.size == sizeof(uint64_t)) {
                uint64_t val = *reinterpret_cast<const uint64_t*>(data.data);
                return std::to_string(val);
            }
            return "<invalid datetime>";
        }
        case FieldType::OLAP_FIELD_TYPE_CHAR:
        case FieldType::OLAP_FIELD_TYPE_VARCHAR:
        case FieldType::OLAP_FIELD_TYPE_STRING:
        case FieldType::OLAP_FIELD_TYPE_HLL:
        case FieldType::OLAP_FIELD_TYPE_BITMAP:
        case FieldType::OLAP_FIELD_TYPE_JSONB:
        case FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE: {
            const StringRef& str = column.get_data_at(row);
            std::string result = "'";
            for (size_t i = 0; i < str.size && i < 50; ++i) {
                // Escape quotes and special characters
                char c = str.data[i];
                if (c == '\0') {
                    result += "\\0";
                } else if (c == '\n') {
                    result += "\\n";
                } else if (c == '\r') {
                    result += "\\r";
                } else if (c == '\t') {
                    result += "\\t";
                } else if (c == '\'') {
                    result += "\\'";
                } else if (c == '\\') {
                    result += "\\\\";
                } else if (static_cast<unsigned char>(c) < 32) {
                    // Other control characters
                    char buf[8];
                    snprintf(buf, sizeof(buf), "\\x%02x", static_cast<unsigned char>(c));
                    result += buf;
                } else {
                    result += c;
                }
            }
            if (str.size > 50) {
                result += "...";
            }
            result += "'";
            return result;
        }
        case FieldType::OLAP_FIELD_TYPE_DECIMAL:
        case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
        case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
        case FieldType::OLAP_FIELD_TYPE_DECIMAL128I: {
            const StringRef& data = column.get_data_at(row);
            if (data.size == sizeof(__int128)) {
                __int128 val = *reinterpret_cast<const __int128*>(data.data);
                return doris::LargeIntValue::to_string(val);
            }
            return "<invalid decimal>";
        }
        default:
            return "<unsupported type>";
        }
    } catch (const std::exception& e) {
        return "<error: " + std::string(e.what()) + ">";
    }
}

// Read and print column data values
void print_column_data_values(const doris::segment_v2::ColumnMetaPB& column_meta,
                              const FileReaderSPtr& file_reader, uint64_t num_segment_rows,
                              int indent_level) {
    std::string indent(indent_level * 2, ' ');

    doris::FieldType field_type = static_cast<doris::FieldType>(column_meta.type());

    // Skip complex types for now
    if (!doris::is_scalar_type(field_type)) {
        std::cout << indent << "(Complex type - cannot display values)" << std::endl;
        return;
    }

    if (num_segment_rows == 0) {
        std::cout << indent << "(No data)" << std::endl;
        return;
    }

    // Create a virtual TabletColumn for the column
    doris::TabletColumn tablet_column;
    tablet_column.set_aggregation_method(
            doris::FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE);
    tablet_column.set_type(field_type);
    tablet_column.set_is_nullable(column_meta.is_nullable());
    tablet_column.set_length(0); // Default length
    tablet_column.set_unique_id(column_meta.column_id());

    // Create column reader
    ColumnReaderOptions reader_opts;
    reader_opts.verify_checksum = false; // Don't verify checksum for performance

    std::shared_ptr<ColumnReader> column_reader;
    Status status = ColumnReader::create(reader_opts, column_meta, num_segment_rows, file_reader,
                                         &column_reader);
    if (!status.ok()) {
        std::cout << indent << "(Failed to create column reader: " << status.to_string() << ")"
                  << std::endl;
        return;
    }

    // Create column iterator
    ColumnIteratorUPtr iterator;
    status = column_reader->new_iterator(&iterator, &tablet_column);
    if (!status.ok()) {
        std::cout << indent << "(Failed to create column iterator: " << status.to_string() << ")"
                  << std::endl;
        return;
    }

    // Initialize iterator
    ColumnIteratorOptions iter_opts;
    iter_opts.file_reader = file_reader.get();
    doris::OlapReaderStatistics stats; // Dummy statistics
    iter_opts.stats = &stats;

    status = iterator->init(iter_opts);
    if (!status.ok()) {
        std::cout << indent << "(Failed to initialize column iterator: " << status.to_string()
                  << ")" << std::endl;
        return;
    }

    // Seek to the beginning
    status = iterator->seek_to_ordinal(0);
    if (!status.ok()) {
        std::cout << indent << "(Failed to seek to ordinal 0: " << status.to_string() << ")"
                  << std::endl;
        return;
    }

    // Create destination column for reading data
    auto data_type = doris::vectorized::DataTypeFactory::instance().create_data_type(column_meta);
    if (!data_type) {
        std::cout << indent << "(Failed to create data type for field type "
                  << static_cast<int>(field_type) << ")" << std::endl;
        return;
    }

    MutableColumnPtr dst_column = data_type->create_column();

    // Determine how many rows to display (max 10 rows for readability)
    const size_t max_display_rows = 10;
    size_t rows_to_read = std::min(static_cast<size_t>(num_segment_rows), max_display_rows);
    size_t rows_read = rows_to_read;

    status = iterator->next_batch(&rows_read, dst_column);
    if (!status.ok()) {
        std::cout << indent << "(Failed to read column data: " << status.to_string() << ")"
                  << std::endl;
        return;
    }

    if (rows_read == 0) {
        std::cout << indent << "(No data read)" << std::endl;
        return;
    }

    // Print the values
    std::cout << indent << "Data Values (" << rows_read << " of " << num_segment_rows
              << " rows, showing first " << std::min(rows_read, max_display_rows)
              << "):" << std::endl;

    for (size_t i = 0; i < rows_read; ++i) {
        std::cout << indent << "  [" << i << "] ";
        if (column_meta.is_nullable()) {
            const auto& nullable_col = assert_cast<const ColumnNullable&>(*dst_column);
            if (nullable_col.is_null_at(i)) {
                std::cout << "NULL";
            } else {
                const IColumn& nested_col = nullable_col.get_nested_column();
                std::cout << format_column_value(nested_col, i, field_type);
            }
        } else {
            std::cout << format_column_value(*dst_column, i, field_type);
        }
        std::cout << std::endl;
    }

    if (num_segment_rows > max_display_rows) {
        std::cout << indent << "  ... (" << (num_segment_rows - max_display_rows) << " more rows)"
                  << std::endl;
    }
}

// Helper function to print column metadata
void print_column_meta(const doris::segment_v2::ColumnMetaPB& column_meta,
                       const FileReaderSPtr& file_reader, uint64_t num_segment_rows,
                       int indent_level) {
    std::string indent(indent_level * 2, ' ');
    std::string column_name;
    if (column_meta.has_column_path_info() && column_meta.column_path_info().has_path()) {
        column_name = column_meta.column_path_info().path();
    } else {
        column_name = "column_id_" + std::to_string(column_meta.column_id());
    }

    doris::FieldType field_type = static_cast<doris::FieldType>(column_meta.type());
    std::cout << indent << "=== " << column_name << ": type=" << get_field_type_string(field_type)
              << ", nullable=" << (column_meta.is_nullable() ? "true" : "false")
              << ", encoding=" << get_encoding_string(column_meta.encoding())
              << " ===" << std::endl;

    // Print size info
    if (column_meta.has_compressed_data_bytes()) {
        std::cout << indent << "Data Size (Compressed): " << column_meta.compressed_data_bytes()
                  << " bytes" << std::endl;
    }
    if (column_meta.has_uncompressed_data_bytes()) {
        std::cout << indent << "Data Size (Uncompressed): " << column_meta.uncompressed_data_bytes()
                  << " bytes" << std::endl;
    }
    if (column_meta.has_raw_data_bytes()) {
        std::cout << indent << "Raw Data Size: " << column_meta.raw_data_bytes() << " bytes"
                  << std::endl;
    }

    // Print dict page info
    if (column_meta.has_dict_page()) {
        const auto& dict_page = column_meta.dict_page();
        std::cout << indent << "Dictionary Page: offset=" << dict_page.offset()
                  << ", size=" << dict_page.size() << " bytes" << std::endl;
    }

    // Print indexes info
    if (column_meta.indexes_size() > 0) {
        std::cout << indent << "Indexes: ";
        for (int i = 0; i < column_meta.indexes_size(); ++i) {
            if (i > 0) std::cout << ", ";
            const auto& index_meta = column_meta.indexes(i);
            if (index_meta.has_type()) {
                switch (index_meta.type()) {
                case doris::segment_v2::ORDINAL_INDEX:
                    std::cout << "ORDINAL";
                    break;
                case doris::segment_v2::ZONE_MAP_INDEX:
                    std::cout << "ZONE_MAP";
                    break;
                case doris::segment_v2::BLOOM_FILTER_INDEX:
                    std::cout << "BLOOM_FILTER";
                    break;
                case doris::segment_v2::BITMAP_INDEX:
                    std::cout << "BITMAP";
                    break;
                default:
                    std::cout << "UNKNOWN";
                    break;
                }
            }
        }
        std::cout << std::endl;
    }

    // Handle complex types recursively
    if (column_meta.children_columns_size() > 0) {
        std::cout << indent << "Sub-columns: " << column_meta.children_columns_size() << std::endl;
        for (int i = 0; i < column_meta.children_columns_size(); ++i) {
            print_column_meta(column_meta.children_columns(i), file_reader, num_segment_rows,
                              indent_level + 1);
        }
        return;
    }

    // Print column data values for scalar types
    if (doris::is_scalar_type(field_type)) {
        print_column_data_values(column_meta, file_reader, num_segment_rows, indent_level);
    } else {
        std::cout << indent << "(Complex type - cannot display values)" << std::endl;
    }
}

// Register hijacked accessors
ACCESS_PRIVATE_FIELD(ExecEnv_encoding_info_resolver, ExecEnv, segment_v2::EncodingInfoResolver*,
                     _encoding_info_resolver);
ACCESS_PRIVATE_FIELD(ExecEnv_orphan_mem_tracker, ExecEnv, std::shared_ptr<MemTrackerLimiter>,
                     _orphan_mem_tracker);
ACCESS_PRIVATE_STATIC_FIELD(ExecEnv_tracking_memory, ExecEnv, std::atomic_bool, _s_tracking_memory);

void show_segment_data(const std::string& file_name) {
    // Initialize ExecEnv components needed for ColumnReader
    // Use macro to access private members temporarily
    auto* exec_env = doris::ExecEnv::GetInstance();

    auto resolver = GET_PRIVATE_FIELD(ExecEnv_encoding_info_resolver);
    auto mem_tracker = GET_PRIVATE_FIELD(ExecEnv_orphan_mem_tracker);
    auto tracking_memory = GET_PRIVATE_STATIC_FIELD(ExecEnv_tracking_memory);
    // Initialize encoding info resolver for ColumnReader
    if (exec_env->*resolver == nullptr) {
        exec_env->*resolver = new doris::segment_v2::EncodingInfoResolver();
    }
    // Initialize mem tracker limiter pool and orphan mem tracker for ThreadMemTrackerMgr
    if (exec_env->mem_tracker_limiter_pool.empty()) {
        exec_env->mem_tracker_limiter_pool.resize(doris::MEM_TRACKER_GROUP_NUM,
                                                  doris::TrackerLimiterGroup());
        (*tracking_memory).store(true, std::memory_order_release);
        exec_env->*mem_tracker = doris::MemTrackerLimiter::create_shared(
                doris::MemTrackerLimiter::Type::GLOBAL, "Orphan");
    }

    doris::io::FileReaderSPtr file_reader;
    Status status = doris::io::global_local_filesystem()->open_file(file_name, &file_reader);
    if (!status.ok()) {
        std::cout << "open file failed: " << status << std::endl;
        return;
    }

    SegmentFooterPB footer;
    status = get_segment_footer(file_reader.get(), &footer);
    if (!status.ok()) {
        std::cout << "get footer failed: " << status.to_string() << std::endl;
        return;
    }

    // Print basic info
    std::cout << "\n=== Segment File Info ===" << std::endl;
    std::cout << "File: " << file_name << std::endl;
    std::cout << "Num Rows: " << footer.num_rows() << std::endl;
    std::cout << "Num Columns: " << footer.columns_size() << std::endl;
    std::cout << "Compression: " << get_compression_string(footer.compress_type()) << std::endl;
    if (footer.has_version()) {
        std::cout << "Version: " << footer.version() << std::endl;
    }
    std::cout << std::endl;

    // Collect statistics
    uint64_t total_compressed_data_bytes = 0;
    uint64_t total_uncompressed_data_bytes = 0;
    uint64_t total_raw_data_bytes = 0;
    uint32_t total_ordinal_indexes = 0;
    uint32_t total_zone_map_indexes = 0;
    uint32_t total_bloom_filter_indexes = 0;
    uint32_t columns_with_dict = 0;

    // Print each column
    for (int i = 0; i < footer.columns_size(); ++i) {
        const auto& column_meta = footer.columns(i);
        print_column_meta(column_meta, file_reader, footer.num_rows(), 0);

        // Collect statistics
        if (column_meta.has_compressed_data_bytes()) {
            total_compressed_data_bytes += column_meta.compressed_data_bytes();
        }
        if (column_meta.has_uncompressed_data_bytes()) {
            total_uncompressed_data_bytes += column_meta.uncompressed_data_bytes();
        }
        if (column_meta.has_raw_data_bytes()) {
            total_raw_data_bytes += column_meta.raw_data_bytes();
        }

        // Count indexes
        for (int j = 0; j < column_meta.indexes_size(); ++j) {
            const auto& index_meta = column_meta.indexes(j);
            if (index_meta.has_type()) {
                switch (index_meta.type()) {
                case doris::segment_v2::ORDINAL_INDEX:
                    total_ordinal_indexes++;
                    break;
                case doris::segment_v2::ZONE_MAP_INDEX:
                    total_zone_map_indexes++;
                    break;
                case doris::segment_v2::BLOOM_FILTER_INDEX:
                    total_bloom_filter_indexes++;
                    break;
                default:
                    break;
                }
            }
        }

        if (column_meta.has_dict_page()) {
            columns_with_dict++;
        }

        std::cout << std::endl;
    }

    // Print statistics
    std::cout << "\n=== Statistics ===" << std::endl;
    uint32_t total_indexes =
            total_ordinal_indexes + total_zone_map_indexes + total_bloom_filter_indexes;

    std::cout << "Total Columns: " << footer.columns_size() << std::endl;
    std::cout << "Columns with Dictionary: " << columns_with_dict << std::endl;
    std::cout << "Total Indexes: " << total_indexes << std::endl;
    std::cout << "  - Ordinal Indexes: " << total_ordinal_indexes << std::endl;
    std::cout << "  - Zone Map Indexes: " << total_zone_map_indexes << std::endl;
    std::cout << "  - Bloom Filter Indexes: " << total_bloom_filter_indexes << std::endl;
    std::cout << "Total Data Size (Compressed): " << total_compressed_data_bytes << " bytes ("
              << std::fixed << std::setprecision(2) << (total_compressed_data_bytes / 1024.0)
              << " KB)" << std::endl;
    std::cout << "Total Data Size (Uncompressed): " << total_uncompressed_data_bytes << " bytes ("
              << std::fixed << std::setprecision(2) << (total_uncompressed_data_bytes / 1024.0)
              << " KB)" << std::endl;
    std::cout << "Total Raw Data Size: " << total_raw_data_bytes << " bytes (" << std::fixed
              << std::setprecision(2) << (total_raw_data_bytes / 1024.0) << " KB)" << std::endl;
    if (footer.has_index_footprint()) {
        std::cout << "Index Footprint: " << footer.index_footprint() << " bytes (" << std::fixed
                  << std::setprecision(2) << (footer.index_footprint() / 1024.0) << " KB)"
                  << std::endl;
    }
    if (footer.has_data_footprint()) {
        std::cout << "Data Footprint: " << footer.data_footprint() << " bytes (" << std::fixed
                  << std::setprecision(2) << (footer.data_footprint() / 1024.0) << " KB)"
                  << std::endl;
    }
}

int main(int argc, char** argv) {
    SCOPED_INIT_THREAD_CONTEXT();
    std::string usage = get_usage(argv[0]);
    gflags::SetUsageMessage(usage);
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_operation == "show_meta") {
        show_meta();
    } else if (FLAGS_operation == "batch_delete_meta") {
        std::string tablet_file;
        Status st =
                doris::io::global_local_filesystem()->canonicalize(FLAGS_tablet_file, &tablet_file);
        if (!st.ok()) {
            std::cout << "invalid tablet file: " << FLAGS_tablet_file
                      << ", error: " << st.to_string() << std::endl;
            return -1;
        }

        batch_delete_meta(tablet_file);
    } else if (FLAGS_operation == "show_segment_footer") {
        if (FLAGS_file == "") {
            std::cout << "no file flag for show dict" << std::endl;
            return -1;
        }
        show_segment_footer(FLAGS_file);
    } else if (FLAGS_operation == "show_segment_data") {
        if (FLAGS_file == "") {
            std::cout << "no file flag for show_segment_data" << std::endl;
            return -1;
        }
        show_segment_data(FLAGS_file);
    } else {
        // operations that need root path should be written here
        std::set<std::string> valid_operations = {"get_meta", "load_meta", "delete_meta"};
        if (valid_operations.find(FLAGS_operation) == valid_operations.end()) {
            std::cout << "invalid operation:" << FLAGS_operation << std::endl;
            return -1;
        }

        StorageEngine engine(doris::EngineOptions {});
        std::unique_ptr<DataDir> data_dir;
        Status st = init_data_dir(engine, FLAGS_root_path, &data_dir);
        if (!st.ok()) {
            std::cout << "invalid root path:" << FLAGS_root_path << ", error: " << st.to_string()
                      << std::endl;
            return -1;
        }

        if (FLAGS_operation == "get_meta") {
            get_meta(data_dir.get());
        } else if (FLAGS_operation == "load_meta") {
            load_meta(data_dir.get());
        } else if (FLAGS_operation == "delete_meta") {
            delete_meta(data_dir.get());
        } else {
            std::cout << "invalid operation: " << FLAGS_operation << "\n" << usage << std::endl;
            return -1;
        }
    }
    gflags::ShutDownCommandLineFlags();
    return 0;
}
