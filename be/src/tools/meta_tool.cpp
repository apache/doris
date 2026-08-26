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
#include <charconv>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/decimal12.h"
#include "core/field.h"
#include "core/types.h"
#include "core/value/large_int_value.h"
#include "cpp/private_member_accessor.hpp"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "json2pb/pb_to_json.h"
#include "runtime/exec_env.h"
#include "runtime/memory/cache_manager.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "storage/data_dir.h"
#include "storage/olap_common.h"
#include "storage/options.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/common.h"
#include "storage/segment/encoding_info.h"
#include "storage/segment/page_pointer.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_column_object_pool.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_meta_manager.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/tablet/tablet_schema_cache.h"
#include "storage/types.h"
#include "util/coding.h"
#include "util/unaligned.h"

using doris::DataDir;
using doris::StorageEngine;
using doris::Status;
using doris::TabletMeta;
using doris::TabletMetaManager;
using doris::Slice;
using doris::segment_v2::SegmentFooterPB;
using doris::io::FileReaderSPtr;

using namespace doris::segment_v2;
using namespace doris;
using namespace doris;

DEFINE_string(root_path, "", "storage root path");
DEFINE_string(operation, "get_meta",
              "valid operation: get_meta, flag, load_meta, delete_meta, show_meta, "
              "show_segment_footer, show_segment_data, check_page_crc, scan_page_crc, "
              "gen_empty_segment");
DEFINE_int64(tablet_id, 0, "tablet_id for tablet meta");
DEFINE_int32(num_rows_per_block, 1024, "num rows per block");
DEFINE_int32(schema_hash, 0, "schema_hash for tablet meta");
DEFINE_string(json_meta_path, "", "absolute json meta file path");
DEFINE_string(pb_meta_path, "", "pb meta file path");
DEFINE_string(tablet_file, "", "file to save a set of tablets");
DEFINE_string(file, "", "segment file path");
DEFINE_string(output_path, "", "output directory path (default: current directory)");
DEFINE_int32(num_short_key_columns, 0, "number of short key columns");
DEFINE_bool(has_sequence_col, false, "whether has sequence column");
DEFINE_bool(enable_unique_key_merge_on_write, false, "whether enable unique key merge on write");
DEFINE_bool(scan_segment_pages, false,
            "scan every data page referenced by the ordinal index and report checksum ranges");
DEFINE_int64(rows, 10,
             "maximum logical rows to read per column for show_segment_data; -1 means all rows");
DEFINE_uint64(row_start, 0, "first logical row ordinal to read for show_segment_data");
DEFINE_uint64(batch_rows, 4096, "maximum rows per read batch for show_segment_data");
DEFINE_bool(check_only, false, "decode rows without printing values for show_segment_data");
DEFINE_bool(verify_checksum, true,
            "verify page checksums while decoding data for show_segment_data");
DEFINE_string(page_ranges, "",
              "comma-separated OFFSET:SIZE ranges for the check_page_crc operation");
DEFINE_string(page_ranges_file, "",
              "file containing OFFSET SIZE pairs for the check_page_crc operation");
DEFINE_uint64(page_scan_start, 0, "known first page offset for the scan_page_crc operation");
DEFINE_uint64(page_scan_end, 0, "exclusive scan end offset for the scan_page_crc operation");
DEFINE_string(page_output, "all", "page CRC output level: all, errors, or summary");

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
    ss << "./meta_tool --operation=batch_delete_meta --tablet_file=file_path\n";
    ss << "./meta_tool --operation=show_meta --pb_meta_path=path\n";
    ss << "./meta_tool --operation=show_segment_footer --file=/path/to/segment/file\n";
    ss << "./meta_tool --operation=show_segment_data --file=/path/to/segment/file "
          "[--row_start=N] [--rows=N|-1] [--batch_rows=N] [--check_only] "
          "[--verify_checksum]\n";
    ss << "./meta_tool --operation=check_page_crc --file=/path/to/segment/file "
          "(--page_ranges=OFFSET:SIZE,... | --page_ranges_file=/path/to/ranges) "
          "[--page_output=all|errors|summary]\n";
    ss << "./meta_tool --operation=scan_page_crc --file=/path/to/segment/file "
          "--page_scan_start=OFFSET --page_scan_end=OFFSET "
          "[--page_output=all|errors|summary]\n";
    ss << "./meta_tool --operation=gen_empty_segment [--output_path=/path/to/output]\n";
    ss << "  Generates an empty segment file (0 rows) at specified path or current directory\n";
    ss << "  Default output file name: empty.dat\n";
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
    if (!s.ok()) {
        if (s.is<doris::ErrorCode::META_KEY_NOT_FOUND>()) {
            std::cout << "no tablet meta for tablet_id:" << FLAGS_tablet_id
                      << ", schema_hash:" << FLAGS_schema_hash << std::endl;
        } else {
            std::cout << "get meta failed: " << s.to_string() << std::endl;
        }
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
        std::cout << "data_dir load failed: " << res.to_string() << std::endl;
        return res;
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
    if (memcmp(fixed_buf + 8, doris::segment_v2::k_segment_magic,
               doris::segment_v2::k_segment_magic_length) != 0) {
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
std::string format_column_value(const doris::IColumn& column, size_t row,
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
                // data.data may not be 16-byte aligned; use unaligned_load to avoid UB.
                __int128 val = unaligned_load<__int128>(data.data);
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
                // data.data may not be 16-byte aligned; use unaligned_load to avoid UB.
                __int128 val = unaligned_load<__int128>(data.data);
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

enum class PageOutputMode { ALL, ERRORS, SUMMARY };

struct RawPageRange {
    uint64_t offset = 0;
    uint64_t size = 0;
};

struct PageChecksumScanResult {
    uint64_t offset = 0;
    uint64_t size = 0;
    bool range_valid = false;
    bool readable = false;
    bool checksum_ok = false;
    bool footer_size_ok = false;
    bool footer_ok = false;
    uint32_t actual_checksum = 0;
    uint32_t expected_checksum = 0;
    uint32_t footer_size = 0;
    PageTypePB page_type = UNKNOWN_PAGE_TYPE;
    std::string error;

    bool ok() const {
        return range_valid && readable && checksum_ok && footer_size_ok && footer_ok;
    }
};

struct PageCrcSummary {
    uint64_t pages_checked = 0;
    uint64_t valid = 0;
    uint64_t bad_crc = 0;
    uint64_t bad_footer = 0;
    uint64_t unreadable = 0;
    uint64_t invalid_range = 0;

    void add(const PageChecksumScanResult& result) {
        ++pages_checked;
        if (result.ok()) {
            ++valid;
            return;
        }
        if (!result.range_valid) {
            ++invalid_range;
        } else if (!result.readable) {
            ++unreadable;
        } else {
            if (!result.checksum_ok) {
                ++bad_crc;
            }
            if (!result.footer_size_ok || !result.footer_ok) {
                ++bad_footer;
            }
        }
    }

    bool ok() const { return pages_checked > 0 && valid == pages_checked; }
};

Status read_file_exact(const FileReaderSPtr& file_reader, uint64_t offset, char* data,
                       size_t size) {
    size_t bytes_read = 0;
    Status status =
            file_reader->read_at(static_cast<size_t>(offset), Slice(data, size), &bytes_read);
    if (!status.ok()) {
        status.prepend("failed to read file at offset " + std::to_string(offset) + ": ");
        return status;
    }
    if (bytes_read != size) {
        return Status::IOError("short read at offset {}: expected={}, actual={}", offset, size,
                               bytes_read);
    }
    return Status::OK();
}

PageChecksumScanResult check_page_checksum(const FileReaderSPtr& file_reader, uint64_t offset,
                                           uint64_t size) {
    PageChecksumScanResult result;
    result.offset = offset;
    result.size = size;
    if (size < 8) {
        result.error = "size_less_than_8";
        return result;
    }

    const uint64_t file_size = file_reader->size();
    if (offset > file_size || size > file_size - offset) {
        result.error = "out_of_range";
        return result;
    }
    result.range_valid = true;

    char trailer[8];
    if (!read_file_exact(file_reader, offset + size - sizeof(trailer), trailer, sizeof(trailer))
                 .ok()) {
        result.error = "read_failed";
        return result;
    }
    result.footer_size = doris::decode_fixed32_le(reinterpret_cast<const uint8_t*>(trailer));
    result.expected_checksum =
            doris::decode_fixed32_le(reinterpret_cast<const uint8_t*>(trailer + 4));

    constexpr size_t crc_chunk_size = 1024 * 1024;
    size_t buffer_size = static_cast<size_t>(std::min<uint64_t>(size - 4, crc_chunk_size));
    std::vector<char> buffer(buffer_size);
    uint64_t cursor = offset;
    uint64_t remaining = size - 4;
    uint32_t checksum = 0;
    while (remaining > 0) {
        size_t bytes_to_read = static_cast<size_t>(std::min<uint64_t>(remaining, buffer.size()));
        if (!read_file_exact(file_reader, cursor, buffer.data(), bytes_to_read).ok()) {
            result.error = "read_failed";
            return result;
        }
        checksum = crc32c::Extend(checksum, reinterpret_cast<const uint8_t*>(buffer.data()),
                                  bytes_to_read);
        cursor += bytes_to_read;
        remaining -= bytes_to_read;
    }
    result.readable = true;
    result.actual_checksum = checksum;
    result.checksum_ok = result.actual_checksum == result.expected_checksum;

    result.footer_size_ok = result.footer_size > 0 && result.footer_size <= size - 8 &&
                            result.footer_size <= std::numeric_limits<int>::max();
    if (result.footer_size_ok) {
        std::vector<char> footer_buffer(result.footer_size);
        uint64_t footer_offset = offset + size - 8 - result.footer_size;
        Status footer_status = read_file_exact(file_reader, footer_offset, footer_buffer.data(),
                                               footer_buffer.size());
        if (footer_status.ok()) {
            PageFooterPB footer;
            result.footer_ok = footer.ParseFromArray(footer_buffer.data(),
                                                     static_cast<int>(footer_buffer.size())) &&
                               footer.has_type() && footer.has_uncompressed_size();
            if (result.footer_ok) {
                result.page_type = footer.type();
            }
        }
    }

    if (!result.checksum_ok) {
        result.error = "checksum_mismatch";
    } else if (!result.footer_size_ok) {
        result.error = "invalid_footer_size";
    } else if (!result.footer_ok) {
        result.error = "invalid_footer";
    }
    return result;
}

PageChecksumScanResult scan_page_checksum(const FileReaderSPtr& file_reader,
                                          const PagePointer& page_pointer) {
    return check_page_checksum(file_reader, page_pointer.offset, page_pointer.size);
}

std::string_view trim_ascii(std::string_view value) {
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front()))) {
        value.remove_prefix(1);
    }
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back()))) {
        value.remove_suffix(1);
    }
    return value;
}

Status parse_uint64_value(std::string_view text, const std::string& field_name, uint64_t& value) {
    text = trim_ascii(text);
    if (text.empty()) {
        return Status::InvalidArgument("{} is empty", field_name);
    }
    auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value, 10);
    if (error != std::errc() || end != text.data() + text.size()) {
        return Status::InvalidArgument("invalid decimal {}: '{}'", field_name, text);
    }
    return Status::OK();
}

Status parse_page_range_token(std::string_view token, RawPageRange& range) {
    token = trim_ascii(token);
    size_t separator = token.find(':');
    if (separator == std::string_view::npos ||
        token.find(':', separator + 1) != std::string_view::npos) {
        return Status::InvalidArgument("invalid page range '{}', expected OFFSET:SIZE", token);
    }
    RETURN_IF_ERROR(parse_uint64_value(token.substr(0, separator), "page offset", range.offset));
    RETURN_IF_ERROR(parse_uint64_value(token.substr(separator + 1), "page size", range.size));
    return Status::OK();
}

Status parse_inline_page_ranges(const std::string& input, std::vector<RawPageRange>& ranges) {
    size_t begin = 0;
    while (begin <= input.size()) {
        size_t end = input.find(',', begin);
        std::string_view token(input.data() + begin,
                               (end == std::string::npos ? input.size() : end) - begin);
        if (trim_ascii(token).empty()) {
            return Status::InvalidArgument("page_ranges contains an empty range");
        }
        RawPageRange range;
        RETURN_IF_ERROR(parse_page_range_token(token, range));
        ranges.push_back(range);
        if (end == std::string::npos) {
            break;
        }
        begin = end + 1;
    }
    return Status::OK();
}

Status parse_page_ranges_file(const std::string& file_name, std::vector<RawPageRange>& ranges) {
    std::ifstream input(file_name);
    if (!input.is_open()) {
        return Status::IOError("failed to open page ranges file {}", file_name);
    }

    std::string line;
    uint64_t line_number = 0;
    while (std::getline(input, line)) {
        ++line_number;
        size_t comment = line.find('#');
        std::string_view content(line.data(), comment == std::string::npos ? line.size() : comment);
        content = trim_ascii(content);
        if (content.empty()) {
            continue;
        }

        std::istringstream line_stream {std::string(content)};
        std::string offset_text;
        std::string size_text;
        std::string extra;
        if (!(line_stream >> offset_text >> size_text) || (line_stream >> extra)) {
            return Status::InvalidArgument("invalid page ranges file line {}: expected OFFSET SIZE",
                                           line_number);
        }
        RawPageRange range;
        RETURN_IF_ERROR(parse_uint64_value(offset_text, "page offset", range.offset));
        RETURN_IF_ERROR(parse_uint64_value(size_text, "page size", range.size));
        ranges.push_back(range);
    }
    if (!input.eof()) {
        return Status::IOError("failed while reading page ranges file {}", file_name);
    }
    return Status::OK();
}

Status load_page_ranges(std::vector<RawPageRange>& ranges) {
    bool has_inline_ranges = !FLAGS_page_ranges.empty();
    bool has_ranges_file = !FLAGS_page_ranges_file.empty();
    if (has_inline_ranges == has_ranges_file) {
        return Status::InvalidArgument(
                "check_page_crc requires exactly one of page_ranges or page_ranges_file");
    }
    if (has_inline_ranges) {
        RETURN_IF_ERROR(parse_inline_page_ranges(FLAGS_page_ranges, ranges));
    } else {
        RETURN_IF_ERROR(parse_page_ranges_file(FLAGS_page_ranges_file, ranges));
    }
    if (ranges.empty()) {
        return Status::InvalidArgument("no page ranges were provided");
    }
    return Status::OK();
}

Status parse_page_output_mode(PageOutputMode& mode) {
    if (FLAGS_page_output == "all") {
        mode = PageOutputMode::ALL;
    } else if (FLAGS_page_output == "errors") {
        mode = PageOutputMode::ERRORS;
    } else if (FLAGS_page_output == "summary") {
        mode = PageOutputMode::SUMMARY;
    } else {
        return Status::InvalidArgument("page_output must be all, errors, or summary, got '{}'",
                                       FLAGS_page_output);
    }
    return Status::OK();
}

bool should_print_page(PageOutputMode mode, const PageChecksumScanResult& result) {
    return mode == PageOutputMode::ALL || (mode == PageOutputMode::ERRORS && !result.ok());
}

void print_page_checksum_result(uint64_t page_index, const PageChecksumScanResult& result) {
    std::cout << "page=" << page_index << " offset=" << result.offset << " size=" << result.size
              << " range_valid=" << (result.range_valid ? "true" : "false")
              << " readable=" << (result.readable ? "true" : "false")
              << " actual=" << result.actual_checksum << " expect=" << result.expected_checksum
              << " checksum_ok=" << (result.checksum_ok ? "true" : "false")
              << " footer_size=" << result.footer_size
              << " footer_ok=" << (result.footer_ok ? "true" : "false")
              << " page_type=" << PageTypePB_Name(result.page_type);
    if (!result.error.empty()) {
        std::cout << " error=" << result.error;
    }
    std::cout << std::endl;
}

void print_page_crc_summary(const std::string& mode, const PageCrcSummary& summary,
                            const std::string& status) {
    std::cout << "\n=== Page CRC Summary ===" << std::endl;
    std::cout << "Mode: " << mode << std::endl;
    std::cout << "Pages Checked: " << summary.pages_checked << std::endl;
    std::cout << "Valid: " << summary.valid << std::endl;
    std::cout << "Bad CRC: " << summary.bad_crc << std::endl;
    std::cout << "Bad Footer: " << summary.bad_footer << std::endl;
    std::cout << "Unreadable: " << summary.unreadable << std::endl;
    std::cout << "Invalid Range: " << summary.invalid_range << std::endl;
    std::cout << "Status: " << status << std::endl;
}

Status open_page_crc_file(FileReaderSPtr& file_reader) {
    Status status = doris::io::global_local_filesystem()->open_file(FLAGS_file, &file_reader);
    if (!status.ok()) {
        status.prepend("failed to open page CRC input " + FLAGS_file + ": ");
    }
    return status;
}

Status check_page_crc_ranges() {
    PageOutputMode output_mode;
    RETURN_IF_ERROR(parse_page_output_mode(output_mode));
    std::vector<RawPageRange> ranges;
    RETURN_IF_ERROR(load_page_ranges(ranges));

    FileReaderSPtr file_reader;
    RETURN_IF_ERROR(open_page_crc_file(file_reader));
    std::cout << "=== Page CRC Check ===" << std::endl;
    std::cout << "File: " << FLAGS_file << std::endl;
    std::cout << "File Size: " << file_reader->size() << std::endl;
    std::cout << "Ranges: " << ranges.size() << std::endl;

    PageCrcSummary summary;
    for (size_t i = 0; i < ranges.size(); ++i) {
        PageChecksumScanResult result =
                check_page_checksum(file_reader, ranges[i].offset, ranges[i].size);
        summary.add(result);
        if (should_print_page(output_mode, result)) {
            print_page_checksum_result(i, result);
        }
    }

    print_page_crc_summary("ranges", summary, summary.ok() ? "OK" : "CORRUPTION");
    if (!summary.ok()) {
        return Status::Corruption(
                "page CRC check failed: valid={}, checked={}, bad_crc={}, bad_footer={}, "
                "unreadable={}, invalid_range={}",
                summary.valid, summary.pages_checked, summary.bad_crc, summary.bad_footer,
                summary.unreadable, summary.invalid_range);
    }
    return Status::OK();
}

Status find_next_crc_page(const FileReaderSPtr& file_reader, uint64_t page_offset,
                          uint64_t scan_end, PageChecksumScanResult& result, bool& found) {
    constexpr size_t scan_chunk_size = 1024 * 1024;
    std::vector<char> buffer(scan_chunk_size + 4);
    uint64_t cursor = page_offset;
    uint32_t checksum = 0;
    found = false;

    while (scan_end - cursor >= 5) {
        size_t bytes_to_process =
                static_cast<size_t>(std::min<uint64_t>(scan_chunk_size, scan_end - cursor - 4));
        size_t bytes_to_read = bytes_to_process + 4;
        RETURN_IF_ERROR(read_file_exact(file_reader, cursor, buffer.data(), bytes_to_read));
        for (size_t i = 0; i < bytes_to_process; ++i) {
            checksum = crc32c::Extend(checksum, reinterpret_cast<const uint8_t*>(buffer.data() + i),
                                      1);
            uint64_t payload_size = cursor + i - page_offset + 1;
            if (payload_size < 4) {
                continue;
            }
            uint32_t expected = doris::decode_fixed32_le(
                    reinterpret_cast<const uint8_t*>(buffer.data() + i + 1));
            if (checksum != expected) {
                continue;
            }

            PageChecksumScanResult candidate =
                    check_page_checksum(file_reader, page_offset, payload_size + 4);
            if (candidate.ok()) {
                result = std::move(candidate);
                found = true;
                return Status::OK();
            }
        }
        cursor += bytes_to_process;
    }
    return Status::OK();
}

Status scan_page_crc_range() {
    PageOutputMode output_mode;
    RETURN_IF_ERROR(parse_page_output_mode(output_mode));
    if (gflags::GetCommandLineFlagInfoOrDie("page_scan_start").is_default ||
        gflags::GetCommandLineFlagInfoOrDie("page_scan_end").is_default) {
        return Status::InvalidArgument(
                "scan_page_crc requires explicit page_scan_start and page_scan_end");
    }
    if (FLAGS_page_scan_end <= FLAGS_page_scan_start) {
        return Status::InvalidArgument("page_scan_end {} must be greater than page_scan_start {}",
                                       FLAGS_page_scan_end, FLAGS_page_scan_start);
    }

    FileReaderSPtr file_reader;
    RETURN_IF_ERROR(open_page_crc_file(file_reader));
    if (FLAGS_page_scan_end > file_reader->size()) {
        return Status::InvalidArgument("page_scan_end {} exceeds file size {}", FLAGS_page_scan_end,
                                       file_reader->size());
    }

    std::cout << "=== Page CRC Scan ===" << std::endl;
    std::cout << "File: " << FLAGS_file << std::endl;
    std::cout << "File Size: " << file_reader->size() << std::endl;
    std::cout << "Range: [" << FLAGS_page_scan_start << "," << FLAGS_page_scan_end << ")"
              << std::endl;

    PageCrcSummary summary;
    uint64_t page_offset = FLAGS_page_scan_start;
    uint64_t page_index = 0;
    while (page_offset < FLAGS_page_scan_end) {
        PageChecksumScanResult result;
        bool found = false;
        RETURN_IF_ERROR(
                find_next_crc_page(file_reader, page_offset, FLAGS_page_scan_end, result, found));
        if (!found) {
            if (output_mode != PageOutputMode::SUMMARY) {
                std::cout << "no_page_boundary offset=" << page_offset
                          << " remaining=" << (FLAGS_page_scan_end - page_offset) << std::endl;
            }
            print_page_crc_summary("scan", summary, "CORRUPTION");
            return Status::Corruption("no page boundary at offset {}, remaining={}", page_offset,
                                      FLAGS_page_scan_end - page_offset);
        }

        summary.add(result);
        if (should_print_page(output_mode, result)) {
            print_page_checksum_result(page_index, result);
        }
        page_offset += result.size;
        ++page_index;
    }

    print_page_crc_summary("scan", summary, summary.ok() ? "OK" : "CORRUPTION");
    if (!summary.ok()) {
        return Status::Corruption("page CRC scan failed");
    }
    return Status::OK();
}

// Keep the forensic scan linear so every counter and printed example follows page order.
// NOLINTNEXTLINE(readability-function-size)
Status print_column_page_checksums(const std::shared_ptr<ColumnReader>& column_reader,
                                   const doris::segment_v2::ColumnMetaPB& column_meta,
                                   const FileReaderSPtr& file_reader, int indent_level) {
    std::string indent(indent_level * 2, ' ');
    doris::OlapReaderStatistics stats;
    OrdinalIndexReader* ordinal_index = nullptr;
    Status status = column_reader->get_ordinal_index_reader(ordinal_index, &stats);
    if (!status.ok()) {
        status.prepend("failed to load ordinal index for column " +
                       std::to_string(column_meta.column_id()) + ": ");
        return status;
    }

    constexpr uint64_t s3_part_size = 5 * 1024 * 1024;
    int valid_pages = 0;
    int bad_pages = 0;
    int unreadable_pages = 0;
    int noncontiguous_pages = 0;
    int multipart_crossing_pages = 0;
    bool dict_bad = false;
    bool dict_unreadable = false;
    uint64_t span_begin = file_reader->size();
    uint64_t span_end = 0;
    uint64_t previous_end = 0;
    bool have_previous = false;
    int bad_examples = 0;

    std::cout << indent << "Page checksum scan:" << std::endl;
    for (auto iter = ordinal_index->begin(); iter.valid(); iter.next()) {
        const PagePointer& pp = iter.page();
        span_begin = std::min(span_begin, pp.offset);
        span_end = std::max(span_end, pp.offset + pp.size);
        if (have_previous && pp.offset != previous_end) {
            ++noncontiguous_pages;
        }
        previous_end = pp.offset + pp.size;
        have_previous = true;

        bool crosses_part =
                pp.size > 0 && pp.offset / s3_part_size != (pp.offset + pp.size - 1) / s3_part_size;
        if (crosses_part) {
            ++multipart_crossing_pages;
        }

        PageChecksumScanResult page = scan_page_checksum(file_reader, pp);
        if (!page.readable) {
            ++unreadable_pages;
        } else if (page.ok()) {
            ++valid_pages;
        } else {
            ++bad_pages;
        }

        if (iter.page_index() < 2 || crosses_part || (!page.ok() && bad_examples < 3)) {
            std::cout << indent << "  page=" << iter.page_index()
                      << " ordinals=" << iter.first_ordinal() << ".." << iter.last_ordinal()
                      << " offset=" << pp.offset << " size=" << pp.size
                      << " readable=" << (page.readable ? "true" : "false")
                      << " checksum_ok=" << (page.checksum_ok ? "true" : "false")
                      << " actual=" << page.actual_checksum << " expect=" << page.expected_checksum
                      << " footer_size=" << page.footer_size
                      << " footer_ok=" << (page.footer_ok ? "true" : "false")
                      << " crosses_s3_part=" << (crosses_part ? "true" : "false") << std::endl;
            if (!page.ok()) {
                ++bad_examples;
            }
        }
    }

    if (column_meta.has_dict_page()) {
        PagePointer dict_page(column_meta.dict_page());
        PageChecksumScanResult page = scan_page_checksum(file_reader, dict_page);
        std::cout << indent << "  dict offset=" << dict_page.offset << " size=" << dict_page.size
                  << " readable=" << (page.readable ? "true" : "false")
                  << " checksum_ok=" << (page.checksum_ok ? "true" : "false")
                  << " actual=" << page.actual_checksum << " expect=" << page.expected_checksum
                  << " footer_size=" << page.footer_size
                  << " footer_ok=" << (page.footer_ok ? "true" : "false") << std::endl;
        if (!page.readable) {
            dict_unreadable = true;
        } else if (!page.ok()) {
            dict_bad = true;
        }
    }

    std::cout << indent << "  summary pages=" << ordinal_index->num_data_pages()
              << " valid=" << valid_pages << " bad=" << bad_pages
              << " unreadable=" << unreadable_pages << " span=[" << span_begin << "," << span_end
              << ") noncontiguous=" << noncontiguous_pages
              << " crosses_s3_part=" << multipart_crossing_pages << std::endl;

    if (bad_pages > 0 || unreadable_pages > 0 || dict_bad || dict_unreadable) {
        return Status::Corruption(
                "page checksum scan failed for column {}: data_bad={}, data_unreadable={}, "
                "dict_bad={}, dict_unreadable={}",
                column_meta.column_id(), bad_pages, unreadable_pages, dict_bad, dict_unreadable);
    }
    return Status::OK();
}

Status validate_segment_data_options(uint64_t num_segment_rows, uint64_t* rows_to_read) {
    if (FLAGS_rows < -1) {
        return Status::InvalidArgument("rows must be -1 or non-negative, got {}", FLAGS_rows);
    }
    if (FLAGS_batch_rows == 0 || FLAGS_batch_rows > std::numeric_limits<size_t>::max()) {
        return Status::InvalidArgument("batch_rows must be in [1, {}], got {}",
                                       std::numeric_limits<size_t>::max(), FLAGS_batch_rows);
    }
    if (FLAGS_row_start > num_segment_rows) {
        return Status::InvalidArgument("row_start {} exceeds segment row count {}", FLAGS_row_start,
                                       num_segment_rows);
    }

    uint64_t remaining_rows = num_segment_rows - FLAGS_row_start;
    *rows_to_read = FLAGS_rows == -1
                            ? remaining_rows
                            : std::min<uint64_t>(static_cast<uint64_t>(FLAGS_rows), remaining_rows);
    return Status::OK();
}

// Read and print column data values. Keep reader setup and the bounded decode loop together so
// failures retain exact column/row context.
// NOLINTNEXTLINE(readability-function-size)
Status print_column_data_values(const doris::segment_v2::ColumnMetaPB& column_meta,
                                const FileReaderSPtr& file_reader, uint64_t num_segment_rows,
                                uint64_t row_start, uint64_t rows_to_read, int indent_level) {
    std::string indent(indent_level * 2, ' ');

    auto field_type = static_cast<doris::FieldType>(column_meta.type());

    if (!doris::is_scalar_type(field_type)) {
        return Status::NotSupported("cannot read complex column {} as scalar data",
                                    column_meta.column_id());
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
    reader_opts.verify_checksum = FLAGS_verify_checksum;

    std::shared_ptr<ColumnReader> column_reader;
    Status status = ColumnReader::create(reader_opts, column_meta, num_segment_rows, file_reader,
                                         &column_reader);
    if (!status.ok()) {
        status.prepend("failed to create reader for column " +
                       std::to_string(column_meta.column_id()) + ": ");
        return status;
    }

    // Create column iterator
    ColumnIteratorUPtr iterator;
    status = column_reader->new_iterator(&iterator, &tablet_column);
    if (!status.ok()) {
        status.prepend("failed to create iterator for column " +
                       std::to_string(column_meta.column_id()) + ": ");
        return status;
    }

    // Initialize iterator
    ColumnIteratorOptions iter_opts;
    iter_opts.file_reader = file_reader.get();
    doris::OlapReaderStatistics stats; // Dummy statistics
    iter_opts.stats = &stats;

    status = iterator->init(iter_opts);
    if (!status.ok()) {
        status.prepend("failed to initialize iterator for column " +
                       std::to_string(column_meta.column_id()) + ": ");
        return status;
    }

    if (FLAGS_scan_segment_pages) {
        RETURN_IF_ERROR(
                print_column_page_checksums(column_reader, column_meta, file_reader, indent_level));
    }

    if (rows_to_read == 0) {
        if (FLAGS_check_only) {
            std::cout << indent << "Data check: rows=0 range=[" << row_start << "," << row_start
                      << ") batches=0 verify_checksum="
                      << (FLAGS_verify_checksum ? "true" : "false") << " status=OK" << std::endl;
        } else {
            std::cout << indent << "Data Values (rows [" << row_start << "," << row_start << ") of "
                      << num_segment_rows << "):" << std::endl;
        }
        return Status::OK();
    }

    status = iterator->seek_to_ordinal(row_start);
    if (!status.ok()) {
        status.prepend("failed to seek column " + std::to_string(column_meta.column_id()) +
                       " to row " + std::to_string(row_start) + ": ");
        return status;
    }

    auto data_type = doris::DataTypeFactory::instance().create_data_type(column_meta);
    if (!data_type) {
        return Status::InternalError("failed to create data type for column {}, field type {}",
                                     column_meta.column_id(), static_cast<int>(field_type));
    }

    if (!FLAGS_check_only) {
        std::cout << indent << "Data Values (rows [" << row_start << ","
                  << (row_start + rows_to_read) << ") of " << num_segment_rows << "):" << std::endl;
    }

    uint64_t decoded_rows = 0;
    uint64_t batches = 0;
    while (decoded_rows < rows_to_read) {
        uint64_t current_row = row_start + decoded_rows;
        size_t requested_rows = static_cast<size_t>(
                std::min<uint64_t>(FLAGS_batch_rows, rows_to_read - decoded_rows));
        size_t rows_read = requested_rows;
        doris::MutableColumnPtr dst_column = data_type->create_column();

        status = iterator->next_batch(&rows_read, dst_column);
        if (!status.ok()) {
            status.prepend("failed to read column " + std::to_string(column_meta.column_id()) +
                           " at row " + std::to_string(current_row) + ": ");
            return status;
        }
        if (rows_read == 0) {
            return Status::Corruption(
                    "column {} reached an unexpected EOF at row {}, expected range end {}",
                    column_meta.column_id(), current_row, row_start + rows_to_read);
        }
        if (rows_read > requested_rows || dst_column->size() != rows_read) {
            return Status::Corruption(
                    "column {} returned an invalid batch at row {}: requested={}, read={}, "
                    "values={}",
                    column_meta.column_id(), current_row, requested_rows, rows_read,
                    dst_column->size());
        }

        if (!FLAGS_check_only) {
            for (size_t i = 0; i < rows_read; ++i) {
                std::cout << indent << "  [" << (current_row + i) << "] ";
                if (column_meta.is_nullable()) {
                    const auto& nullable_col =
                            assert_cast<const doris::ColumnNullable&>(*dst_column);
                    if (nullable_col.is_null_at(i)) {
                        std::cout << "NULL";
                    } else {
                        const doris::IColumn& nested_col = nullable_col.get_nested_column();
                        std::cout << format_column_value(nested_col, i, field_type);
                    }
                } else {
                    std::cout << format_column_value(*dst_column, i, field_type);
                }
                std::cout << std::endl;
            }
        }

        decoded_rows += rows_read;
        ++batches;
    }

    if (FLAGS_check_only) {
        std::cout << indent << "Data check: rows=" << decoded_rows << " range=[" << row_start << ","
                  << (row_start + decoded_rows) << ") batches=" << batches
                  << " verify_checksum=" << (FLAGS_verify_checksum ? "true" : "false")
                  << " status=OK" << std::endl;
    } else if (rows_to_read < num_segment_rows) {
        std::cout << indent << "  ... (" << (num_segment_rows - rows_to_read)
                  << " rows outside selected range)" << std::endl;
    }

    return Status::OK();
}

// Helper function to print column metadata. The output intentionally mirrors protobuf field order
// for forensic readability.
// NOLINTNEXTLINE(readability-function-size)
Status print_column_meta(const doris::segment_v2::ColumnMetaPB& column_meta,
                         const FileReaderSPtr& file_reader, uint64_t num_segment_rows,
                         uint64_t row_start, uint64_t rows_to_read, int indent_level) {
    std::string indent(indent_level * 2, ' ');
    std::string column_name;
    if (column_meta.has_column_path_info() && column_meta.column_path_info().has_path()) {
        column_name = column_meta.column_path_info().path();
    } else {
        column_name = "column_id_" + std::to_string(column_meta.column_id());
    }

    auto field_type = static_cast<doris::FieldType>(column_meta.type());
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
            if (i > 0) {
                std::cout << ", ";
            }
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
        if (FLAGS_check_only) {
            return Status::NotSupported(
                    "check_only does not yet support complex column {} with {} children",
                    column_meta.column_id(), column_meta.children_columns_size());
        }
        std::cout << indent << "Sub-columns: " << column_meta.children_columns_size() << std::endl;
        for (int i = 0; i < column_meta.children_columns_size(); ++i) {
            RETURN_IF_ERROR(print_column_meta(column_meta.children_columns(i), file_reader,
                                              num_segment_rows, row_start, rows_to_read,
                                              indent_level + 1));
        }
        return Status::OK();
    }

    // Print column data values for scalar types
    if (doris::is_scalar_type(field_type)) {
        return print_column_data_values(column_meta, file_reader, num_segment_rows, row_start,
                                        rows_to_read, indent_level);
    } else {
        return Status::NotSupported("cannot display values for column {} with type {}",
                                    column_meta.column_id(), get_field_type_string(field_type));
    }
}

// Register hijacked accessors
ACCESS_PRIVATE_FIELD(ExecEnv_encoding_info_resolver, doris::ExecEnv,
                     doris::segment_v2::EncodingInfoResolver*, _encoding_info_resolver);
ACCESS_PRIVATE_FIELD(ExecEnv_orphan_mem_tracker, doris::ExecEnv,
                     std::shared_ptr<doris::MemTrackerLimiter>, _orphan_mem_tracker);
ACCESS_PRIVATE_STATIC_FIELD(ExecEnv_tracking_memory, doris::ExecEnv, std::atomic_bool,
                            _s_tracking_memory);

// Keep report sections in execution order so a failure never prints a misleading final summary.
// NOLINTNEXTLINE(readability-function-size)
Status show_segment_data(const std::string& file_name) {
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
        tracking_memory->store(true, std::memory_order_release);
        exec_env->*mem_tracker = doris::MemTrackerLimiter::create_shared(
                doris::MemTrackerLimiter::Type::GLOBAL, "Orphan");
    }

    doris::io::FileReaderSPtr file_reader;
    Status status = doris::io::global_local_filesystem()->open_file(file_name, &file_reader);
    if (!status.ok()) {
        status.prepend("failed to open segment file " + file_name + ": ");
        return status;
    }

    SegmentFooterPB footer;
    status = get_segment_footer(file_reader.get(), &footer);
    if (!status.ok()) {
        status.prepend("failed to read segment footer from " + file_name + ": ");
        return status;
    }

    uint64_t rows_to_read = 0;
    RETURN_IF_ERROR(validate_segment_data_options(footer.num_rows(), &rows_to_read));

    // Print basic info
    std::cout << "\n=== Segment File Info ===" << std::endl;
    std::cout << "File: " << file_name << std::endl;
    std::cout << "Num Rows: " << footer.num_rows() << std::endl;
    std::cout << "Num Columns: " << footer.columns_size() << std::endl;
    std::cout << "Compression: " << get_compression_string(footer.compress_type()) << std::endl;
    std::cout << "Selected Row Range: [" << FLAGS_row_start << ","
              << (FLAGS_row_start + rows_to_read) << ")" << std::endl;
    std::cout << "Check Only: " << (FLAGS_check_only ? "true" : "false") << std::endl;
    std::cout << "Verify Checksum: " << (FLAGS_verify_checksum ? "true" : "false") << std::endl;
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
        RETURN_IF_ERROR(print_column_meta(column_meta, file_reader, footer.num_rows(),
                                          FLAGS_row_start, rows_to_read, 0));

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

    std::cout << "\n=== Data Read Summary ===" << std::endl;
    std::cout << "Columns Checked: " << footer.columns_size() << std::endl;
    std::cout << "Rows Per Column: " << rows_to_read << std::endl;
    std::cout << "Row Range: [" << FLAGS_row_start << "," << (FLAGS_row_start + rows_to_read) << ")"
              << std::endl;
    std::cout << "Check Only: " << (FLAGS_check_only ? "true" : "false") << std::endl;
    std::cout << "Verify Checksum: " << (FLAGS_verify_checksum ? "true" : "false") << std::endl;
    std::cout << "Status: OK" << std::endl;
    return Status::OK();
}

void init_common_components() {
    // init meta_tool.log to current dir
    if (doris::config::sys_log_dir == "") {
        doris::config::sys_log_dir = ".";
    }
    if (doris::config::sys_log_level == "") {
        doris::config::sys_log_level = "INFO";
    }
    if (doris::config::sys_log_roll_mode == "") {
        doris::config::sys_log_roll_mode = "SIZE-MB-1024";
    }
    FLAGS_log_dir = doris::config::sys_log_dir;
    if (!doris::init_glog("meta_tool")) {
        fprintf(stderr, "init glog failed.\n");
    }

    doris::ExecEnv::GetInstance()->init_mem_tracker();
    doris::ExecEnv::GetInstance()->set_cache_manager(doris::CacheManager::create_global_instance());
    doris::ExecEnv::GetInstance()->set_tablet_schema_cache(
            doris::TabletSchemaCache::create_global_schema_cache(
                    doris::config::tablet_schema_cache_capacity));
    doris::ExecEnv::GetInstance()->set_tablet_column_object_pool(
            doris::TabletColumnObjectPool::create_global_column_cache(
                    doris::config::tablet_schema_cache_capacity));
}

void gen_empty_segment() {
    std::string output_path = FLAGS_output_path.empty() ? "." : FLAGS_output_path;

    // Create output file path
    std::string file_path = output_path + "/empty.dat";

    // Open file for writing
    std::ofstream out_file(file_path, std::ios::binary);
    if (!out_file.is_open()) {
        std::cout << "failed to open output file: " << file_path << std::endl;
        return;
    }

    // 1. Build empty short key index page
    std::vector<Slice> index_body;
    segment_v2::PageFooterPB index_footer;
    index_footer.set_type(segment_v2::SHORT_KEY_PAGE);
    index_footer.set_uncompressed_size(0); // empty body

    segment_v2::ShortKeyFooterPB* sk_footer = index_footer.mutable_short_key_page_footer();
    sk_footer->set_num_items(0);    // 0 keys
    sk_footer->set_key_bytes(0);    // empty key buffer
    sk_footer->set_offset_bytes(0); // empty offset buffer
    sk_footer->set_segment_id(0);
    sk_footer->set_num_rows_per_block(FLAGS_num_rows_per_block);
    sk_footer->set_num_segment_rows(0);

    // Empty key and offset buffers
    std::string key_buf;
    std::string offset_buf;

    index_body.push_back(Slice(key_buf.data(), key_buf.size()));
    index_body.push_back(Slice(offset_buf.data(), offset_buf.size()));

    // Serialize index footer
    std::string index_footer_buf;
    index_footer.SerializeToString(&index_footer_buf);
    doris::put_fixed32_le(&index_footer_buf, static_cast<uint32_t>(index_footer_buf.size()));
    index_body.push_back(Slice(index_footer_buf.data(), index_footer_buf.size()));

    // Calculate checksum for index page
    uint32_t index_checksum = 0;
    for (const auto& slice : index_body) {
        index_checksum = crc32c::Extend(index_checksum, (const uint8_t*)slice.data, slice.size);
    }
    uint8_t index_checksum_buf[sizeof(uint32_t)];
    doris::encode_fixed32_le(index_checksum_buf, index_checksum);
    index_body.push_back(Slice(index_checksum_buf, sizeof(uint32_t)));

    // 2. Build segment footer
    SegmentFooterPB footer;
    footer.set_num_rows(0);

    // Calculate total index page size
    uint64_t index_page_size = 0;
    for (const auto& slice : index_body) {
        index_page_size += slice.size;
    }

    // Set short key index page pointer
    segment_v2::PagePointer index_pp;
    index_pp.offset = 0;
    index_pp.size = static_cast<uint32_t>(index_page_size);
    index_pp.to_proto(footer.mutable_short_key_index_page());

    // Serialize footer
    std::string footer_buf;
    if (!footer.SerializeToString(&footer_buf)) {
        std::cout << "failed to serialize footer" << std::endl;
        return;
    }

    // 3. Write footer data to file
    std::vector<Slice> footer_slices = {footer_buf};

    // Footer size (4 bytes, little-endian)
    uint32_t footer_size = static_cast<uint32_t>(footer_buf.size());
    uint8_t footer_size_buf[4];
    doris::encode_fixed32_le(footer_size_buf, footer_size);
    footer_slices.push_back(Slice(footer_size_buf, 4));

    // Footer checksum (4 bytes, crc32c)
    uint32_t footer_checksum = crc32c::Crc32c(footer_buf.data(), footer_buf.size());
    uint8_t footer_checksum_buf[4];
    doris::encode_fixed32_le(footer_checksum_buf, footer_checksum);
    footer_slices.push_back(Slice(footer_checksum_buf, 4));

    // Magic number (4 bytes): "D0R1"
    footer_slices.push_back(
            Slice(doris::segment_v2::k_segment_magic, doris::segment_v2::k_segment_magic_length));

    // Write index page first, then footer
    for (const auto& slice : index_body) {
        out_file.write(slice.data, slice.size);
    }

    // Write footer
    for (const auto& slice : footer_slices) {
        out_file.write(slice.data, slice.size);
    }

    out_file.close();

    // Print summary
    std::cout << "Generated empty segment file: " << file_path << std::endl;
    std::cout << "  - Index page size: " << index_page_size << " bytes" << std::endl;
    std::cout << "  - Footer size: " << footer_slices.size() << " slices, "
              << (footer_buf.size() + 12) << " bytes" << std::endl;
    std::cout << "  - Total file size: " << (index_page_size + footer_buf.size() + 12) << " bytes"
              << std::endl;
    std::cout << "  - num_rows: 0" << std::endl;
}

int main(int argc, char** argv) {
    SCOPED_INIT_THREAD_CONTEXT();
    std::string usage = get_usage(argv[0]);
    gflags::SetUsageMessage(usage);
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_operation == "show_meta") {
        init_common_components();
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

        init_common_components();
        batch_delete_meta(tablet_file);
    } else if (FLAGS_operation == "show_segment_footer") {
        if (FLAGS_file == "") {
            std::cout << "no file flag for show dict" << std::endl;
            return -1;
        }
        init_common_components();
        show_segment_footer(FLAGS_file);
    } else if (FLAGS_operation == "show_segment_data") {
        if (FLAGS_file == "") {
            std::cerr << "no file flag for show_segment_data" << std::endl;
            return 2;
        }
        init_common_components();
        Status status = show_segment_data(FLAGS_file);
        if (!status.ok()) {
            std::cerr << "show_segment_data failed: " << status.to_string() << std::endl;
            gflags::ShutDownCommandLineFlags();
            return status.is<ErrorCode::INVALID_ARGUMENT>() ? 2 : 1;
        }
    } else if (FLAGS_operation == "check_page_crc" || FLAGS_operation == "scan_page_crc") {
        if (FLAGS_file.empty()) {
            std::cerr << "no file flag for " << FLAGS_operation << std::endl;
            return 2;
        }
        init_common_components();
        Status status = FLAGS_operation == "check_page_crc" ? check_page_crc_ranges()
                                                            : scan_page_crc_range();
        if (!status.ok()) {
            std::cerr << FLAGS_operation << " failed: " << status.to_string() << std::endl;
            gflags::ShutDownCommandLineFlags();
            return status.is<ErrorCode::INVALID_ARGUMENT>() ? 2 : 1;
        }
    } else if (FLAGS_operation == "gen_empty_segment") {
        gen_empty_segment();
    } else {
        // operations that need root path should be written here
        std::set<std::string> valid_operations = {"get_meta", "load_meta", "delete_meta"};
        if (valid_operations.find(FLAGS_operation) == valid_operations.end()) {
            std::cout << "invalid operation:" << FLAGS_operation << std::endl;
            return -1;
        }

        if (getenv("DORIS_HOME") == nullptr) {
            fprintf(stderr, "you need set DORIS_HOME environment variable.\n");
            exit(-1);
        }

        std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
        if (!doris::config::init(conffile.c_str(), true, true, true)) {
            fprintf(stderr, "error read config file. \n");
            return -1;
        }

        std::string custom_conffile = doris::config::custom_config_dir + "/be_custom.conf";
        if (!doris::config::init(custom_conffile.c_str(), true, false, false)) {
            fprintf(stderr, "error read custom config file. \n");
            return -1;
        }

        init_common_components();

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
