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

#include "format/table/iceberg_delete_file_reader_helper.h"

#include <fmt/format.h>
#include <parallel_hashmap/phmap.h>

#include <cstring>
#include <memory>
#include <stdexcept>
#include <unordered_map>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_dictionary.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exec/common/endian.h"
#include "format/orc/vorc_reader.h"
#include "format/parquet/vparquet_column_chunk_reader.h"
#include "format/parquet/vparquet_reader.h"
#include "format/table/deletion_vector_reader.h"
#include "format/table/table_format_reader.h"
#include "io/hdfs_builder.h"
#include "runtime/runtime_state.h"
#include "storage/predicate/column_predicate.h"
#include "util/debug_points.h"
#include "util/hash_util.hpp"

namespace doris {

namespace {

constexpr const char* ICEBERG_FILE_PATH = "file_path";
constexpr const char* ICEBERG_ROW_POS = "pos";
constexpr size_t ICEBERG_DELETION_VECTOR_MIN_BYTES = 12;

const std::vector<std::string> DELETE_COL_NAMES {ICEBERG_FILE_PATH, ICEBERG_ROW_POS};
std::unordered_map<std::string, uint32_t> DELETE_COL_NAME_TO_BLOCK_IDX = {{ICEBERG_FILE_PATH, 0},
                                                                          {ICEBERG_ROW_POS, 1}};

Status validate_position_delete_file_format(const TIcebergDeleteFileDesc& delete_file,
                                            TFileFormatType::type* file_format) {
    if (file_format == nullptr) {
        return Status::InvalidArgument("position delete file format output is null");
    }
    if (!delete_file.__isset.file_format) {
        return Status::InternalError("Iceberg position delete file is missing file format");
    }
    if (delete_file.file_format != TFileFormatType::FORMAT_PARQUET &&
        delete_file.file_format != TFileFormatType::FORMAT_ORC) {
        return Status::NotSupported("Unsupported Iceberg delete file format {}",
                                    delete_file.file_format);
    }
    *file_format = delete_file.file_format;
    return Status::OK();
}

Status visit_position_delete_block(const Block& block, size_t read_rows,
                                   IcebergPositionDeleteVisitor* visitor) {
    if (visitor == nullptr) {
        return Status::InvalidArgument("position delete visitor is null");
    }
    if (read_rows == 0) {
        return Status::OK();
    }

    auto name_to_pos_map = block.get_name_to_pos_map();
    auto path_it = name_to_pos_map.find(ICEBERG_FILE_PATH);
    auto pos_it = name_to_pos_map.find(ICEBERG_ROW_POS);
    if (path_it == name_to_pos_map.end() || pos_it == name_to_pos_map.end()) {
        return Status::InternalError("Position delete block is missing required columns");
    }

    ColumnPtr path_column_ptr = block.get_by_position(path_it->second).column;
    ColumnPtr pos_column_ptr = block.get_by_position(pos_it->second).column;
    if (const auto* nullable_col = check_and_get_column<ColumnNullable>(*path_column_ptr);
        nullable_col != nullptr) {
        if (nullable_col->has_null(0, read_rows)) {
            return Status::Corruption(
                    "Iceberg position delete column file_path contains null values");
        }
        path_column_ptr = remove_nullable(path_column_ptr);
    }
    if (const auto* nullable_col = check_and_get_column<ColumnNullable>(*pos_column_ptr);
        nullable_col != nullptr) {
        if (nullable_col->has_null(0, read_rows)) {
            return Status::Corruption("Iceberg position delete column pos contains null values");
        }
        pos_column_ptr = remove_nullable(pos_column_ptr);
    }

    const auto* pos_column = assert_cast<const ColumnInt64*>(pos_column_ptr.get());
    const auto* path_column = path_column_ptr.get();

    if (const auto* string_column = check_and_get_column<ColumnString>(path_column);
        string_column != nullptr) {
        for (size_t i = 0; i < read_rows; ++i) {
            RETURN_IF_ERROR(visitor->visit(string_column->get_data_at(i).to_string(),
                                           pos_column->get_element(i)));
        }
        return Status::OK();
    }

    if (const auto* dict_column = check_and_get_column<ColumnDictI32>(path_column);
        dict_column != nullptr) {
        const auto& codes = dict_column->get_data();
        for (size_t i = 0; i < read_rows; ++i) {
            RETURN_IF_ERROR(visitor->visit(dict_column->get_value(codes[i]).to_string(),
                                           pos_column->get_element(i)));
        }
        return Status::OK();
    }

    return Status::InternalError("Unsupported file_path column type in position delete block");
}

Status init_parquet_delete_reader(ParquetReader* reader) {
    if (reader == nullptr) {
        return Status::InvalidArgument("invalid parquet delete reader arguments");
    }

    VExprContextSPtrs conjuncts;
    phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>> slot_id_to_predicates;
    RETURN_IF_ERROR(reader->init_reader(DELETE_COL_NAMES, &DELETE_COL_NAME_TO_BLOCK_IDX, conjuncts,
                                        slot_id_to_predicates, nullptr, nullptr, nullptr, nullptr,
                                        nullptr, TableSchemaChangeHelper::ConstNode::get_instance(),
                                        false));

    return Status::OK();
}

Status init_orc_delete_reader(OrcReader* reader) {
    if (reader == nullptr) {
        return Status::InvalidArgument("orc delete reader is null");
    }

    VExprContextSPtrs conjuncts;
    RETURN_IF_ERROR(reader->init_reader(&DELETE_COL_NAMES, &DELETE_COL_NAME_TO_BLOCK_IDX, conjuncts,
                                        false, nullptr, nullptr, nullptr, nullptr,
                                        TableSchemaChangeHelper::ConstNode::get_instance()));
    return Status::OK();
}

Status decode_deletion_vector_buffer(const char* buf, size_t buffer_size,
                                     roaring::Roaring64Map* rows_to_delete) {
    if (buf == nullptr || rows_to_delete == nullptr) {
        return Status::InvalidArgument("invalid deletion vector decode arguments");
    }
    if (buffer_size < ICEBERG_DELETION_VECTOR_MIN_BYTES) {
        return Status::DataQualityError("Deletion vector file size too small: {}", buffer_size);
    }

    const uint32_t total_length = BigEndian::Load32(buf);
    if (static_cast<uint64_t>(total_length) + 8 != buffer_size) {
        return Status::DataQualityError("Deletion vector length mismatch, expected: {}, actual: {}",
                                        static_cast<uint64_t>(total_length) + 8, buffer_size);
    }

    constexpr static char MAGIC_NUMBER[] = {'\xD1', '\xD3', '\x39', '\x64'};
    if (memcmp(buf + sizeof(total_length), MAGIC_NUMBER, 4) != 0) {
        return Status::DataQualityError("Deletion vector magic number mismatch");
    }

    const uint32_t expected_crc = BigEndian::Load32(buf + sizeof(total_length) + total_length);
    const uint32_t actual_crc =
            HashUtil::zlib_crc_hash(buf + sizeof(total_length), total_length, 0);
    if (actual_crc != expected_crc) {
        return Status::DataQualityError("Deletion vector CRC32 mismatch, expected: {}, actual: {}",
                                        expected_crc, actual_crc);
    }

    try {
        *rows_to_delete |= roaring::Roaring64Map::readSafe(
                buf + 8, buffer_size - ICEBERG_DELETION_VECTOR_MIN_BYTES);
    } catch (const std::runtime_error& e) {
        return Status::DataQualityError("Decode roaring bitmap failed, {}", e.what());
    }
    return Status::OK();
}

} // namespace

IcebergDeleteFileIOContext::IcebergDeleteFileIOContext(RuntimeState* state) {
    io_ctx.file_cache_stats = &file_cache_stats;
    io_ctx.file_reader_stats = &file_reader_stats;
    if (state != nullptr) {
        // branch-4.1 has no shared FileScanIOContext helper; keep delete-file reads attributed to
        // the parent query and classified identically to ordinary external scans.
        io_ctx.query_id = &state->query_id();
        if (state->query_options().query_type == TQueryType::SELECT) {
            io_ctx.reader_type = ReaderType::READER_QUERY;
        }
    }
}

TFileScanRangeParams build_iceberg_delete_scan_range_params(
        const std::map<std::string, std::string>& hadoop_conf, TFileType::type file_type,
        const std::vector<TNetworkAddress>& broker_addresses) {
    TFileScanRangeParams params;
    params.__set_file_type(file_type);
    params.__set_properties(hadoop_conf);
    if (file_type == TFileType::FILE_HDFS) {
        params.__set_hdfs_params(parse_properties(hadoop_conf));
    }
    if (!broker_addresses.empty()) {
        params.__set_broker_addresses(broker_addresses);
    }
    return params;
}

TFileRangeDesc build_iceberg_delete_file_range(const std::string& path) {
    TFileRangeDesc range;
    range.path = path;
    range.start_offset = 0;
    range.size = -1;
    range.file_size = -1;
    return range;
}

bool is_iceberg_deletion_vector(const TIcebergDeleteFileDesc& delete_file) {
    return delete_file.__isset.content && delete_file.content == 3;
}

std::string build_iceberg_deletion_vector_cache_key(const std::string& data_file_path,
                                                    const TIcebergDeleteFileDesc& delete_file) {
    return fmt::format("delete_dv_{}:{}{}:{}#{}#{}", data_file_path.size(), data_file_path,
                       delete_file.path.size(), delete_file.path, delete_file.content_offset,
                       delete_file.content_size_in_bytes);
}

Status validate_iceberg_deletion_vector_descriptor(const TIcebergDeleteFileDesc& delete_file,
                                                   size_t& bytes_read) {
    if (!delete_file.__isset.path || !delete_file.__isset.content_offset ||
        !delete_file.__isset.content_size_in_bytes) {
        return Status::DataQualityError(
                "Iceberg deletion vector descriptor misses "
                "path/content_offset/content_size_in_bytes");
    }
    return validate_iceberg_deletion_vector_read_range(
            delete_file.content_offset, delete_file.content_size_in_bytes, bytes_read);
}

Status read_iceberg_position_delete_file(const TIcebergDeleteFileDesc& delete_file,
                                         const IcebergDeleteFileReaderOptions& options,
                                         IcebergPositionDeleteVisitor* visitor) {
    if (options.state == nullptr || options.profile == nullptr || options.scan_params == nullptr ||
        options.io_ctx == nullptr || visitor == nullptr) {
        return Status::InvalidArgument("invalid position delete reader options");
    }

    TFileRangeDesc delete_range = build_iceberg_delete_file_range(delete_file.path);
    if (options.fs_name != nullptr && !options.fs_name->empty()) {
        delete_range.__set_fs_name(*options.fs_name);
    }

    TFileFormatType::type file_format;
    RETURN_IF_ERROR(validate_position_delete_file_format(delete_file, &file_format));

    if (file_format == TFileFormatType::FORMAT_PARQUET) {
        ParquetReader reader(options.profile, *options.scan_params, delete_range,
                             options.batch_size,
                             const_cast<cctz::time_zone*>(&options.state->timezone_obj()),
                             options.io_ctx, options.state, options.meta_cache);
        RETURN_IF_ERROR(init_parquet_delete_reader(&reader));

        bool eof = false;
        while (!eof) {
            Block block;
            // branch-4.1 cannot safely nest ColumnDictI32 in ColumnNullable. Decode paths to
            // strings so optional delete columns keep their null map for corruption checks.
            block.insert(ColumnWithTypeAndName(make_nullable(std::make_shared<DataTypeString>()),
                                               ICEBERG_FILE_PATH));
            block.insert(ColumnWithTypeAndName(make_nullable(std::make_shared<DataTypeInt64>()),
                                               ICEBERG_ROW_POS));
            size_t read_rows = 0;
            RETURN_IF_ERROR(reader.get_next_block(&block, &read_rows, &eof));
            RETURN_IF_ERROR(visit_position_delete_block(block, read_rows, visitor));
        }
        return Status::OK();
    }

    if (file_format == TFileFormatType::FORMAT_ORC) {
        OrcReader reader(options.profile, options.state, *options.scan_params, delete_range,
                         options.batch_size, options.state->timezone(), options.io_ctx,
                         options.meta_cache);
        RETURN_IF_ERROR(init_orc_delete_reader(&reader));

        bool eof = false;
        while (!eof) {
            Block block;
            block.insert(ColumnWithTypeAndName(
                    ColumnString::create(), std::make_shared<DataTypeString>(), ICEBERG_FILE_PATH));
            block.insert(ColumnWithTypeAndName(ColumnInt64::create(),
                                               std::make_shared<DataTypeInt64>(), ICEBERG_ROW_POS));
            size_t read_rows = 0;
            RETURN_IF_ERROR(reader.get_next_block(&block, &read_rows, &eof));
            RETURN_IF_ERROR(visit_position_delete_block(block, read_rows, visitor));
        }
        return Status::OK();
    }

    return Status::NotSupported("Unsupported Iceberg delete file format {}", file_format);
}

Status read_iceberg_deletion_vector(const TIcebergDeleteFileDesc& delete_file,
                                    const IcebergDeleteFileReaderOptions& options,
                                    DeletionVector* rows_to_delete) {
    if (options.state == nullptr || options.profile == nullptr || options.scan_params == nullptr ||
        options.io_ctx == nullptr || rows_to_delete == nullptr) {
        return Status::InvalidArgument("invalid deletion vector reader options");
    }
    size_t bytes_read = 0;
    RETURN_IF_ERROR(validate_iceberg_deletion_vector_descriptor(delete_file, bytes_read));
    DBUG_EXECUTE_IF("IcebergDeleteFileReader.read_deletion_vector.io_error",
                    { return Status::IOError("injected Iceberg deletion vector read failure"); });
    DBUG_EXECUTE_IF("IcebergDeleteFileReader.read_deletion_vector.should_stop",
                    { return Status::EndOfFile("stop read."); });

    TFileRangeDesc delete_range = build_iceberg_delete_file_range(delete_file.path);
    if (options.fs_name != nullptr && !options.fs_name->empty()) {
        delete_range.__set_fs_name(*options.fs_name);
    }
    delete_range.start_offset = delete_file.content_offset;
    delete_range.size = delete_file.content_size_in_bytes;

    // Iceberg v3 deletion-vector-v1 blobs are uncompressed and metadata provides the exact range.
    // Parse the Puffin footer first if future blob types or compression codecs are supported.
    DeletionVectorReader dv_reader(options.state, options.profile, *options.scan_params,
                                   delete_range, options.io_ctx);
    RETURN_IF_ERROR(dv_reader.open());

    std::vector<char> buf(bytes_read);
    const auto read_status = dv_reader.read_at(delete_range.start_offset, {buf.data(), bytes_read});
    if (options.deletion_vector_file_cache_stats != nullptr) {
        options.deletion_vector_file_cache_stats->merge_from(dv_reader.file_cache_statistics());
    }
    RETURN_IF_ERROR(read_status);
    return decode_deletion_vector_buffer(buf.data(), bytes_read, rows_to_delete);
}

Status decode_iceberg_deletion_vector_buffer(const char* buf, size_t buffer_size,
                                             DeletionVector* rows_to_delete) {
    return decode_deletion_vector_buffer(buf, buffer_size, rows_to_delete);
}

} // namespace doris
