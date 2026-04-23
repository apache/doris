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

#include <gen_cpp/parquet_types.h>
#include <parallel_hashmap/phmap.h>

#include <cstring>
#include <memory>
#include <stdexcept>
#include <unordered_map>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_dictionary.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exec/common/endian.h"
#include "format/orc/vorc_reader.h"
#include "format/parquet/vparquet_column_chunk_reader.h"
#include "format/parquet/vparquet_reader.h"
#include "format/table/deletion_vector_reader.h"
#include "format/table/iceberg_reader.h"
#include "format/table/table_format_reader.h"
#include "io/hdfs_builder.h"
#include "runtime/runtime_state.h"
#include "storage/predicate/column_predicate.h"

namespace doris {

namespace {

constexpr const char* ICEBERG_FILE_PATH = "file_path";
constexpr const char* ICEBERG_ROW_POS = "pos";

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

    const auto* pos_column =
            assert_cast<const ColumnInt64*>(block.get_by_position(pos_it->second).column.get());
    const auto* path_column = block.get_by_position(path_it->second).column.get();

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

Status init_parquet_delete_reader(ParquetReader* reader, bool* dictionary_coded) {
    if (reader == nullptr || dictionary_coded == nullptr) {
        return Status::InvalidArgument("invalid parquet delete reader arguments");
    }

    ParquetInitContext ctx;
    ctx.column_names = DELETE_COL_NAMES;
    ctx.col_name_to_block_idx = &DELETE_COL_NAME_TO_BLOCK_IDX;
    ctx.filter_groups = false;
    RETURN_IF_ERROR(reader->init_reader(&ctx));

    const tparquet::FileMetaData* meta_data = reader->get_meta_data();
    *dictionary_coded = true;
    for (const auto& row_group : meta_data->row_groups) {
        const auto& column_chunk = row_group.columns[0];
        if (!(column_chunk.__isset.meta_data &&
              IcebergTableReader::_is_fully_dictionary_encoded(column_chunk.meta_data))) {
            *dictionary_coded = false;
            break;
        }
    }
    return Status::OK();
}

Status init_orc_delete_reader(OrcReader* reader) {
    if (reader == nullptr) {
        return Status::InvalidArgument("orc delete reader is null");
    }

    OrcInitContext ctx;
    ctx.column_names = DELETE_COL_NAMES;
    ctx.col_name_to_block_idx = &DELETE_COL_NAME_TO_BLOCK_IDX;
    RETURN_IF_ERROR(reader->init_reader(&ctx));
    return Status::OK();
}

Status decode_deletion_vector_buffer(const char* buf, size_t buffer_size,
                                     roaring::Roaring64Map* rows_to_delete) {
    if (buf == nullptr || rows_to_delete == nullptr) {
        return Status::InvalidArgument("invalid deletion vector decode arguments");
    }
    if (buffer_size < 12) {
        return Status::DataQualityError("Deletion vector file size too small: {}", buffer_size);
    }

    auto total_length = BigEndian::Load32(buf);
    if (total_length + 8 != buffer_size) {
        return Status::DataQualityError("Deletion vector length mismatch, expected: {}, actual: {}",
                                        total_length + 8, buffer_size);
    }

    constexpr static char MAGIC_NUMBER[] = {'\xD1', '\xD3', '\x39', '\x64'};
    if (memcmp(buf + sizeof(total_length), MAGIC_NUMBER, 4) != 0) {
        return Status::DataQualityError("Deletion vector magic number mismatch");
    }

    try {
        *rows_to_delete |= roaring::Roaring64Map::readSafe(buf + 8, buffer_size - 12);
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
        io_ctx.query_id = &state->query_id();
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
                             options.batch_size, &options.state->timezone_obj(), options.io_ctx,
                             options.state, options.meta_cache);
        bool dictionary_coded = false;
        RETURN_IF_ERROR(init_parquet_delete_reader(&reader, &dictionary_coded));

        bool eof = false;
        while (!eof) {
            Block block;
            if (dictionary_coded) {
                block.insert(ColumnWithTypeAndName(
                        ColumnDictI32::create(FieldType::OLAP_FIELD_TYPE_VARCHAR),
                        std::make_shared<DataTypeString>(), ICEBERG_FILE_PATH));
            } else {
                block.insert(ColumnWithTypeAndName(ColumnString::create(),
                                                   std::make_shared<DataTypeString>(),
                                                   ICEBERG_FILE_PATH));
            }
            block.insert(ColumnWithTypeAndName(ColumnInt64::create(),
                                               std::make_shared<DataTypeInt64>(), ICEBERG_ROW_POS));
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
                                    roaring::Roaring64Map* rows_to_delete) {
    if (options.state == nullptr || options.profile == nullptr || options.scan_params == nullptr ||
        options.io_ctx == nullptr || rows_to_delete == nullptr) {
        return Status::InvalidArgument("invalid deletion vector reader options");
    }
    if (!delete_file.__isset.content_offset || !delete_file.__isset.content_size_in_bytes) {
        return Status::InternalError("Deletion vector is missing content offset or length");
    }

    TFileRangeDesc delete_range = build_iceberg_delete_file_range(delete_file.path);
    if (options.fs_name != nullptr && !options.fs_name->empty()) {
        delete_range.__set_fs_name(*options.fs_name);
    }
    delete_range.start_offset = delete_file.content_offset;
    delete_range.size = delete_file.content_size_in_bytes;

    DeletionVectorReader dv_reader(options.state, options.profile, *options.scan_params,
                                   delete_range, options.io_ctx);
    RETURN_IF_ERROR(dv_reader.open());

    std::vector<char> buf(delete_range.size);
    RETURN_IF_ERROR(dv_reader.read_at(delete_range.start_offset,
                                      {buf.data(), cast_set<size_t>(delete_range.size)}));
    return decode_deletion_vector_buffer(buf.data(), delete_range.size, rows_to_delete);
}

} // namespace doris
