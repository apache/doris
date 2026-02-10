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

#include "parquet_predicate.h"

#include <glog/logging.h>

#include "olap/column_predicate.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
Status ParquetPredicate::read_column_stats(
        const FieldSchema* col_schema, const tparquet::ColumnMetaData& column_meta_data,
        std::unordered_map<tparquet::Type::type, bool>* ignored_stats,
        const std::string& file_created_by, ZoneMapInfo* ans_stat) {
    auto& statistic = column_meta_data.statistics;

    std::string min_string;
    std::string max_string;
    if (!statistic.__isset.null_count) [[unlikely]] {
        return Status::DataQualityError("This parquet Column meta no set null_count.");
    }
    ans_stat->has_null = statistic.null_count > 0;
    ans_stat->is_all_null = statistic.null_count == column_meta_data.num_values;
    if (ans_stat->is_all_null) {
        return Status::OK();
    }
    auto prim_type = remove_nullable(col_schema->data_type)->get_primitive_type();

    // Min-max of statistic is plain-encoded value
    if (statistic.__isset.min_value && statistic.__isset.max_value) {
        ColumnOrderName column_order =
                col_schema->physical_type == tparquet::Type::INT96 ||
                                col_schema->parquet_schema.logicalType.__isset.UNKNOWN
                        ? ColumnOrderName::UNDEFINED
                        : ColumnOrderName::TYPE_DEFINED_ORDER;
        if ((statistic.min_value != statistic.max_value) &&
            (column_order != ColumnOrderName::TYPE_DEFINED_ORDER)) {
            return Status::DataQualityError("Can not use this parquet min/max value.");
        }
        min_string = statistic.min_value;
        max_string = statistic.max_value;

        if (prim_type == TYPE_VARCHAR || prim_type == TYPE_CHAR || prim_type == TYPE_STRING) {
            auto encoded_min_copy = min_string;
            auto encoded_max_copy = max_string;
            if (!_try_read_old_utf8_stats(encoded_min_copy, encoded_max_copy)) {
                return Status::DataQualityError("Can not use this parquet min/max value.");
            }
            min_string = encoded_min_copy;
            max_string = encoded_max_copy;
        }

    } else if (statistic.__isset.min && statistic.__isset.max) {
        bool max_equals_min = statistic.min == statistic.max;

        SortOrder sort_order = _determine_sort_order(col_schema->parquet_schema);
        bool sort_orders_match = SortOrder::SIGNED == sort_order;
        if (!sort_orders_match && !max_equals_min) {
            return Status::NotSupported("Can not use this parquet min/max value.");
        }

        bool should_ignore_corrupted_stats = false;
        if (ignored_stats != nullptr) {
            if (ignored_stats->count(col_schema->physical_type) == 0) {
                if (CorruptStatistics::should_ignore_statistics(file_created_by,
                                                                col_schema->physical_type)) {
                    ignored_stats->emplace(col_schema->physical_type, true);
                    should_ignore_corrupted_stats = true;
                } else {
                    ignored_stats->emplace(col_schema->physical_type, false);
                }
            } else if (ignored_stats->at(col_schema->physical_type)) {
                should_ignore_corrupted_stats = true;
            }
        } else if (CorruptStatistics::should_ignore_statistics(file_created_by,
                                                               col_schema->physical_type)) {
            should_ignore_corrupted_stats = true;
        }

        if (should_ignore_corrupted_stats) {
            return Status::DataQualityError("Error statistics, should ignore.");
        }

        min_string = statistic.min;
        max_string = statistic.max;
    } else {
        return Status::DataQualityError("This parquet file not set min/max value");
    }

    if (!ParquetPredicate::parse_min_max_value(ans_stat->col_schema, min_string, max_string,
                                               *ans_stat->ctz, &ans_stat->min_value,
                                               &ans_stat->max_value)
                 .ok()) {
        return Status::DataQualityError("Parse min/max value failed!");
    }

    return Status::OK();
}

Status ParquetPredicate::read_bloom_filter(const tparquet::ColumnMetaData& column_meta_data,
                                           io::FileReaderSPtr file_reader, io::IOContext* io_ctx,
                                           std::unique_ptr<segment_v2::BloomFilter>& bf) {
    size_t size;
    if (!column_meta_data.__isset.bloom_filter_offset) {
        return Status::NotSupported("Can not use this parquet bloom filter.");
    }

    if (column_meta_data.__isset.bloom_filter_length && column_meta_data.bloom_filter_length > 0) {
        size = column_meta_data.bloom_filter_length;
    } else {
        size = BLOOM_FILTER_MAX_HEADER_LENGTH;
    }
    size_t bytes_read = 0;
    std::vector<uint8_t> header_buffer(size);
    RETURN_IF_ERROR(file_reader->read_at(column_meta_data.bloom_filter_offset,
                                         Slice(header_buffer.data(), size), &bytes_read, io_ctx));

    tparquet::BloomFilterHeader t_bloom_filter_header;
    uint32_t t_bloom_filter_header_size = static_cast<uint32_t>(bytes_read);
    RETURN_IF_ERROR(deserialize_thrift_msg(header_buffer.data(), &t_bloom_filter_header_size, true,
                                           &t_bloom_filter_header));

    // TODO the bloom filter could be encrypted, too, so need to double check that this is NOT the case
    if (!t_bloom_filter_header.algorithm.__isset.BLOCK ||
        !t_bloom_filter_header.compression.__isset.UNCOMPRESSED ||
        !t_bloom_filter_header.hash.__isset.XXHASH) {
        return Status::NotSupported("Can not use this parquet bloom filter.");
    }

    bf = std::make_unique<ParquetBlockSplitBloomFilter>();

    std::vector<uint8_t> data_buffer(t_bloom_filter_header.numBytes);
    RETURN_IF_ERROR(file_reader->read_at(
            column_meta_data.bloom_filter_offset + t_bloom_filter_header_size,
            Slice(data_buffer.data(), t_bloom_filter_header.numBytes), &bytes_read, io_ctx));

    RETURN_IF_ERROR(bf->init(reinterpret_cast<const char*>(data_buffer.data()),
                             t_bloom_filter_header.numBytes,
                             segment_v2::HashStrategyPB::XX_HASH_64));

    return Status::OK();
}
#include "common/compile_check_end.h"

} // namespace doris::vectorized
