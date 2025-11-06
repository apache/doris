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

#pragma once

#include <gen_cpp/parquet_types.h>

#include <cmath>
#include <cstring>
#include <vector>

#include "cctz/time_zone.h"
#include "exec/olap_common.h"
#include "olap/rowset/segment_v2/row_ranges.h"
#include "parquet_common.h"
#include "util/timezone_utils.h"
#include "vec/common/endian.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/exec/format/format_common.h"
#include "vec/exec/format/parquet/parquet_column_convert.h"
#include "vec/exec/format/parquet/schema_desc.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class ParquetPredicate {
private:
    static inline bool _is_ascii(uint8_t byte) { return byte < 128; }

    static int _common_prefix(const std::string& encoding_min, const std::string& encoding_max) {
        size_t min_length = std::min(encoding_min.size(), encoding_max.size());
        int common_length = 0;
        while (common_length < min_length &&
               encoding_min[common_length] == encoding_max[common_length]) {
            common_length++;
        }
        return common_length;
    }

    static bool _try_read_old_utf8_stats(std::string& encoding_min, std::string& encoding_max) {
        if (encoding_min == encoding_max) {
            // If min = max, then there is a single value only
            // No need to modify, just use min
            encoding_max = encoding_min;
            return true;
        } else {
            int common_prefix_length = _common_prefix(encoding_min, encoding_max);

            // For min we can retain all-ASCII, because this produces a strictly lower value.
            int min_good_length = common_prefix_length;
            while (min_good_length < encoding_min.size() &&
                   _is_ascii(static_cast<uint8_t>(encoding_min[min_good_length]))) {
                min_good_length++;
            }

            // For max we can be sure only of the part matching the min. When they differ, we can consider only one next, and only if both are ASCII
            int max_good_length = common_prefix_length;
            if (max_good_length < encoding_max.size() && max_good_length < encoding_min.size() &&
                _is_ascii(static_cast<uint8_t>(encoding_min[max_good_length])) &&
                _is_ascii(static_cast<uint8_t>(encoding_max[max_good_length]))) {
                max_good_length++;
            }
            // Incrementing 127 would overflow. Incrementing within non-ASCII can have side-effects.
            while (max_good_length > 0 &&
                   (static_cast<uint8_t>(encoding_max[max_good_length - 1]) == 127 ||
                    !_is_ascii(static_cast<uint8_t>(encoding_max[max_good_length - 1])))) {
                max_good_length--;
            }
            if (max_good_length == 0) {
                // We can return just min bound, but code downstream likely expects both are present or both are absent.
                return false;
            }

            encoding_min.resize(min_good_length);
            encoding_max.resize(max_good_length);
            if (max_good_length > 0) {
                encoding_max[max_good_length - 1]++;
            }
            return true;
        }
    }

    static SortOrder _determine_sort_order(const tparquet::SchemaElement& parquet_schema) {
        tparquet::Type::type physical_type = parquet_schema.type;
        const tparquet::LogicalType& logical_type = parquet_schema.logicalType;

        // Assume string type is SortOrder::SIGNED, use ParquetPredicate::_try_read_old_utf8_stats() to handle it.
        if (logical_type.__isset.STRING &&
            (physical_type == tparquet::Type::BYTE_ARRAY ||
             physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY)) {
            return SortOrder::SIGNED;
        }

        if (logical_type.__isset.INTEGER) {
            if (logical_type.INTEGER.isSigned) {
                return SortOrder::SIGNED;
            } else {
                return SortOrder::UNSIGNED;
            }
        } else if (logical_type.__isset.DATE) {
            return SortOrder::SIGNED;
        } else if (logical_type.__isset.ENUM) {
            return SortOrder::UNSIGNED;
        } else if (logical_type.__isset.BSON) {
            return SortOrder::UNSIGNED;
        } else if (logical_type.__isset.JSON) {
            return SortOrder::UNSIGNED;
        } else if (logical_type.__isset.STRING) {
            return SortOrder::UNSIGNED;
        } else if (logical_type.__isset.DECIMAL) {
            return SortOrder::UNKNOWN;
        } else if (logical_type.__isset.MAP) {
            return SortOrder::UNKNOWN;
        } else if (logical_type.__isset.LIST) {
            return SortOrder::UNKNOWN;
        } else if (logical_type.__isset.TIME) {
            return SortOrder::SIGNED;
        } else if (logical_type.__isset.TIMESTAMP) {
            return SortOrder::SIGNED;
        } else if (logical_type.__isset.UNKNOWN) {
            return SortOrder::UNKNOWN;
        } else {
            switch (physical_type) {
            case tparquet::Type::BOOLEAN:
            case tparquet::Type::INT32:
            case tparquet::Type::INT64:
            case tparquet::Type::FLOAT:
            case tparquet::Type::DOUBLE:
                return SortOrder::SIGNED;
            case tparquet::Type::BYTE_ARRAY:
            case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
                return SortOrder::UNSIGNED;
            case tparquet::Type::INT96:
                return SortOrder::UNKNOWN;
            default:
                return SortOrder::UNKNOWN;
            }
        }
    }

public:
    struct ColumnStat {
        std::string encoded_min_value;
        std::string encoded_max_value;
        bool has_null;
        bool is_all_null;
        const FieldSchema* col_schema;
        const cctz::time_zone* ctz;
        std::function<bool(ParquetPredicate::ColumnStat*, const int)>* get_stat_func;
    };

    struct PageIndexStat {
        // Indicates whether the page index information in this column can be used.
        bool available = false;
        int64_t num_of_pages;
        std::vector<std::string> encoded_min_value;
        std::vector<std::string> encoded_max_value;
        std::vector<bool> has_null;
        std::vector<bool> is_all_null;
        const FieldSchema* col_schema;

        // Record the row range corresponding to each page.
        std::vector<segment_v2::RowRange> ranges;
    };

    struct CachedPageIndexStat {
        const cctz::time_zone* ctz;
        std::map<int, PageIndexStat> stats;
        std::function<bool(PageIndexStat**, int)> get_stat_func;
    };

    // The encoded Parquet min-max value is parsed into  `fields`;
    // `available` indicates that the min-max information is not available.
    // Can be used in row groups and page indexe statistics.
    static Status get_min_max_value(const FieldSchema* col_schema,
                                    const std::vector<std::string>& encoded_min,
                                    const std::vector<std::string>& encoded_max,
                                    const cctz::time_zone& ctz, std::vector<Field>* min_fields,
                                    std::vector<Field>* max_fields, std::vector<bool>* available) {
        auto sz = encoded_min.size();
        min_fields->resize(sz);
        max_fields->resize(sz);
        available->resize(sz, true);

        auto logical_data_type = remove_nullable(col_schema->data_type);
        auto converter = parquet::PhysicalToLogicalConverter::get_converter(
                col_schema, logical_data_type, logical_data_type, &ctz);
        ColumnPtr physical_column;
        switch (col_schema->parquet_schema.type) {
        case tparquet::Type::type::BOOLEAN: {
            auto physical_col = ColumnUInt8::create();
            physical_col->resize(2 * sz);

            for (size_t idx = 0; idx < sz; idx++) {
                physical_col->get_data()[idx * 2] =
                        *reinterpret_cast<const bool*>(encoded_min[idx].data());
                physical_col->get_data()[idx * 2 + 1] =
                        *reinterpret_cast<const bool*>(encoded_max[idx].data());
            }
            physical_column = std::move(physical_col);
            break;
        }
        case tparquet::Type::type::INT32: {
            auto physical_col = ColumnInt32::create();

            physical_col->resize(2 * sz);
            for (size_t idx = 0; idx < sz; idx++) {
                physical_col->get_data()[idx * 2] =
                        *reinterpret_cast<const int32_t*>(encoded_min[idx].data());
                physical_col->get_data()[idx * 2 + 1] =
                        *reinterpret_cast<const int32_t*>(encoded_max[idx].data());
            }
            physical_column = std::move(physical_col);
            break;
        }
        case tparquet::Type::type::INT64: {
            auto physical_col = ColumnInt64::create();

            physical_col->resize(2 * sz);
            for (size_t idx = 0; idx < sz; idx++) {
                physical_col->get_data()[idx * 2] =
                        *reinterpret_cast<const int64_t*>(encoded_min[idx].data());
                physical_col->get_data()[idx * 2 + 1] =
                        *reinterpret_cast<const int64_t*>(encoded_max[idx].data());
            }
            physical_column = std::move(physical_col);
            break;
        }
        case tparquet::Type::type::FLOAT: {
            auto physical_col = ColumnFloat32::create();
            physical_col->resize(2 * sz);

            for (size_t idx = 0; idx < sz; idx++) {
                physical_col->get_data()[idx * 2] =
                        *reinterpret_cast<const float*>(encoded_min[idx].data());
                physical_col->get_data()[idx * 2 + 1] =
                        *reinterpret_cast<const float*>(encoded_max[idx].data());
            }
            physical_column = std::move(physical_col);
            break;
        }
        case tparquet::Type::type::DOUBLE: {
            auto physical_col = ColumnFloat64 ::create();
            physical_col->resize(2 * sz);

            for (size_t idx = 0; idx < sz; idx++) {
                physical_col->get_data()[idx * 2] =
                        *reinterpret_cast<const double*>(encoded_min[idx].data());
                physical_col->get_data()[idx * 2 + 1] =
                        *reinterpret_cast<const double*>(encoded_max[idx].data());
            }

            physical_column = std::move(physical_col);
            break;
        }
        case tparquet::Type::type::BYTE_ARRAY: {
            auto physical_col = ColumnString::create();
            for (size_t idx = 0; idx < sz; idx++) {
                physical_col->insert_data(encoded_min[idx].data(), encoded_min[idx].size());
                physical_col->insert_data(encoded_max[idx].data(), encoded_max[idx].size());
            }
            physical_column = std::move(physical_col);
            break;
        }
        case tparquet::Type::type::FIXED_LEN_BYTE_ARRAY: {
            auto physical_col = ColumnUInt8::create();
            physical_col->resize(2 * col_schema->parquet_schema.type_length * sz);

            auto ptr = physical_col->get_data().data();

            for (size_t idx = 0; idx < sz; idx++) {
                DCHECK(col_schema->parquet_schema.type_length == encoded_min[idx].length());
                DCHECK(col_schema->parquet_schema.type_length == encoded_max[idx].length());
                memcpy(ptr, encoded_min[idx].data(), encoded_min[idx].length());
                ptr += encoded_min[idx].length();
                memcpy(ptr, encoded_max[idx].data(), encoded_max[idx].length());
                ptr += encoded_max[idx].length();
            }
            physical_column = std::move(physical_col);
            break;
        }
        case tparquet::Type::type::INT96: {
            auto physical_col = ColumnInt8::create();
            physical_col->resize(2 * sizeof(ParquetInt96) * sz);

            auto ptr = physical_col->get_data().data();
            for (size_t idx = 0; idx < sz; idx++) {
                DCHECK(sizeof(ParquetInt96) == encoded_min[idx].length());
                DCHECK(sizeof(ParquetInt96) == encoded_max[idx].length());

                memcpy(ptr, encoded_min[idx].data(), encoded_min[idx].length());
                ptr += encoded_min[idx].length();
                memcpy(ptr, encoded_max[idx].data(), encoded_max[idx].length());
                ptr += encoded_max[idx].length();
            }
            physical_column = std::move(physical_col);
            break;
        }
        }

        ColumnPtr logical_column;
        if (converter->is_consistent()) {
            logical_column = physical_column;
        } else {
            logical_column = logical_data_type->create_column();
            RETURN_IF_ERROR(converter->physical_convert(physical_column, logical_column));
        }

        DCHECK(logical_column->size() == 2 * sz);

        for (size_t idx = 0; idx < sz; idx++) {
            min_fields->operator[](idx) = logical_column->operator[](idx * 2);
            max_fields->operator[](idx) = logical_column->operator[](idx * 2 + 1);
        }

        auto logical_prim_type = logical_data_type->get_primitive_type();

        if (logical_prim_type == TYPE_FLOAT) {
            for (size_t idx = 0; idx < sz; idx++) {
                auto& min_value = min_fields->operator[](idx)
                                          .get<PrimitiveTypeTraits<TYPE_FLOAT>::NearestFieldType>();
                auto& max_value = max_fields->operator[](idx)
                                          .get<PrimitiveTypeTraits<TYPE_FLOAT>::NearestFieldType>();

                if (std::isnan(min_value) || std::isnan(max_value)) {
                    available->operator[](idx) = false;
                }
                // Updating min to -0.0 and max to +0.0 to ensure that no 0.0 values would be skipped
                if (std::signbit(min_value) == 0 && min_value == 0.0F) {
                    min_value = -0.0F;
                }
                if (std::signbit(max_value) != 0 && max_value == -0.0F) {
                    max_value = 0.0F;
                }
            }

        } else if (logical_prim_type == TYPE_DOUBLE) {
            for (size_t idx = 0; idx < sz; idx++) {
                auto& min_value =
                        min_fields->operator[](idx)
                                .get<PrimitiveTypeTraits<TYPE_DOUBLE>::NearestFieldType>();
                auto& max_value =
                        max_fields->operator[](idx)
                                .get<PrimitiveTypeTraits<TYPE_DOUBLE>::NearestFieldType>();

                if (std::isnan(min_value) || std::isnan(max_value)) {
                    available->operator[](idx) = false;
                }
                // Updating min to -0.0 and max to +0.0 to ensure that no 0.0 values would be skipped
                if (std::signbit(min_value) == 0 && min_value == 0.0F) {
                    min_value = -0.0F;
                }
                if (std::signbit(max_value) != 0 && max_value == -0.0F) {
                    max_value = 0.0F;
                }
            }
        } else if (col_schema->parquet_schema.type == tparquet::Type::type::INT96 ||
                   logical_prim_type == TYPE_DATETIMEV2) {
            for (size_t idx = 0; idx < sz; idx++) {
                auto& min_value =
                        min_fields->operator[](idx)
                                .get<PrimitiveTypeTraits<TYPE_DATETIMEV2>::NearestFieldType>();
                auto& max_value =
                        max_fields->operator[](idx)
                                .get<PrimitiveTypeTraits<TYPE_DATETIMEV2>::NearestFieldType>();

                // From Trino: Parquet INT96 timestamp values were compared incorrectly
                // for the purposes of producing statistics by older parquet writers,
                // so PARQUET-1065 deprecated them. The result is that any writer that produced stats
                // was producing unusable incorrect values, except the special case where min == max
                // and an incorrect ordering would not be material to the result.
                // PARQUET-1026 made binary stats available and valid in that special case.
                if (min_value != max_value) {
                    available->operator[](idx) = false;
                }
            }
        }

        return Status::OK();
    }

    static Status read_column_stats(const FieldSchema* col_schema,
                                    const tparquet::ColumnMetaData& column_meta_data,
                                    std::unordered_map<tparquet::Type::type, bool>* ignored_stats,
                                    const std::string& file_created_by, ColumnStat* ans_stat) {
        auto& statistic = column_meta_data.statistics;

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
            ans_stat->encoded_min_value = statistic.min_value;
            ans_stat->encoded_max_value = statistic.max_value;

            if (prim_type == TYPE_VARCHAR || prim_type == TYPE_CHAR || prim_type == TYPE_STRING) {
                auto encoded_min_copy = ans_stat->encoded_min_value;
                auto encoded_max_copy = ans_stat->encoded_max_value;
                if (!_try_read_old_utf8_stats(encoded_min_copy, encoded_max_copy)) {
                    return Status::DataQualityError("Can not use this parquet min/max value.");
                }
                ans_stat->encoded_min_value = encoded_min_copy;
                ans_stat->encoded_max_value = encoded_max_copy;
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

            ans_stat->encoded_min_value = statistic.min;
            ans_stat->encoded_max_value = statistic.max;
        } else {
            return Status::DataQualityError("This parquet file not set min/max value");
        }

        return Status::OK();
    }
};
#include "common/compile_check_end.h"

} // namespace doris::vectorized
