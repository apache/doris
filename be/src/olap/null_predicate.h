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

#include <glog/logging.h>
#include <stdint.h>

#include <ostream>
#include <string>
#include <utility>

#include "common/status.h"
#include "olap/column_predicate.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "olap/schema.h"
#include "vec/exec/format/parquet/parquet_predicate.h"

namespace roaring {
class Roaring;
} // namespace roaring

namespace doris {
namespace segment_v2 {
class InvertedIndexIterator;
} // namespace segment_v2
namespace vectorized {
class IColumn;
} // namespace vectorized

class NullPredicate final : public ColumnPredicate {
public:
    ENABLE_FACTORY_CREATOR(NullPredicate);
    NullPredicate(uint32_t column_id, std::string col_name, bool is_null, PrimitiveType type,
                  bool opposite = false);
    NullPredicate(const NullPredicate& other) = delete;
    NullPredicate(const NullPredicate& other, uint32_t column_id)
            : ColumnPredicate(other, column_id), _is_null(other._is_null) {}
    ~NullPredicate() override = default;
    std::shared_ptr<ColumnPredicate> clone(uint32_t column_id) const override {
        return NullPredicate::create_shared(*this, column_id);
    }
    std::string debug_string() const override {
        fmt::memory_buffer debug_string_buffer;
        fmt::format_to(debug_string_buffer, "NullPredicate({}, is_null={})",
                       ColumnPredicate::debug_string(), _is_null);
        return fmt::to_string(debug_string_buffer);
    }
    bool could_be_erased() const override { return true; }

    PredicateType type() const override;

    Status evaluate(const vectorized::IndexFieldNameAndTypePair& name_with_type,
                    IndexIterator* iterator, uint32_t num_rows,
                    roaring::Roaring* bitmap) const override;

    void evaluate_or(const vectorized::IColumn& column, const uint16_t* sel, uint16_t size,
                     bool* flags) const override;

    void evaluate_and(const vectorized::IColumn& column, const uint16_t* sel, uint16_t size,
                      bool* flags) const override;

    bool evaluate_and(const segment_v2::ZoneMap& zone_map) const override {
        if (_is_null) {
            return zone_map.has_null;
        } else {
            return zone_map.has_not_null;
        }
    }

    bool evaluate_and(vectorized::ParquetPredicate::ColumnStat* statistic) const override {
        if (!(*statistic->get_stat_func)(statistic, column_id())) {
            return true;
        }
        if (_is_null) {
            return true;
        } else {
            return !statistic->is_all_null;
        }
    }

    bool evaluate_and(vectorized::ParquetPredicate::CachedPageIndexStat* statistic,
                      RowRanges* row_ranges) const override {
        vectorized::ParquetPredicate::PageIndexStat* stat = nullptr;
        if (!(statistic->get_stat_func)(&stat, column_id())) {
            row_ranges->add(statistic->row_group_range);
            return true;
        }
        for (int page_id = 0; page_id < stat->num_of_pages; page_id++) {
            if (_is_null || !stat->is_all_null[page_id]) {
                row_ranges->add(stat->ranges[page_id]);
            }
        };
        return row_ranges->count() > 0;
    }

    bool evaluate_del(const segment_v2::ZoneMap& zone_map) const override {
        // evaluate_del only use for delete condition to filter page, need use delete condition origin value,
        // when opposite==true, origin value 'is null'->'is not null' and 'is not null'->'is null',
        // so when _is_null==true, need check 'is not null' and _is_null==false, need check 'is null'
        if (_is_null) {
            return !zone_map.has_null;
        } else {
            return !zone_map.has_not_null;
        }
    }

    bool evaluate_and(const segment_v2::BloomFilter* bf) const override {
        // null predicate can not use ngram bf, just return true to accept
        if (bf->is_ngram_bf()) return true;
        if (_is_null) {
            return bf->test_bytes(nullptr, 0);
        } else {
            throw Exception(Status::FatalError(
                    "Bloom filter is not supported by predicate type: is_null="));
        }
    }

    bool can_do_bloom_filter(bool ngram) const override { return _is_null && !ngram; }

    void evaluate_vec(const vectorized::IColumn& column, uint16_t size, bool* flags) const override;

private:
    uint16_t _evaluate_inner(const vectorized::IColumn& column, uint16_t* sel,
                             uint16_t size) const override;

    bool _is_null; //true for null, false for not null
};

} //namespace doris
