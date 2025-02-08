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

#include "common/status.h"
#include "exprs/bloom_filter_func.h"
#include "exprs/runtime_filter/runtime_filter_definitions.h"
#include "runtime/runtime_state.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/exprs/vruntimefilter_wrapper.h"

namespace doris {

// This class is a wrapper of runtime predicate function
class RuntimePredicateWrapper {
public:
    RuntimePredicateWrapper(const RuntimeFilterParams* params);
    // for a 'tmp' runtime predicate wrapper
    // only could called assign method or as a param for merge
    RuntimePredicateWrapper(PrimitiveType column_type, RuntimeFilterType type, uint32_t filter_id)
            : _column_return_type(column_type),
              _filter_type(type),
              _context(new RuntimeFilterContext()),
              _filter_id(filter_id) {}

    Status change_to_bloom_filter();

    Status init_bloom_filter(const size_t build_bf_cardinality);

    bool get_build_bf_cardinality() const;

    void insert_to_bloom_filter(BloomFilterFuncBase* bloom_filter) const;

    BloomFilterFuncBase* get_bloomfilter() const { return _context->bloom_filter_func.get(); }

    void insert_fixed_len(const vectorized::ColumnPtr& column, size_t start);

    void insert_batch(const vectorized::ColumnPtr& column, size_t start) {
        if (get_real_type() == RuntimeFilterType::BITMAP_FILTER) {
            bitmap_filter_insert_batch(column, start);
        } else {
            insert_fixed_len(column, start);
        }
    }

    void bitmap_filter_insert_batch(const vectorized::ColumnPtr column, size_t start);

    RuntimeFilterType get_real_type() const {
        if (_filter_type == RuntimeFilterType::IN_OR_BLOOM_FILTER) {
            if (_context->hybrid_set) {
                return RuntimeFilterType::IN_FILTER;
            }
            return RuntimeFilterType::BLOOM_FILTER;
        }
        return _filter_type;
    }

    size_t get_bloom_filter_size() const;

    Status get_push_exprs(std::list<vectorized::VExprContextSPtr>& probe_ctxs,
                          std::vector<vectorized::VRuntimeFilterPtr>& push_exprs,
                          const TExpr& probe_expr);

    Status merge(const RuntimePredicateWrapper* wrapper);

    Status assign(const PInFilter& in_filter, bool contain_null);

    // used by shuffle runtime filter
    // assign this filter by protobuf
    Status assign(const PBloomFilter& bloom_filter, butil::IOBufAsZeroCopyInputStream* data,
                  bool contain_null);

    // used by shuffle runtime filter
    // assign this filter by protobuf
    Status assign(const PMinMaxFilter& minmax_filter, bool contain_null);

    void get_bloom_filter_desc(char** data, int* filter_length);

    PrimitiveType column_type() { return _column_return_type; }

    bool is_bloomfilter() const { return get_real_type() == RuntimeFilterType::BLOOM_FILTER; }

    bool contain_null() const;

    bool is_ignored() const { return _context->ignored; }

    void set_ignored() { _context->ignored = true; }

    bool is_disabled() const { return _context->disabled; }

    void set_disabled();

    void batch_assign(const PInFilter& filter,
                      void (*assign_func)(std::shared_ptr<HybridSetBase>& _hybrid_set,
                                          PColumnValue&));

    size_t get_in_filter_size() const;

    std::shared_ptr<BitmapFilterFuncBase> get_bitmap_filter() const {
        return _context->bitmap_filter_func;
    }

    friend class RuntimeFilter;
    friend class RuntimeFilterProducer;
    friend class RuntimeFilterConsumer;
    friend class RuntimeFilterMerger;

    void set_filter_id(int id);

    template <class T>
    Status assign_data(const T& request, butil::IOBufAsZeroCopyInputStream* data) {
        PFilterType filter_type = request.filter_type();

        if (request.has_disabled() && request.disabled()) {
            set_disabled();
            return Status::OK();
        }

        if (request.has_ignored() && request.ignored()) {
            set_ignored();
            return Status::OK();
        }

        switch (filter_type) {
        case PFilterType::IN_FILTER: {
            DCHECK(request.has_in_filter());
            return assign(request.in_filter(), request.contain_null());
        }
        case PFilterType::BLOOM_FILTER: {
            DCHECK(request.has_bloom_filter());
            _context->hybrid_set.reset(); // change in_or_bloom filter to bloom filter
            return assign(request.bloom_filter(), data, request.contain_null());
        }
        case PFilterType::MIN_FILTER:
        case PFilterType::MAX_FILTER:
        case PFilterType::MINMAX_FILTER: {
            DCHECK(request.has_minmax_filter());
            return assign(request.minmax_filter(), request.contain_null());
        }
        default:
            return Status::InternalError("unknown filter type {}", int(filter_type));
        }
    }

    std::string debug_string() const {
        return fmt::format(
                "filter_id: {}, ignored: {}, disabled: {}, build_bf_cardinality: {}, error_msg: "
                "[{}]",
                _filter_id, _context->ignored, _context->disabled, get_build_bf_cardinality(),
                _context->err_msg);
    }

private:
    // When a runtime filter received from remote and it is a bloom filter, _column_return_type will be invalid.
    PrimitiveType _column_return_type; // column type
    RuntimeFilterType _filter_type;
    int32_t _max_in_num = -1;

    RuntimeFilterContextSPtr _context;
    uint32_t _filter_id;
};

} // namespace doris
