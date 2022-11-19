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

#include "exprs/runtime_filter.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"

namespace doris {
// this class used in a hash join node
// Provide a unified interface for other classes
template <typename ExprCtxType>
class RuntimeFilterSlotsBase {
public:
    RuntimeFilterSlotsBase(const std::vector<ExprCtxType*>& prob_expr_ctxs,
                           const std::vector<ExprCtxType*>& build_expr_ctxs,
                           const std::vector<TRuntimeFilterDesc>& runtime_filter_descs)
            : _probe_expr_context(prob_expr_ctxs),
              _build_expr_context(build_expr_ctxs),
              _runtime_filter_descs(runtime_filter_descs) {}

    Status init(RuntimeState* state, int64_t hash_table_size) {
        DCHECK(_probe_expr_context.size() == _build_expr_context.size());

        // runtime filter effect strategy
        // 1. we will ignore IN filter when hash_table_size is too big
        // 2. we will ignore BLOOM filter and MinMax filter when hash_table_size
        // is too small and IN filter has effect

        std::map<int, bool> has_in_filter;

        auto ignore_local_filter = [state](int filter_id) {
            IRuntimeFilter* consumer_filter = nullptr;
            state->runtime_filter_mgr()->get_consume_filter(filter_id, &consumer_filter);
            DCHECK(consumer_filter != nullptr);
            consumer_filter->set_ignored();
            consumer_filter->signal();
        };

        auto ignore_remote_filter = [](IRuntimeFilter* runtime_filter, std::string& msg) {
            runtime_filter->set_ignored();
            runtime_filter->set_ignored_msg(msg);
            runtime_filter->publish();
            runtime_filter->publish_finally();
        };

        // ordered vector: IN, IN_OR_BLOOM, others.
        // so we can ignore other filter if IN Predicate exists.
        std::vector<TRuntimeFilterDesc> sorted_runtime_filter_descs(_runtime_filter_descs);
        auto compare_desc = [](TRuntimeFilterDesc& d1, TRuntimeFilterDesc& d2) {
            if (d1.type == d2.type) {
                return false;
            } else if (d1.type == TRuntimeFilterType::IN) {
                return true;
            } else if (d2.type == TRuntimeFilterType::IN) {
                return false;
            } else if (d1.type == TRuntimeFilterType::IN_OR_BLOOM) {
                return true;
            } else if (d2.type == TRuntimeFilterType::IN_OR_BLOOM) {
                return false;
            } else {
                return d1.type < d2.type;
            }
        };
        std::sort(sorted_runtime_filter_descs.begin(), sorted_runtime_filter_descs.end(),
                  compare_desc);

        for (auto& filter_desc : sorted_runtime_filter_descs) {
            IRuntimeFilter* runtime_filter = nullptr;
            RETURN_IF_ERROR(state->runtime_filter_mgr()->get_producer_filter(filter_desc.filter_id,
                                                                             &runtime_filter));
            DCHECK(runtime_filter != nullptr);
            DCHECK(runtime_filter->expr_order() >= 0);
            DCHECK(runtime_filter->expr_order() < _probe_expr_context.size());

            // do not create 'in filter' when hash_table size over limit
            auto max_in_num = state->runtime_filter_max_in_num();
            bool over_max_in_num = (hash_table_size >= max_in_num);

            bool is_in_filter = (runtime_filter->type() == RuntimeFilterType::IN_FILTER);

            if (over_max_in_num &&
                runtime_filter->type() == RuntimeFilterType::IN_OR_BLOOM_FILTER) {
                runtime_filter->change_to_bloom_filter();
            }

            // Note:
            // In the case that exist *remote target* and in filter and other filter,
            // we must merge other filter whatever in filter is over the max num in current node,
            // because:
            // case 1: (in filter >= max num) in current node, so in filter will be ignored,
            //         and then other filter can be used
            // case 2: (in filter < max num) in current node, we don't know whether the in filter
            //         will be ignored in merge node, so we must transfer other filter to merge node
            if (!runtime_filter->has_remote_target()) {
                bool exists_in_filter = has_in_filter[runtime_filter->expr_order()];
                if (is_in_filter && over_max_in_num) {
                    VLOG_DEBUG << "fragment instance " << print_id(state->fragment_instance_id())
                               << " ignore runtime filter(in filter id " << filter_desc.filter_id
                               << ") because: in_num(" << hash_table_size << ") >= max_in_num("
                               << max_in_num << ")";
                    ignore_local_filter(filter_desc.filter_id);
                    continue;
                } else if (!is_in_filter && exists_in_filter) {
                    // do not create 'bloom filter' and 'minmax filter' when 'in filter' has created
                    // because in filter is exactly filter, so it is enough to filter data
                    VLOG_DEBUG << "fragment instance " << print_id(state->fragment_instance_id())
                               << " ignore runtime filter(" << to_string(runtime_filter->type())
                               << " id " << filter_desc.filter_id
                               << ") because: already exists in filter";
                    ignore_local_filter(filter_desc.filter_id);
                    continue;
                }
            } else if (is_in_filter && over_max_in_num) {
#ifdef VLOG_DEBUG_IS_ON
                std::string msg = fmt::format(
                        "fragment instance {} ignore runtime filter(in filter id {}) because: "
                        "in_num({}) >= max_in_num({})",
                        print_id(state->fragment_instance_id()), filter_desc.filter_id,
                        hash_table_size, max_in_num);
                ignore_remote_filter(runtime_filter, msg);
#else
                ignore_remote_filter(runtime_filter, "ignored");
#endif
                continue;
            }

            if ((runtime_filter->type() == RuntimeFilterType::IN_FILTER) ||
                (runtime_filter->type() == RuntimeFilterType::IN_OR_BLOOM_FILTER &&
                 !over_max_in_num)) {
                has_in_filter[runtime_filter->expr_order()] = true;
            }
            _runtime_filters[runtime_filter->expr_order()].push_back(runtime_filter);
        }

        return Status::OK();
    }

    void insert(TupleRow* row) {
        for (int i = 0; i < _build_expr_context.size(); ++i) {
            auto iter = _runtime_filters.find(i);
            if (iter != _runtime_filters.end()) {
                void* val = _build_expr_context[i]->get_value(row);
                if (val != nullptr) {
                    for (auto filter : iter->second) {
                        filter->insert(val);
                    }
                }
            }
        }
    }

    void insert(std::unordered_map<const vectorized::Block*, std::vector<int>>& datas) {
        for (int i = 0; i < _build_expr_context.size(); ++i) {
            auto iter = _runtime_filters.find(i);
            if (iter == _runtime_filters.end()) {
                continue;
            }

            int result_column_id = _build_expr_context[i]->get_last_result_column_id();
            for (auto it : datas) {
                auto& column = it.first->get_by_position(result_column_id).column;

                if (auto* nullable =
                            vectorized::check_and_get_column<vectorized::ColumnNullable>(*column)) {
                    auto& column_nested = nullable->get_nested_column_ptr();
                    auto& column_nullmap = nullable->get_null_map_column_ptr();
                    std::vector<int> indexs;
                    for (int row_num : it.second) {
                        if (assert_cast<const vectorized::ColumnUInt8*>(column_nullmap.get())
                                    ->get_bool(row_num)) {
                            continue;
                        }
                        indexs.push_back(row_num);
                    }
                    for (auto filter : iter->second) {
                        filter->insert_batch(column_nested, indexs);
                    }

                } else {
                    for (auto filter : iter->second) {
                        filter->insert_batch(column, it.second);
                    }
                }
            }
        }
    }

    // should call this method after insert
    void ready_for_publish() {
        for (auto& pair : _runtime_filters) {
            for (auto filter : pair.second) {
                filter->ready_for_publish();
            }
        }
    }
    // publish runtime filter
    void publish() {
        for (int i = 0; i < _probe_expr_context.size(); ++i) {
            auto iter = _runtime_filters.find(i);
            if (iter != _runtime_filters.end()) {
                for (auto filter : iter->second) {
                    filter->publish();
                }
            }
        }
        for (auto& pair : _runtime_filters) {
            for (auto filter : pair.second) {
                filter->publish_finally();
            }
        }
    }

    Status apply_from_other(RuntimeFilterSlotsBase<ExprCtxType>* other) {
        for (auto& it : _runtime_filters) {
            auto& other_filters = other->_runtime_filters[it.first];
            for (auto& filter : it.second) {
                auto filter_id = filter->filter_id();
                auto ret = std::find_if(other_filters.begin(), other_filters.end(),
                                        [&](IRuntimeFilter* other_filter) {
                                            return other_filter->filter_id() == filter_id;
                                        });
                if (ret == other_filters.end()) {
                    return Status::Aborted("invalid runtime filter id: {}", filter_id);
                }
                filter->apply_from_other(*ret);
            }
        }
        return Status::OK();
    }

    bool empty() { return !_runtime_filters.size(); }

private:
    const std::vector<ExprCtxType*>& _probe_expr_context;
    const std::vector<ExprCtxType*>& _build_expr_context;
    const std::vector<TRuntimeFilterDesc>& _runtime_filter_descs;
    // prob_contition index -> [IRuntimeFilter]
    std::map<int, std::list<IRuntimeFilter*>> _runtime_filters;
};

using RuntimeFilterSlots = RuntimeFilterSlotsBase<ExprContext>;
using VRuntimeFilterSlots = RuntimeFilterSlotsBase<vectorized::VExprContext>;
} // namespace doris
