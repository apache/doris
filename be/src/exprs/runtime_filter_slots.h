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

        auto ignore_remote_filter = [](IRuntimeFilter* runtime_filter, std::string &msg) {
            runtime_filter->set_ignored();
            runtime_filter->set_ignored_msg(msg);
            runtime_filter->publish();
            runtime_filter->publish_finally();
        };

        for (auto& filter_desc : _runtime_filter_descs) {
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
                    LOG(INFO) << "fragment instance " << print_id(state->fragment_instance_id())
                              << " ignore runtime filter(in filter id " << filter_desc.filter_id
                              << ") because: in_num(" << hash_table_size
                              << ") >= max_in_num(" << max_in_num << ")";
                    ignore_local_filter(filter_desc.filter_id);
                    continue;
                } else if (!is_in_filter && exists_in_filter) {
                    // do not create 'bloom filter' and 'minmax filter' when 'in filter' has created
                    // because in filter is exactly filter, so it is enough to filter data
                    LOG(INFO) << "fragment instance " << print_id(state->fragment_instance_id())
                              << " ignore runtime filter(" << to_string(runtime_filter->type())
                              << " id " << filter_desc.filter_id
                              << ") because: already exists in filter";
                    ignore_local_filter(filter_desc.filter_id);
                    continue;
                }
            } else if (is_in_filter && over_max_in_num) {
                std::string msg = fmt::format("fragment instance {} ignore runtime filter(in filter id {}) because: in_num({}) >= max_in_num({})",
                  print_id(state->fragment_instance_id()), filter_desc.filter_id, hash_table_size, max_in_num);
                ignore_remote_filter(runtime_filter, msg);
                continue;
            }

            has_in_filter[runtime_filter->expr_order()] =
                    (runtime_filter->type() == RuntimeFilterType::IN_FILTER);
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

    bool empty() { return !_runtime_filters.size(); }

private:
    const std::vector<ExprCtxType*>& _probe_expr_context;
    const std::vector<ExprCtxType*>& _build_expr_context;
    const std::vector<TRuntimeFilterDesc>& _runtime_filter_descs;
    // prob_contition index -> [IRuntimeFilter]
    std::map<int, std::list<IRuntimeFilter*>> _runtime_filters;
};

using RuntimeFilterSlots = RuntimeFilterSlotsBase<ExprContext>;

} // namespace doris
