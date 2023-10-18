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

#include "common/exception.h"
#include "common/status.h"
#include "exprs/runtime_filter.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h" // IWYU pragma: keep
#include "vec/runtime/shared_hash_table_controller.h"

namespace doris {
// this class used in hash join node
class VRuntimeFilterSlots {
public:
    VRuntimeFilterSlots(
            const std::vector<std::shared_ptr<vectorized::VExprContext>>& build_expr_ctxs,
            const std::vector<TRuntimeFilterDesc>& runtime_filter_descs)
            : _build_expr_context(build_expr_ctxs), _runtime_filter_descs(runtime_filter_descs) {}

    Status init(RuntimeState* state, int64_t hash_table_size, size_t build_bf_cardinality) {
        // runtime filter effect strategy
        // 1. we will ignore IN filter when hash_table_size is too big
        // 2. we will ignore BLOOM filter and MinMax filter when hash_table_size
        // is too small and IN filter has effect

        std::map<int, bool> has_in_filter;

        auto ignore_local_filter = [state](int filter_id) {
            std::vector<IRuntimeFilter*> filters;
            static_cast<void>(state->runtime_filter_mgr()->get_consume_filters(filter_id, filters));
            if (filters.empty()) {
                throw Exception(ErrorCode::INTERNAL_ERROR, "filters empty, filter_id={}",
                                filter_id);
            }
            for (auto filter : filters) {
                filter->set_ignored();
                filter->signal();
            }
        };

        auto ignore_remote_filter = [](IRuntimeFilter* runtime_filter, std::string& msg) {
            runtime_filter->set_ignored();
            runtime_filter->set_ignored_msg(msg);
            RETURN_IF_ERROR(runtime_filter->publish());
            return Status::OK();
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
            if (runtime_filter->expr_order() < 0 ||
                runtime_filter->expr_order() >= _build_expr_context.size()) {
                return Status::InternalError(
                        "runtime_filter meet invalid expr_order, expr_order={}, "
                        "_build_expr_context.size={}",
                        runtime_filter->expr_order(), _build_expr_context.size());
            }

            // do not create 'in filter' when hash_table size over limit
            auto max_in_num = state->runtime_filter_max_in_num();
            bool over_max_in_num = (hash_table_size >= max_in_num);

            bool is_in_filter = (runtime_filter->type() == RuntimeFilterType::IN_FILTER);

            if (over_max_in_num &&
                runtime_filter->type() == RuntimeFilterType::IN_OR_BLOOM_FILTER) {
                runtime_filter->change_to_bloom_filter();
            }

            if (runtime_filter->is_bloomfilter()) {
                RETURN_IF_ERROR(runtime_filter->init_bloom_filter(build_bf_cardinality));
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
                               << " ignore runtime filter("
                               << IRuntimeFilter::to_string(runtime_filter->type()) << " id "
                               << filter_desc.filter_id << ") because: already exists in filter";
                    ignore_local_filter(filter_desc.filter_id);
                    continue;
                }
            } else if (is_in_filter && over_max_in_num) {
                std::string msg = fmt::format(
                        "fragment instance {} ignore runtime filter(in filter id {}) because: "
                        "in_num({}) >= max_in_num({})",
                        print_id(state->fragment_instance_id()), filter_desc.filter_id,
                        hash_table_size, max_in_num);
                RETURN_IF_ERROR(ignore_remote_filter(runtime_filter, msg));
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

    void insert(const std::unordered_set<const vectorized::Block*>& datas) {
        for (int i = 0; i < _build_expr_context.size(); ++i) {
            auto iter = _runtime_filters.find(i);
            if (iter == _runtime_filters.end()) {
                continue;
            }

            int result_column_id = _build_expr_context[i]->get_last_result_column_id();
            for (const auto* it : datas) {
                auto column = it->get_by_position(result_column_id).column;

                std::vector<int> indexs;
                if (const auto* nullable =
                            vectorized::check_and_get_column<vectorized::ColumnNullable>(*column)) {
                    column = nullable->get_nested_column_ptr();
                    const uint8_t* null_map = assert_cast<const vectorized::ColumnUInt8*>(
                                                      nullable->get_null_map_column_ptr().get())
                                                      ->get_data()
                                                      .data();
                    for (int i = 0; i < column->size(); i++) {
                        if (null_map[i]) {
                            continue;
                        }
                        indexs.push_back(i);
                    }
                } else {
                    for (int i = 0; i < column->size(); i++) {
                        indexs.push_back(i);
                    }
                }
                for (auto* filter : iter->second) {
                    filter->insert_batch(column, indexs);
                }
            }
        }
    }

    bool ready_finish_publish() {
        for (auto& pair : _runtime_filters) {
            for (auto filter : pair.second) {
                if (!filter->is_finish_rpc()) {
                    return false;
                }
            }
        }
        return true;
    }

    void finish_publish() {
        for (auto& pair : _runtime_filters) {
            for (auto filter : pair.second) {
                static_cast<void>(filter->join_rpc());
            }
        }
    }

    // publish runtime filter
    Status publish() {
        for (auto& pair : _runtime_filters) {
            for (auto filter : pair.second) {
                RETURN_IF_ERROR(filter->publish());
            }
        }
        return Status::OK();
    }

    void copy_to_shared_context(vectorized::SharedHashTableContextPtr& context) {
        for (auto& it : _runtime_filters) {
            for (auto& filter : it.second) {
                auto& target = context->runtime_filters[filter->filter_id()];
                filter->copy_to_shared_context(target);
            }
        }
    }

    Status copy_from_shared_context(vectorized::SharedHashTableContextPtr& context) {
        for (auto& it : _runtime_filters) {
            for (auto& filter : it.second) {
                auto filter_id = filter->filter_id();
                auto ret = context->runtime_filters.find(filter_id);
                if (ret == context->runtime_filters.end()) {
                    return Status::Aborted("invalid runtime filter id: {}", filter_id);
                }
                static_cast<void>(filter->copy_from_shared_context(ret->second));
            }
        }
        return Status::OK();
    }

    bool empty() { return !_runtime_filters.size(); }

private:
    const std::vector<std::shared_ptr<vectorized::VExprContext>>& _build_expr_context;
    const std::vector<TRuntimeFilterDesc>& _runtime_filter_descs;
    // prob_contition index -> [IRuntimeFilter]
    std::map<int, std::list<IRuntimeFilter*>> _runtime_filters;
};

} // namespace doris
