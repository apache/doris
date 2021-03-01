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

#include "runtime/data_spliter.h"

#include <thrift/protocol/TDebugProtocol.h>

#include <sstream>

#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gen_cpp/DataSinks_types.h"
#include "runtime/dpp_sink.h"
#include "runtime/load_path_mgr.h"
#include "runtime/mem_tracker.h"
#include "runtime/raw_value.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple_row.h"
#include "service/backend_options.h"
#include "util/file_utils.h"
#include "util/runtime_profile.h"
#include "util/uid_util.h"

namespace doris {

DataSpliter::DataSpliter(const RowDescriptor& row_desc)
        : _obj_pool(new ObjectPool()), _row_desc(row_desc) {}

DataSpliter::~DataSpliter() {}

// We use the PartitionRange to compare here. It should not be a member function of PartitionInfo
// class because there are some other member in it.
static bool compare_part_use_range(const PartitionInfo* v1, const PartitionInfo* v2) {
    return v1->range() < v2->range();
}

Status DataSpliter::from_thrift(ObjectPool* pool, const TDataSplitSink& t_sink,
                                DataSpliter* spliter) {
    VLOG_ROW << "TDataSplitSink: " << apache::thrift::ThriftDebugString(t_sink);

    // Partition Exprs
    RETURN_IF_ERROR(
            Expr::create_expr_trees(pool, t_sink.partition_exprs, &spliter->_partition_expr_ctxs));
    // Partition infos
    int num_parts = t_sink.partition_infos.size();
    if (num_parts == 0) {
        return Status::InternalError("Empty partition info.");
    }
    for (int i = 0; i < num_parts; ++i) {
        PartitionInfo* info = pool->add(new PartitionInfo());
        RETURN_IF_ERROR(PartitionInfo::from_thrift(pool, t_sink.partition_infos[i], info));
        spliter->_partition_infos.push_back(info);
    }

    // partitions should be in ascending order
    std::sort(spliter->_partition_infos.begin(), spliter->_partition_infos.end(),
              compare_part_use_range);

    // schema infos
    for (auto& iter : t_sink.rollup_schemas) {
        RollupSchema* schema = pool->add(new RollupSchema());
        RETURN_IF_ERROR(RollupSchema::from_thrift(pool, iter.second, schema));
        spliter->_rollup_map[iter.first] = schema;
    }

    return Status::OK();
}

Status DataSpliter::prepare(RuntimeState* state) {
    std::stringstream title;
    title << "DataSplitSink (dst_fragment_instance_id=" << print_id(state->fragment_instance_id())
          << ")";
    RETURN_IF_ERROR(DataSink::prepare(state));
    RETURN_IF_ERROR(Expr::prepare(_partition_expr_ctxs, state, _row_desc, _expr_mem_tracker));
    for (auto& iter : _rollup_map) {
        RETURN_IF_ERROR(iter.second->prepare(state, _row_desc, _expr_mem_tracker));
    }
    _profile = state->obj_pool()->add(new RuntimeProfile(title.str()));
    for (auto iter : _partition_infos) {
        RETURN_IF_ERROR(iter->prepare(state, _row_desc, _expr_mem_tracker));
    }
    return Status::OK();
}

Status DataSpliter::open(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::open(_partition_expr_ctxs, state));

    for (auto& iter : _rollup_map) {
        RETURN_IF_ERROR(iter.second->open(state));
    }

    RETURN_IF_ERROR(state->create_load_dir());

    for (auto iter : _partition_infos) {
        RETURN_IF_ERROR(iter->open(state));

        DppSink* dpp_sink = _obj_pool->add(new DppSink(_row_desc, _rollup_map));
        _dpp_sink_vec.push_back(dpp_sink);

        RETURN_IF_ERROR(dpp_sink->init(state));
        _profile->add_child(dpp_sink->profile(), true, nullptr);
    }

    _split_timer = ADD_TIMER(_profile, "process batch");
    _finish_timer = ADD_TIMER(_profile, "sort time");

    return Status::OK();
}

int DataSpliter::binary_find_partition(const PartRangeKey& key) const {
    int low = 0;
    int high = _partition_infos.size() - 1;

    VLOG_ROW << "range key: " << key.debug_string() << std::endl;
    while (low <= high) {
        int mid = low + (high - low) / 2;
        int cmp = _partition_infos[mid]->range().compare_key(key);
        if (cmp == 0) {
            return mid;
        } else if (cmp < 0) { // current < partition[mid]
            low = mid + 1;
        } else {
            high = mid - 1;
        }
    }

    return -1;
}

Status DataSpliter::process_partition(RuntimeState* state, TupleRow* row, PartitionInfo** info,
                                      int32_t* part_index) {
    if (_partition_expr_ctxs.size() == 0) {
        *part_index = 0;
        *info = _partition_infos[0];
        return Status::OK();
    } else {
        // use binary search to get the right partition.
        ExprContext* ctx = _partition_expr_ctxs[0];
        void* partition_val = ctx->get_value(row);
        // construct a PartRangeKey
        PartRangeKey tmpPartKey;
        if (NULL != partition_val) {
            RETURN_IF_ERROR(
                    PartRangeKey::from_value(ctx->root()->type().type, partition_val, &tmpPartKey));
        } else {
            tmpPartKey = PartRangeKey::neg_infinite();
        }

        *part_index = binary_find_partition(tmpPartKey);
        if (*part_index < 0) {
            std::stringstream error_log;
            error_log << "there is no corresponding partition for this key: ";
            ctx->print_value(row, &error_log);
            state->update_num_rows_load_filtered(1);
            return Status::InternalError(error_log.str());
        }
        *info = _partition_infos[*part_index];
    }
    return Status::OK();
}

Status DataSpliter::process_distribute(RuntimeState* state, TupleRow* row,
                                       const PartitionInfo* part, uint32_t* mod) {
    uint32_t hash_val = 0;

    for (auto& ctx : part->distributed_expr_ctxs()) {
        void* partition_val = ctx->get_value(row);
        if (partition_val != NULL) {
            hash_val = RawValue::zlib_crc32(partition_val, ctx->root()->type(), hash_val);
        } else {
            //NULL is treat as 0 when hash
            static const int INT_VALUE = 0;
            static const TypeDescriptor INT_TYPE(TYPE_INT);
            hash_val = RawValue::zlib_crc32(&INT_VALUE, INT_TYPE, hash_val);
        }
    }

    *mod = hash_val % part->distributed_bucket();

    return Status::OK();
}

Status DataSpliter::send_row(RuntimeState* state, const TabletDesc& desc, TupleRow* row,
                             DppSink* dpp_sink) {
    RowBatch* batch = nullptr;
    auto batch_iter = _batch_map.find(desc);
    if (batch_iter == _batch_map.end()) {
        batch = _obj_pool->add(
                new RowBatch(_row_desc, state->batch_size(), _expr_mem_tracker.get()));
        _batch_map[desc] = batch;
    } else {
        batch = batch_iter->second;
    }

    // Add this row to this batch
    int idx = batch->add_row();
    // Just deep copy this row
    row->deep_copy(batch->get_row(idx), _row_desc.tuple_descriptors(), batch->tuple_data_pool(),
                   false);
    batch->commit_last_row();

    // If this batch is full send this to dpp_sink
    if (batch->is_full()) {
        RETURN_IF_ERROR(dpp_sink->add_batch(_obj_pool.get(), state, desc, batch));
        batch->reset();
    }
    return Status::OK();
}

Status DataSpliter::process_one_row(RuntimeState* state, TupleRow* row) {
    TabletDesc desc;
    int32_t part_index = 0;

    // process partition
    PartitionInfo* part = nullptr;
    Status status = process_partition(state, row, &part, &part_index);
    // TODO(lingbin): adjust 'process_partition' function's return value. It is a little inelegant
    // to return another OK when pri-status is not OK.
    // If find no partition, this row should be omitted.
    if (!status.ok()) {
        state->set_error_row_number(state->get_error_row_number() + 1);
        state->set_normal_row_number(state->get_normal_row_number() - 1);

        state->append_error_msg_to_file(row->to_string(_row_desc), status.get_error_msg());
        return Status::OK();
    }

    desc.partition_id = part->id();

    // process distribute
    RETURN_IF_ERROR(process_distribute(state, row, part, &desc.bucket_id));

    // construct dpp_sink map
    _sink_map[desc] = _dpp_sink_vec[part_index];

    // process distribute
    RETURN_IF_ERROR(send_row(state, desc, row, _dpp_sink_vec[part_index]));

    return Status::OK();
}

Status DataSpliter::send(RuntimeState* state, RowBatch* batch) {
    SCOPED_TIMER(_split_timer);
    int num_rows = batch->num_rows();
    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(process_one_row(state, batch->get_row(i)));
    }
    return Status::OK();
}

Status DataSpliter::close(RuntimeState* state, Status close_status) {
    bool is_ok = true;
    Status err_status;
    if (_closed) {
        return Status::OK();
    }
    if (close_status.ok()) {
        SCOPED_TIMER(_finish_timer);
        // Flush data have not been sent
        for (const auto& iter : _batch_map) {
            if (iter.second->num_rows() > 0) {
                DppSink* dpp_sink = _sink_map[iter.first];
                Status status =
                        dpp_sink->add_batch(_obj_pool.get(), state, iter.first, iter.second);
                if (UNLIKELY(is_ok && !status.ok())) {
                    LOG(WARNING) << "add_batch error"
                                 << " err_msg=" << status.get_error_msg();
                    is_ok = false;
                    err_status = status;
                }
                iter.second->clear();
            }
        }
    } else {
        for (const auto& iter : _batch_map) {
            iter.second->clear();
        }
    }
    // finish sink
    for (const auto& iter : _dpp_sink_vec) {
        Status status = iter->finish(state);
        if (UNLIKELY(is_ok && !status.ok())) {
            LOG(WARNING) << "finish dpp_sink error"
                         << " err_msg=" << status.get_error_msg()
                         << " backend=" << BackendOptions::get_localhost();
            is_ok = false;
            err_status = status;
        }
    }
    Expr::close(_partition_expr_ctxs, state);
    for (auto& iter : _rollup_map) {
        iter.second->close(state);
    }
    for (auto iter : _partition_infos) {
        iter->close(state);
    }

    _expr_mem_tracker.reset();
    _closed = true;
    if (is_ok) {
        return Status::OK();
    } else {
        return err_status;
    }
}

} // namespace doris
