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

#include "service/tablet_lookup_metric.h"

#include "olap/row_cursor.h"
#include "olap/storage_engine.h"
#include "service/internal_service.h"
#include "util/defer_op.h"
#include "util/key_util.h"
#include "util/runtime_profile.h"
#include "util/thrift_util.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vliteral.h"
#include "vec/sink/vmysql_result_writer.cpp"

namespace doris {

Reusable::~Reusable() {
    for (vectorized::VExprContext* ctx : _output_exprs_ctxs) {
        ctx->close(_runtime_state.get());
    }
}

Status Reusable::init(const TDescriptorTable& t_desc_tbl, const std::vector<TExpr>& output_exprs,
                      size_t block_size) {
    _runtime_state.reset(new RuntimeState());
    RETURN_IF_ERROR(DescriptorTbl::create(_runtime_state->obj_pool(), t_desc_tbl, &_desc_tbl));
    _runtime_state->set_desc_tbl(_desc_tbl);
    _block_pool.resize(block_size);
    for (int i = 0; i < _block_pool.size(); ++i) {
        _block_pool[i] = std::make_unique<vectorized::Block>(tuple_desc()->slots(), 10);
    }

    RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(_runtime_state->obj_pool(), output_exprs,
                                                         &_output_exprs_ctxs));
    RowDescriptor row_desc(tuple_desc(), false);
    // Prepare the exprs to run.
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_exprs_ctxs, _runtime_state.get(), row_desc));
    _create_timestamp = butil::gettimeofday_ms();
    return Status::OK();
}

std::unique_ptr<vectorized::Block> Reusable::get_block() {
    std::lock_guard lock(_block_mutex);
    if (_block_pool.empty()) {
        return std::make_unique<vectorized::Block>(tuple_desc()->slots(), 4);
    }
    auto block = std::move(_block_pool.back());
    _block_pool.pop_back();
    return block;
}

void Reusable::return_block(std::unique_ptr<vectorized::Block>& block) {
    std::lock_guard lock(_block_mutex);
    if (_block_pool.size() > 128) {
        return;
    }
    block->clear_column_data();
    _block_pool.push_back(std::move(block));
}

Status TabletLookupMetric::init(const PTabletKeyLookupRequest* request,
                                PTabletKeyLookupResponse* response) {
    SCOPED_TIMER(&_profile_metrics.init_ns);
    _response = response;
    // using cache
    uint128 uuid {static_cast<uint64_t>(request->uuid().uuid_high()),
                  static_cast<uint64_t>(request->uuid().uuid_low())};
    auto cache_handle = LookupCache::instance().get(uuid);
    _binary_row_format = request->is_binary_row();
    if (cache_handle != nullptr) {
        _reusable = cache_handle;
        _hit_lookup_cache = true;
    } else {
        // init handle
        auto reusable_ptr = std::make_shared<Reusable>();
        TDescriptorTable t_desc_tbl;
        TExprList t_output_exprs;
        uint32_t len = request->desc_tbl().size();
        RETURN_IF_ERROR(
                deserialize_thrift_msg(reinterpret_cast<const uint8_t*>(request->desc_tbl().data()),
                                       &len, false, &t_desc_tbl));
        len = request->output_expr().size();
        RETURN_IF_ERROR(deserialize_thrift_msg(
                reinterpret_cast<const uint8_t*>(request->output_expr().data()), &len, false,
                &t_output_exprs));
        _reusable = reusable_ptr;
        if (uuid != 0) {
            LookupCache::instance().add(uuid, reusable_ptr);
            // could be reused by requests after, pre allocte more blocks
            RETURN_IF_ERROR(reusable_ptr->init(t_desc_tbl, t_output_exprs.exprs, 128));
        } else {
            RETURN_IF_ERROR(reusable_ptr->init(t_desc_tbl, t_output_exprs.exprs, 1));
        }
    }
    _tablet = StorageEngine::instance()->tablet_manager()->get_tablet(request->tablet_id());
    if (_tablet == nullptr) {
        LOG(WARNING) << "failed to do tablet_fetch_data. tablet [" << request->tablet_id()
                     << "] is not exist";
        return Status::NotFound(fmt::format("tablet {} not exist", request->tablet_id()));
    }
    RETURN_IF_ERROR(_init_keys(request));
    _result_block = _reusable->get_block();
    DCHECK(_result_block != nullptr);
    return Status::OK();
}

Status TabletLookupMetric::lookup_up() {
    RETURN_IF_ERROR(_lookup_row_key());
    RETURN_IF_ERROR(_lookup_row_data());
    RETURN_IF_ERROR(_output_data());
    return Status::OK();
}

std::string TabletLookupMetric::print_profile() {
    auto init_us = _profile_metrics.init_ns.value() / 1000;
    auto init_key_us = _profile_metrics.init_key_ns.value() / 1000;
    auto lookup_key_us = _profile_metrics.lookup_key_ns.value() / 1000;
    auto lookup_data_us = _profile_metrics.lookup_data_ns.value() / 1000;
    auto output_data_us = _profile_metrics.output_data_ns.value() / 1000;
    auto total_us = init_us + lookup_key_us + lookup_data_us + output_data_us;
    return fmt::format(
            ""
            "[lookup profile:{}us] init:{}us, init_key:{}us,"
            ""
            ""
            "lookup_key:{}us, lookup_data:{}us, output_data:{}us, hit_lookup_cache:{}"
            ""
            ""
            ", is_binary_row:{}"
            "",
            total_us, init_us, init_key_us, lookup_key_us, lookup_data_us, output_data_us,
            _hit_lookup_cache, _binary_row_format);
}

Status TabletLookupMetric::_init_keys(const PTabletKeyLookupRequest* request) {
    SCOPED_TIMER(&_profile_metrics.init_key_ns);
    // 1. get primary key from conditions
    std::vector<OlapTuple> olap_tuples;
    olap_tuples.resize(request->key_tuples().size());
    for (size_t i = 0; i < request->key_tuples().size(); ++i) {
        const KeyTuple& key_tuple = request->key_tuples(i);
        for (const std::string& key_col : key_tuple.key_column_rep()) {
            olap_tuples[i].add_value(key_col);
        }
    }
    _primary_keys.resize(olap_tuples.size());
    // get row cursor and encode keys
    for (size_t i = 0; i < olap_tuples.size(); ++i) {
        RowCursor cursor;
        RETURN_IF_ERROR(cursor.init_scan_key(_tablet->tablet_schema(), olap_tuples[i].values()));
        RETURN_IF_ERROR(cursor.from_tuple(olap_tuples[i]));
        encode_key_with_padding<RowCursor, true, true>(
                &_primary_keys[i], cursor, _tablet->tablet_schema()->num_short_key_columns(), true);
    }
    return Status::OK();
}

Status TabletLookupMetric::_lookup_row_key() {
    SCOPED_TIMER(&_profile_metrics.lookup_key_ns);
    _row_locations.reserve(_primary_keys.size());
    // 2. lookup row location
    Status st;
    for (size_t i = 0; i < _primary_keys.size(); ++i) {
        RowLocation location;
        st = (_tablet->lookup_row_key(_primary_keys[i], nullptr, &location,
                                      INT32_MAX /*rethink?*/));
        if (st.is_not_found()) {
            continue;
        }
        RETURN_IF_ERROR(st);
        _row_locations.push_back(location);
    }
    return Status::OK();
}

Status TabletLookupMetric::_lookup_row_data() {
    // 3. get values
    SCOPED_TIMER(&_profile_metrics.lookup_data_ns);
    for (size_t i = 0; i < _row_locations.size(); ++i) {
        RETURN_IF_ERROR(_tablet->lookup_row_data(_row_locations[i], _reusable->tuple_desc(),
                                                 _result_block.get()));
    }
    return Status::OK();
}

template <typename MysqlWriter>
static Status _serialize_block(MysqlWriter& mysql_writer, vectorized::Block& block,
                               PTabletKeyLookupResponse* response) {
    RETURN_IF_ERROR(mysql_writer.append_block(block));
    assert(mysql_writer.results().size() == 1);
    uint8_t* buf = nullptr;
    uint32_t len = 0;
    ThriftSerializer ser(false, 4096);
    RETURN_IF_ERROR(ser.serialize(&(mysql_writer.results()[0])->result_batch, &len, &buf));
    response->set_row_batch(std::string((const char*)buf, len));
    return Status::OK();
}

Status TabletLookupMetric::_output_data() {
    // 4. exprs exec and serialize to mysql row batches
    SCOPED_TIMER(&_profile_metrics.output_data_ns);
    if (_result_block->rows()) {
        // TODO reuse mysql_writer
        if (_binary_row_format) {
            vectorized::VMysqlResultWriter<true> mysql_writer(nullptr, _reusable->output_exprs(),
                                                              nullptr);
            RETURN_IF_ERROR(_serialize_block(mysql_writer, *_result_block, _response));
        } else {
            vectorized::VMysqlResultWriter<false> mysql_writer(nullptr, _reusable->output_exprs(),
                                                               nullptr);
            RETURN_IF_ERROR(_serialize_block(mysql_writer, *_result_block, _response));
        }
    } else {
        _response->set_empty_batch(true);
    }
    _reusable->return_block(_result_block);
    return Status::OK();
}

} // namespace doris