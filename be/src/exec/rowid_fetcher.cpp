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

#include "exec/rowid_fetcher.h"

#include <brpc/callback.h>
#include <butil/endpoint.h>
#include <fmt/format.h>
#include <gen_cpp/data.pb.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bthread/countdown_event.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cloud/config.h"
#include "common/config.h"
#include "common/consts.h"
#include "common/exception.h"
#include "exec/tablet_info.h" // DorisNodesInfo
#include "olap/olap_common.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/storage_engine.h"
#include "olap/tablet_fwd.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"      // ExecEnv
#include "runtime/runtime_state.h" // RuntimeState
#include "runtime/types.h"
#include "util/brpc_client_cache.h" // BrpcClientCache
#include "util/defer_op.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h" // Block
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/jsonb/serialize.h"

namespace doris {

Status RowIDFetcher::init() {
    DorisNodesInfo nodes_info;
    nodes_info.setNodes(_fetch_option.t_fetch_opt.nodes_info);
    for (auto [node_id, node_info] : nodes_info.nodes_info()) {
        auto client = ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(
                node_info.host, node_info.brpc_port);
        if (!client) {
            LOG(WARNING) << "Get rpc stub failed, host=" << node_info.host
                         << ", port=" << node_info.brpc_port;
            return Status::InternalError("RowIDFetcher failed to init rpc client, host={}, port={}",
                                         node_info.host, node_info.brpc_port);
        }
        _stubs.push_back(client);
    }
    return Status::OK();
}

PMultiGetRequest RowIDFetcher::_init_fetch_request(const vectorized::ColumnString& row_locs) const {
    PMultiGetRequest mget_req;
    _fetch_option.desc->to_protobuf(mget_req.mutable_desc());
    for (SlotDescriptor* slot : _fetch_option.desc->slots()) {
        // ignore rowid
        if (slot->col_name() == BeConsts::ROWID_COL) {
            continue;
        }
        slot->to_protobuf(mget_req.add_slots());
    }
    for (size_t i = 0; i < row_locs.size(); ++i) {
        PRowLocation row_loc;
        StringRef row_id_rep = row_locs.get_data_at(i);
        // TODO: When transferring data between machines with different byte orders (endianness),
        // not performing proper handling may lead to issues in parsing and exchanging the data.
        auto location = reinterpret_cast<const GlobalRowLoacation*>(row_id_rep.data);
        row_loc.set_tablet_id(location->tablet_id);
        row_loc.set_rowset_id(location->row_location.rowset_id.to_string());
        row_loc.set_segment_id(location->row_location.segment_id);
        row_loc.set_ordinal_id(location->row_location.row_id);
        *mget_req.add_row_locs() = std::move(row_loc);
    }
    // Set column desc
    for (const TColumn& tcolumn : _fetch_option.t_fetch_opt.column_desc) {
        TabletColumn column(tcolumn);
        column.to_schema_pb(mget_req.add_column_desc());
    }
    PUniqueId& query_id = *mget_req.mutable_query_id();
    query_id.set_hi(_fetch_option.runtime_state->query_id().hi);
    query_id.set_lo(_fetch_option.runtime_state->query_id().lo);
    mget_req.set_be_exec_version(_fetch_option.runtime_state->be_exec_version());
    mget_req.set_fetch_row_store(_fetch_option.t_fetch_opt.fetch_row_store);
    return mget_req;
}

static void fetch_callback(bthread::CountdownEvent* counter) {
    Defer __defer([&] { counter->signal(); });
}

Status RowIDFetcher::_merge_rpc_results(const PMultiGetRequest& request,
                                        const std::vector<PMultiGetResponse>& rsps,
                                        const std::vector<brpc::Controller>& cntls,
                                        vectorized::Block* output_block,
                                        std::vector<PRowLocation>* rows_id) const {
    output_block->clear();
    for (const auto& cntl : cntls) {
        if (cntl.Failed()) {
            LOG(WARNING) << "Failed to fetch meet rpc error:" << cntl.ErrorText()
                         << ", host:" << cntl.remote_side();
            return Status::InternalError(cntl.ErrorText());
        }
    }
    vectorized::DataTypeSerDeSPtrs serdes;
    std::unordered_map<uint32_t, uint32_t> col_uid_to_idx;
    std::vector<std::string> default_values;
    default_values.resize(_fetch_option.desc->slots().size());
    auto merge_function = [&](const PMultiGetResponse& resp) {
        Status st(Status::create(resp.status()));
        if (!st.ok()) {
            LOG(WARNING) << "Failed to fetch " << st.to_string();
            return st;
        }
        for (const PRowLocation& row_id : resp.row_locs()) {
            rows_id->push_back(row_id);
        }
        // Merge binary rows
        if (request.fetch_row_store()) {
            CHECK(resp.row_locs().size() == resp.binary_row_data_size());
            if (output_block->is_empty_column()) {
                *output_block = vectorized::Block(_fetch_option.desc->slots(), 1);
            }
            if (serdes.empty() && col_uid_to_idx.empty()) {
                serdes = vectorized::create_data_type_serdes(_fetch_option.desc->slots());
                for (int i = 0; i < _fetch_option.desc->slots().size(); ++i) {
                    col_uid_to_idx[_fetch_option.desc->slots()[i]->col_unique_id()] = i;
                    default_values[i] = _fetch_option.desc->slots()[i]->col_default_value();
                }
            }
            for (int i = 0; i < resp.binary_row_data_size(); ++i) {
                vectorized::JsonbSerializeUtil::jsonb_to_block(
                        serdes, resp.binary_row_data(i).data(), resp.binary_row_data(i).size(),
                        col_uid_to_idx, *output_block, default_values, {});
            }
            return Status::OK();
        }
        // Merge partial blocks
        vectorized::Block partial_block;
        RETURN_IF_ERROR(partial_block.deserialize(resp.block()));
        if (partial_block.is_empty_column()) {
            return Status::OK();
        }
        CHECK(resp.row_locs().size() == partial_block.rows());
        if (output_block->is_empty_column()) {
            output_block->swap(partial_block);
        } else if (partial_block.columns() != output_block->columns()) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "Merge block not match, self:[{}], input:[{}], ", output_block->dump_types(),
                    partial_block.dump_types());
        } else {
            for (int i = 0; i < output_block->columns(); ++i) {
                output_block->get_by_position(i).column->assume_mutable()->insert_range_from(
                        *partial_block.get_by_position(i)
                                 .column->convert_to_full_column_if_const()
                                 .get(),
                        0, partial_block.rows());
            }
        }
        return Status::OK();
    };

    for (const auto& resp : rsps) {
        RETURN_IF_ERROR(merge_function(resp));
    }
    return Status::OK();
}

bool _has_char_type(const TypeDescriptor& desc) {
    switch (desc.type) {
    case TYPE_CHAR:
        return true;
    case TYPE_ARRAY:
    case TYPE_MAP:
    case TYPE_STRUCT:
        for (int idx = 0; idx < desc.children.size(); ++idx) {
            if (_has_char_type(desc.children[idx])) {
                return true;
            }
        }
        return false;
    default:
        return false;
    }
}

Status RowIDFetcher::fetch(const vectorized::ColumnPtr& column_row_ids,
                           vectorized::Block* res_block) {
    CHECK(!_stubs.empty());
    PMultiGetRequest mget_req = _init_fetch_request(assert_cast<const vectorized::ColumnString&>(
            *vectorized::remove_nullable(column_row_ids).get()));
    std::vector<PMultiGetResponse> resps(_stubs.size());
    std::vector<brpc::Controller> cntls(_stubs.size());
    bthread::CountdownEvent counter(_stubs.size());
    for (size_t i = 0; i < _stubs.size(); ++i) {
        cntls[i].set_timeout_ms(config::fetch_rpc_timeout_seconds * 1000);
        auto callback = brpc::NewCallback(fetch_callback, &counter);
        _stubs[i]->multiget_data(&cntls[i], &mget_req, &resps[i], callback);
    }
    counter.wait();

    // Merge
    std::vector<PRowLocation> rows_locs;
    rows_locs.reserve(rows_locs.size());
    RETURN_IF_ERROR(_merge_rpc_results(mget_req, resps, cntls, res_block, &rows_locs));
    // Final sort by row_ids sequence, since row_ids is already sorted if need
    std::map<GlobalRowLoacation, size_t> positions;
    for (size_t i = 0; i < rows_locs.size(); ++i) {
        RowsetId rowset_id;
        rowset_id.init(rows_locs[i].rowset_id());
        GlobalRowLoacation grl(rows_locs[i].tablet_id(), rowset_id, rows_locs[i].segment_id(),
                               rows_locs[i].ordinal_id());
        positions[grl] = i;
    };
    // TODO remove this warning code
    if (positions.size() < rows_locs.size()) {
        LOG(WARNING) << "contains duplicated row entry";
    }
    vectorized::IColumn::Permutation permutation;
    permutation.reserve(column_row_ids->size());
    for (size_t i = 0; i < column_row_ids->size(); ++i) {
        auto location =
                reinterpret_cast<const GlobalRowLoacation*>(column_row_ids->get_data_at(i).data);
        permutation.push_back(positions[*location]);
    }
    for (size_t i = 0; i < res_block->columns(); ++i) {
        res_block->get_by_position(i).column =
                res_block->get_by_position(i).column->permute(permutation, permutation.size());
    }
    // Check row consistency
    RETURN_IF_CATCH_EXCEPTION(res_block->check_number_of_rows());
    // shrink for char type
    std::vector<size_t> char_type_idx;
    for (size_t i = 0; i < _fetch_option.desc->slots().size(); i++) {
        const auto& column_desc = _fetch_option.desc->slots()[i];
        const TypeDescriptor& type_desc = column_desc->type();
        if (_has_char_type(type_desc)) {
            char_type_idx.push_back(i);
        }
    }
    res_block->shrink_char_type_column_suffix_zero(char_type_idx);
    VLOG_DEBUG << "dump block:" << res_block->dump_data(0, 10);
    return Status::OK();
}

template <typename Func>
auto scope_timer_run(Func fn, int64_t* cost) -> decltype(fn()) {
    MonotonicStopWatch watch;
    watch.start();
    auto res = fn();
    *cost += watch.elapsed_time() / 1000 / 1000;
    return res;
}

struct IteratorKey {
    int64_t tablet_id;
    RowsetId rowset_id;
    uint64_t segment_id;
    int slot_id;

    // unordered map std::equal_to
    bool operator==(const IteratorKey& rhs) const {
        return tablet_id == rhs.tablet_id && rowset_id == rhs.rowset_id &&
               segment_id == rhs.segment_id && slot_id == rhs.slot_id;
    }
};

struct HashOfIteratorKey {
    size_t operator()(const IteratorKey& key) const {
        size_t seed = 0;
        seed = HashUtil::hash64(&key.tablet_id, sizeof(key.tablet_id), seed);
        seed = HashUtil::hash64(&key.rowset_id.hi, sizeof(key.rowset_id.hi), seed);
        seed = HashUtil::hash64(&key.rowset_id.mi, sizeof(key.rowset_id.mi), seed);
        seed = HashUtil::hash64(&key.rowset_id.lo, sizeof(key.rowset_id.lo), seed);
        seed = HashUtil::hash64(&key.segment_id, sizeof(key.segment_id), seed);
        seed = HashUtil::hash64(&key.slot_id, sizeof(key.slot_id), seed);
        return seed;
    }
};

struct IteratorItem {
    std::unique_ptr<ColumnIterator> iterator;
    // for holding the reference of segment to avoid use after release
    SegmentSharedPtr segment;
};

Status RowIdStorageReader::read_by_rowids(const PMultiGetRequest& request,
                                          PMultiGetResponse* response) {
    // read from storage engine row id by row id
    OlapReaderStatistics stats;
    vectorized::Block result_block;
    int64_t acquire_tablet_ms = 0;
    int64_t acquire_rowsets_ms = 0;
    int64_t acquire_segments_ms = 0;
    int64_t lookup_row_data_ms = 0;

    // init desc
    TupleDescriptor desc(request.desc());
    std::vector<SlotDescriptor> slots;
    slots.reserve(request.slots().size());
    for (const auto& pslot : request.slots()) {
        slots.push_back(SlotDescriptor(pslot));
        desc.add_slot(&slots.back());
    }

    // init read schema
    TabletSchema full_read_schema;
    for (const ColumnPB& column_pb : request.column_desc()) {
        full_read_schema.append_column(TabletColumn(column_pb));
    }

    std::unordered_map<IteratorKey, IteratorItem, HashOfIteratorKey> iterator_map;
    // read row by row
    for (size_t i = 0; i < request.row_locs_size(); ++i) {
        const auto& row_loc = request.row_locs(i);
        MonotonicStopWatch watch;
        watch.start();
        BaseTabletSPtr tablet = scope_timer_run(
                [&]() {
                    auto res = ExecEnv::get_tablet(row_loc.tablet_id());
                    return !res.has_value() ? nullptr
                                            : std::dynamic_pointer_cast<BaseTablet>(res.value());
                },
                &acquire_tablet_ms);
        RowsetId rowset_id;
        rowset_id.init(row_loc.rowset_id());
        if (!tablet) {
            continue;
        }
        // We ensured it's rowset is not released when init Tablet reader param, rowset->update_delayed_expired_timestamp();
        BetaRowsetSharedPtr rowset = std::static_pointer_cast<BetaRowset>(scope_timer_run(
                [&]() {
                    return ExecEnv::GetInstance()->storage_engine().get_quering_rowset(rowset_id);
                },
                &acquire_rowsets_ms));
        if (!rowset) {
            LOG(INFO) << "no such rowset " << rowset_id;
            continue;
        }
        size_t row_size = 0;
        Defer _defer([&]() {
            LOG_EVERY_N(INFO, 100)
                    << "multiget_data single_row, cost(us):" << watch.elapsed_time() / 1000
                    << ", row_size:" << row_size;
            *response->add_row_locs() = row_loc;
        });
        // TODO: supoort session variable enable_page_cache and disable_file_cache if necessary.
        SegmentCacheHandle segment_cache;
        RETURN_IF_ERROR(scope_timer_run(
                [&]() {
                    return SegmentLoader::instance()->load_segments(rowset, &segment_cache, true);
                },
                &acquire_segments_ms));
        // find segment
        auto it = std::find_if(segment_cache.get_segments().cbegin(),
                               segment_cache.get_segments().cend(),
                               [&row_loc](const segment_v2::SegmentSharedPtr& seg) {
                                   return seg->id() == row_loc.segment_id();
                               });
        if (it == segment_cache.get_segments().end()) {
            continue;
        }
        segment_v2::SegmentSharedPtr segment = *it;
        GlobalRowLoacation row_location(row_loc.tablet_id(), rowset->rowset_id(),
                                        row_loc.segment_id(), row_loc.ordinal_id());
        // fetch by row store, more effcient way
        if (request.fetch_row_store()) {
            CHECK(tablet->tablet_schema()->has_row_store_for_all_columns());
            RowLocation loc(rowset_id, segment->id(), row_loc.ordinal_id());
            string* value = response->add_binary_row_data();
            RETURN_IF_ERROR(scope_timer_run(
                    [&]() {
                        return tablet->lookup_row_data({}, loc, rowset, &desc, stats, *value);
                    },
                    &lookup_row_data_ms));
            row_size = value->size();
            continue;
        }

        // fetch by column store
        if (result_block.is_empty_column()) {
            result_block = vectorized::Block(desc.slots(), request.row_locs().size());
        }
        VLOG_DEBUG << "Read row location "
                   << fmt::format("{}, {}, {}, {}", row_location.tablet_id,
                                  row_location.row_location.rowset_id.to_string(),
                                  row_location.row_location.segment_id,
                                  row_location.row_location.row_id);
        for (int x = 0; x < desc.slots().size(); ++x) {
            auto row_id = static_cast<segment_v2::rowid_t>(row_loc.ordinal_id());
            vectorized::MutableColumnPtr column =
                    result_block.get_by_position(x).column->assume_mutable();
            IteratorKey iterator_key {.tablet_id = tablet->tablet_id(),
                                      .rowset_id = rowset_id,
                                      .segment_id = row_loc.segment_id(),
                                      .slot_id = desc.slots()[x]->id()};
            IteratorItem& iterator_item = iterator_map[iterator_key];
            if (iterator_item.segment == nullptr) {
                // hold the reference
                iterator_map[iterator_key].segment = segment;
            }
            segment = iterator_item.segment;
            RETURN_IF_ERROR(segment->seek_and_read_by_rowid(full_read_schema, desc.slots()[x],
                                                            row_id, column, stats,
                                                            iterator_item.iterator));
        }
    }
    // serialize block if not empty
    if (!result_block.is_empty_column()) {
        VLOG_DEBUG << "dump block:" << result_block.dump_data(0, 10)
                   << ", be_exec_version:" << request.be_exec_version();
        [[maybe_unused]] size_t compressed_size = 0;
        [[maybe_unused]] size_t uncompressed_size = 0;
        int be_exec_version = request.has_be_exec_version() ? request.be_exec_version() : 0;
        RETURN_IF_ERROR(result_block.serialize(be_exec_version, response->mutable_block(),
                                               &uncompressed_size, &compressed_size,
                                               segment_v2::CompressionTypePB::LZ4));
    }

    LOG(INFO) << "Query stats: "
              << fmt::format(
                         "hit_cached_pages:{}, total_pages_read:{}, compressed_bytes_read:{}, "
                         "io_latency:{}ns, "
                         "uncompressed_bytes_read:{},"
                         "bytes_read:{},"
                         "acquire_tablet_ms:{}, acquire_rowsets_ms:{}, acquire_segments_ms:{}, "
                         "lookup_row_data_ms:{}",
                         stats.cached_pages_num, stats.total_pages_num, stats.compressed_bytes_read,
                         stats.io_ns, stats.uncompressed_bytes_read, stats.bytes_read,
                         acquire_tablet_ms, acquire_rowsets_ms, acquire_segments_ms,
                         lookup_row_data_ms);
    return Status::OK();
}

} // namespace doris
