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
#include "runtime/fragment_mgr.h" // FragmentMgr
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

#include "vec/exec/format/parquet/vparquet_reader.h"
#include "vec/exec/format/orc/vorc_reader.h"

#include "vec/exec/scan/vfile_scanner.h"

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
    if (rows_locs.size() < column_row_ids->size()) {
        return Status::InternalError("Miss matched return row loc count {}, expected {}, input {}",
                                     rows_locs.size(), res_block->rows(), column_row_ids->size());
    }
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
    std::vector<SlotDescriptor> slots;
    slots.reserve(request.slots().size());
    for (const auto& pslot : request.slots()) {
        slots.push_back(SlotDescriptor(pslot));
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
                    [&]() { return tablet->lookup_row_data({}, loc, rowset, stats, *value); },
                    &lookup_row_data_ms));
            row_size = value->size();
            continue;
        }

        // fetch by column store
        if (result_block.is_empty_column()) {
            result_block = vectorized::Block(slots, request.row_locs().size());
        }
        VLOG_DEBUG << "Read row location "
                   << fmt::format("{}, {}, {}, {}", row_location.tablet_id,
                                  row_location.row_location.rowset_id.to_string(),
                                  row_location.row_location.segment_id,
                                  row_location.row_location.row_id);
        for (int x = 0; x < slots.size(); ++x) {
            auto row_id = static_cast<segment_v2::rowid_t>(row_loc.ordinal_id());
            vectorized::MutableColumnPtr column =
                    result_block.get_by_position(x).column->assume_mutable();
            IteratorKey iterator_key {.tablet_id = tablet->tablet_id(),
                                      .rowset_id = rowset_id,
                                      .segment_id = row_loc.segment_id(),
                                      .slot_id = slots[x].id()};
            IteratorItem& iterator_item = iterator_map[iterator_key];
            if (iterator_item.segment == nullptr) {
                // hold the reference
                iterator_map[iterator_key].segment = segment;
            }
            segment = iterator_item.segment;
            RETURN_IF_ERROR(segment->seek_and_read_by_rowid(full_read_schema, &slots[x], row_id,
                                                            column, stats, iterator_item.iterator));
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

Status RowIdStorageReader::read_by_rowids(const PMultiGetRequestV2& request,
                                          PMultiGetResponseV2* response) {
    std::cout <<"read \n";
    if (request.request_block_descs_size()) {
        auto tquery_id = ((UniqueId)request.query_id()).to_thrift();
        std::vector<vectorized::Block> result_blocks(request.request_block_descs_size());

        OlapReaderStatistics stats;
        int64_t acquire_tablet_ms = 0;
        int64_t acquire_rowsets_ms = 0;
        int64_t acquire_segments_ms = 0;
        int64_t lookup_row_data_ms = 0;

        // Add counters for different file mapping types
        std::unordered_map<FileMappingType, int64_t> file_type_counts;

        auto id_file_map =
                ExecEnv::GetInstance()->get_id_manager()->get_id_file_map(request.query_id());
        if (!id_file_map) {
            return Status::InternalError("Backend:{} id_file_map is null, query_id: {}",
                                         BackendOptions::get_localhost(), print_id(tquery_id));
        }

        for (int i = 0; i < request.request_block_descs_size(); ++i) {
            std::cout  << "request.request_block_descs_size() = " << request.request_block_descs_size() <<"\n";

            const auto& request_block_desc = request.request_block_descs(i);
            if (request_block_desc.row_id_size() >= 1) {
                // Since this block belongs to the same table, we only need to take the first type for judgment.
                auto first_file_id = request_block_desc.file_id(0);
                auto first_file_mapping = id_file_map->get_file_mapping(first_file_id);
                if (!first_file_mapping) {
                    return Status::InternalError(
                            "Backend:{} file_mapping not found, query_id: {}, file_id: {}",
                            BackendOptions::get_localhost(), print_id(request.query_id()), first_file_id);
                }

                file_type_counts[first_file_mapping->type] += request_block_desc.row_id_size();

                if (first_file_mapping->type == FileMappingType::DORIS_FORMAT) {
                    RETURN_IF_ERROR(read_batch_doris_format_row(request_block_desc, id_file_map, tquery_id,
                                                result_blocks[i],stats, &acquire_tablet_ms,
                                                &acquire_rowsets_ms, &acquire_segments_ms, &lookup_row_data_ms));
                } else {
                    RETURN_IF_ERROR(read_batch_external_row( request_block_desc, id_file_map, first_file_mapping,
                                                             tquery_id, result_blocks[i]));
                }
            }

            [[maybe_unused]] size_t compressed_size = 0;
            [[maybe_unused]] size_t uncompressed_size = 0;
            int be_exec_version = request.has_be_exec_version() ? request.be_exec_version() : 0;
            RETURN_IF_ERROR(result_blocks[i].serialize(
                    be_exec_version, response->add_blocks()->mutable_block(), &uncompressed_size,
                    &compressed_size, segment_v2::CompressionTypePB::LZ4));
        }

        // Build file type statistics string
        std::string file_type_stats;
        for (const auto& [type, count] : file_type_counts) {
            if (!file_type_stats.empty()) {
                file_type_stats += ", ";
            }
            file_type_stats += fmt::format("{}:{}", type, count);
        }

        LOG(INFO) << "Query stats: "
                  << fmt::format(
                             "hit_cached_pages:{}, total_pages_read:{}, compressed_bytes_read:{}, "
                             "io_latency:{}ns, uncompressed_bytes_read:{}, bytes_read:{}, "
                             "acquire_tablet_ms:{}, acquire_rowsets_ms:{}, acquire_segments_ms:{}, "
                             "lookup_row_data_ms:{}, file_types:[{}]",
                             stats.cached_pages_num, stats.total_pages_num,
                             stats.compressed_bytes_read, stats.io_ns,
                             stats.uncompressed_bytes_read, stats.bytes_read, acquire_tablet_ms,
                             acquire_rowsets_ms, acquire_segments_ms, lookup_row_data_ms,
                             file_type_stats);
    }

    if (request.has_gc_id_map() && request.gc_id_map()) {
        ExecEnv::GetInstance()->get_id_manager()->remove_id_file_map(request.query_id());
    }

    return Status::OK();
}

Status RowIdStorageReader::read_batch_doris_format_row(const PRequestBlockDesc& request_block_desc,
                       std::shared_ptr<IdFileMap> id_file_map, const TUniqueId& query_id,
                       vectorized::Block& result_block,OlapReaderStatistics& stats,
            int64_t* acquire_tablet_ms, int64_t* acquire_rowsets_ms, int64_t* acquire_segments_ms,
            int64_t* lookup_row_data_ms) {

    std::vector<SlotDescriptor> slots;
    slots.reserve(request_block_desc.slots_size());
    for (const auto& pslot : request_block_desc.slots()) {
        slots.push_back(SlotDescriptor(pslot));
    }
    if (result_block.is_empty_column()) [[likely]] {
        result_block = vectorized::Block(slots, request_block_desc.row_id_size());
    }

    TabletSchema full_read_schema;
    for (const ColumnPB& column_pb : request_block_desc.column_descs()) {
        full_read_schema.append_column(TabletColumn(column_pb));
    }
    std::unordered_map<IteratorKey, IteratorItem, HashOfIteratorKey> iterator_map;
    std::string row_store_buffer;
    RowStoreReadStruct row_store_read_struct(row_store_buffer);
    if (request_block_desc.fetch_row_store()) {
        for (int i = 0; i < request_block_desc.slots_size(); ++i) {
            row_store_read_struct.serdes.emplace_back(slots[i].get_data_type_ptr()->get_serde());
            row_store_read_struct.col_uid_to_idx[slots[i].col_unique_id()] = i;
            row_store_read_struct.default_values.emplace_back(slots[i].col_default_value());
        }
    }

    for (size_t j = 0; j < request_block_desc.row_id_size(); ++j) {
        auto file_id = request_block_desc.file_id(j);
        auto file_mapping = id_file_map->get_file_mapping(file_id);
        if (!file_mapping) {
            return Status::InternalError(
                    "Backend:{} file_mapping not found, query_id: {}, file_id: {}",
                    BackendOptions::get_localhost(), print_id(query_id), file_id);
        }

        RETURN_IF_ERROR(read_doris_format_row(id_file_map, file_mapping, request_block_desc.row_id(j), slots,
              full_read_schema, row_store_read_struct, stats, acquire_tablet_ms, acquire_rowsets_ms,
              acquire_segments_ms, lookup_row_data_ms,iterator_map, result_block));
    }
    return Status::OK();
}

Status RowIdStorageReader::read_batch_external_row(const PRequestBlockDesc& request_block_desc,
                                                   std::shared_ptr<IdFileMap> id_file_map,
                                                   std::shared_ptr<FileMapping> first_file_mapping,
                                                   const TUniqueId& query_id,
                                                   vectorized::Block& result_block) {
    std::vector<SlotDescriptor> slots;
    TFileScanRangeParams rpc_scan_params;
    TupleDescriptor tuple_desc(request_block_desc.desc(), false);
    std::unordered_map<std::string, int> colname_to_slot_id;
    std::unique_ptr<RuntimeState> runtime_state = nullptr;
    {
        auto [plan_node_id, external_info] = first_file_mapping->get_external_file_info();
        const auto& first_scan_range_desc = external_info.external_scan_range_desc;

        auto query_ctx = ExecEnv::GetInstance()->fragment_mgr()->get_query_ctx(query_id);
        const auto* old_scan_params= &(query_ctx->file_scan_range_params_map[plan_node_id]);




        std::cout <<"plan node id =" << plan_node_id <<"\n";

        /*
在 scan 阶段  用到的信息如下：
// TFileScanRange represents a set of descriptions of a file and the rules for reading and converting it.
//  TFileScanRangeParams: describe how to read and convert file
//  list<TFileRangeDesc>: file location and range
struct TFileScanRange {
1: optional list<TFileRangeDesc> ranges
// If file_scan_params in TExecPlanFragmentParams is set in TExecPlanFragmentParams
// will use that field, otherwise, use this field.
// file_scan_params in TExecPlanFragmentParams will always be set in query request,
// and TFileScanRangeParams here is used for some other request such as fetch table schema for tvf.
2: optional TFileScanRangeParams params
3: optional TSplitSource split_source
}

struct TFileRangeDesc {
// If load_id is set, this is for stream/routine load.
// If path is set, this is for bulk load.
1: optional Types.TUniqueId load_id
// Path of this range
2: optional string path;
// Offset of this file start
3: optional i64 start_offset;
// Size of this range, if size = -1, this means that will read to the end of file
4: optional i64 size;
// total size of file this range belongs to, -1 means unset
5: optional i64 file_size = -1;
// columns parsed from file path should be after the columns read from file
6: optional list<string> columns_from_path;
// column names from file path, in the same order with columns_from_path
7: optional list<string> columns_from_path_keys;
// For data lake table format
8: optional TTableFormatFileDesc table_format_params
// Use modification time to determine whether the file is changed
9: optional i64 modification_time
10: optional Types.TFileType file_type;
11: optional TFileCompressType compress_type;
// for hive table, different files may have different fs,
// so fs_name should be with TFileRangeDesc
12: optional string fs_name
13: optional TFileFormatType format_type;
}
 *
 */
        // 物化阶段：
        // 通过向  fileMapping 中记录 plan node id , 根据 plan node id 可以从 query ctx 中找到scan 阶段用的 TFileScanRangeParams
        // 因为 scanRangeParams 中有一些比较重要的信息 （创建hdfs/s3 reader的时候需要用到）:
        //     8: optional THdfsParams hdfs_params;
        //    // properties for file such as s3 information
        //    9: optional map<string, string> properties;
        //
        // 向  fileMapping 中记录 TFileRangeDesc external_scan_range_desc， 用处：
        //    文件如果属于某个分区 ，在物化分区列的时候 需要用到TFileRangeDesc 中的 columns_from_path_keys 和 columns_from_path
        //    path, file_type ， modification_time,compress_type ....   用于读取文件
        //    TFileFormatType 就可以区分出来他是iceberg/hive         /hudi/paimon (虽然现在还用不上)
        //
        // Q：为什么不直接开一个新的class 只记录所需要的值，而是记录老的值 然后在这个基础上变更呢
        // A: 1. 考虑到后续如果在 TFileScanRangeParams TFileRangeDesc 的基础上增加新的变量的时候，不需要在新的class 上再增加东西
        //    2.  vfile_scanner 中有一些 考虑到版本之间的兼容性的代码  TFileScanRangeParams 中的一些信息被弃用了，但是 还不得不判断有没有设置
        /*
         * 创建一个新的  TFileScanRangeParams rpc_scan_params 用于 从文件中读取一行
         * 考虑到可能多线程修改同一个 TFileScanRangeParams, 故创建一个新的
         *
         */
        rpc_scan_params = *old_scan_params;
        rpc_scan_params.required_slots.clear();
        rpc_scan_params.column_idxs.clear();
        rpc_scan_params.slot_name_to_schema_pos.clear();

        std::set partition_name_set(first_scan_range_desc.columns_from_path_keys.begin(),
                                    first_scan_range_desc.columns_from_path_keys.end());
        slots.reserve(request_block_desc.slots().size());
        for (auto slot_idx = 0 ;slot_idx  < request_block_desc.slots().size(); ++slot_idx) {
            auto pslot = request_block_desc.slots().at(slot_idx);
            pslot.set_is_materialized(false);//  由于vfile_scanner不处理物化的slot ，我在这里手动将slot改成非物化的
            slots.emplace_back(SlotDescriptor{pslot});
            auto& slot = slots.back();

            tuple_desc.add_slot(&slot);
            colname_to_slot_id.emplace(slot.col_name(),slot.id());
            TFileScanSlotInfo slot_info;
            slot_info.slot_id = slot.id();
            auto column_idx = request_block_desc.column_idxs(slot_idx);
            if (partition_name_set.contains(slot.col_name())) {
                //This is partition column.
                slot_info.is_file_slot = false;
            } else {
                rpc_scan_params.column_idxs.emplace_back(column_idx);
                slot_info.is_file_slot = true;
            }
            rpc_scan_params.required_slots.emplace_back(slot_info);
            rpc_scan_params.slot_name_to_schema_pos.emplace(slot.col_name(), column_idx);
        }

        if (result_block.is_empty_column()) [[likely]] {
            result_block = vectorized::Block(slots, request_block_desc.row_id_size());
        }

        const auto& query_options = query_ctx->get_query_options();
        const auto& query_globals = query_ctx->get_query_globals();
        // scan 阶段需要 query_options 中的信息  根据具体变量的不同 来产生不同的行为：
        //  query_options.hive_parquet_use_column_names
        // query_options.truncate_char_or_varchar_columns
        // query_globals.time_zone
        //为了保证与scan 阶段的行为一致  我从query_ctx 中拿   query_options   query_globals ，
        // 然后创建runtime_state  传到vfile_scanner  这样 runtime_state 信息就与scan阶段的一样  行为也一致。
        runtime_state = RuntimeState::create_unique(query_id, -1, query_options, query_globals,
                                                    ExecEnv::GetInstance(), query_ctx.get());
    }

    for (size_t j = 0; j < request_block_desc.row_id_size(); ++j) {
        auto file_id = request_block_desc.file_id(j);
        auto file_mapping = id_file_map->get_file_mapping(file_id);
        if (!file_mapping) {
            return Status::InternalError(
                    "Backend:{} file_mapping not found, query_id: {}, file_id: {}",
                    BackendOptions::get_localhost(), print_id(query_id), file_id);
        }

        auto [_, external_info] = file_mapping->get_external_file_info();
        auto& scan_range_desc = external_info.external_scan_range_desc;

        // Clear to avoid reading iceberg position delete file...
        scan_range_desc.table_format_params.iceberg_params = TIcebergFileDesc {};

        // 由于vfile_scanner 中有填充分区列 缺失列  以及处理schema change 的逻辑
        // 为了避免把这些逻辑再写一遍，这个决定创建vfile_scanner , 复用其中的逻辑，
        // 也可以避免如果scan有什么行为上的变更，没能同步到物化阶段。（避免 开启topn_lazy优化前后的行为不一致 ）

        vectorized::VFileScanner vfile_scanner{runtime_state.get(), &rpc_scan_params, scan_range_desc,
                                               &colname_to_slot_id, &tuple_desc};
        RETURN_IF_ERROR(vfile_scanner.read_one_line_from_current_range(
                request_block_desc.row_id(j), &result_block, external_info));
    }
    return Status::OK();
}


Status RowIdStorageReader::read_doris_format_row(
        const std::shared_ptr<IdFileMap>& id_file_map,
        const std::shared_ptr<FileMapping>& file_mapping, int64_t row_id,
        std::vector<SlotDescriptor>& slots, const TabletSchema& full_read_schema,
        RowStoreReadStruct& row_store_read_struct, OlapReaderStatistics& stats,
        int64_t* acquire_tablet_ms, int64_t* acquire_rowsets_ms, int64_t* acquire_segments_ms,
        int64_t* lookup_row_data_ms,
        std::unordered_map<IteratorKey, IteratorItem, HashOfIteratorKey>& iterator_map,
        vectorized::Block& result_block) {
    auto [tablet_id, rowset_id, segment_id] = file_mapping->get_doris_format_info();
    BaseTabletSPtr tablet = scope_timer_run(
            [&]() {
                auto res = ExecEnv::get_tablet(tablet_id);
                return !res.has_value() ? nullptr
                                        : std::dynamic_pointer_cast<BaseTablet>(res.value());
            },
            acquire_tablet_ms);
    if (!tablet) {
        return Status::InternalError(
                "Backend:{} tablet not found, tablet_id: {}, rowset_id: {}, segment_id: {}, "
                "row_id: {}",
                BackendOptions::get_localhost(), tablet_id, rowset_id.to_string(), segment_id,
                row_id);
    }

    BetaRowsetSharedPtr rowset = std::static_pointer_cast<BetaRowset>(
            scope_timer_run([&]() { return id_file_map->get_temp_rowset(tablet_id, rowset_id); },
                            acquire_rowsets_ms));
    if (!rowset) {
        return Status::InternalError(
                "Backend:{} rowset_id not found, tablet_id: {}, rowset_id: {}, segment_id: {}, "
                "row_id: {}",
                BackendOptions::get_localhost(), tablet_id, rowset_id.to_string(), segment_id,
                row_id);
    }

    SegmentCacheHandle segment_cache;
    RETURN_IF_ERROR(scope_timer_run(
            [&]() {
                return SegmentLoader::instance()->load_segments(rowset, &segment_cache, true);
            },
            acquire_segments_ms));

    auto it =
            std::find_if(segment_cache.get_segments().cbegin(), segment_cache.get_segments().cend(),
                         [segment_id](const segment_v2::SegmentSharedPtr& seg) {
                             return seg->id() == segment_id;
                         });
    if (it == segment_cache.get_segments().end()) {
        return Status::InternalError(
                "Backend:{} segment not found, tablet_id: {}, rowset_id: {}, segment_id: {}, "
                "row_id: {}",
                BackendOptions::get_localhost(), tablet_id, rowset_id.to_string(), segment_id,
                row_id);
    }
    segment_v2::SegmentSharedPtr segment = *it;

    // if row_store_read_struct not empty, means the line we should read from row_store
    if (!row_store_read_struct.default_values.empty()) {
        CHECK(tablet->tablet_schema()->has_row_store_for_all_columns());
        RowLocation loc(rowset_id, segment->id(), row_id);
        row_store_read_struct.row_store_buffer.clear();
        RETURN_IF_ERROR(scope_timer_run(
                [&]() {
                    return tablet->lookup_row_data({}, loc, rowset, stats,
                                                   row_store_read_struct.row_store_buffer);
                },
                lookup_row_data_ms));

        vectorized::JsonbSerializeUtil::jsonb_to_block(
                row_store_read_struct.serdes, row_store_read_struct.row_store_buffer.data(),
                row_store_read_struct.row_store_buffer.size(), row_store_read_struct.col_uid_to_idx,
                result_block, row_store_read_struct.default_values, {});
    } else {
        for (int x = 0; x < slots.size(); ++x) {
            vectorized::MutableColumnPtr column =
                    result_block.get_by_position(x).column->assume_mutable();
            IteratorKey iterator_key {.tablet_id = tablet_id,
                                      .rowset_id = rowset_id,
                                      .segment_id = segment_id,
                                      .slot_id = slots[x].id()};
            IteratorItem& iterator_item = iterator_map[iterator_key];
            if (iterator_item.segment == nullptr) {
                iterator_map[iterator_key].segment = segment;
            }
            segment = iterator_item.segment;
            RETURN_IF_ERROR(segment->seek_and_read_by_rowid(full_read_schema, &slots[x], row_id,
                                                            column, stats, iterator_item.iterator));
        }
    }

    return Status::OK();
}

} // namespace doris
