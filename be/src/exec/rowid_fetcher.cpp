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
#include "runtime/fragment_mgr.h"  // FragmentMgr
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
#include "vec/data_types/data_type_struct.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/exec/format/orc/vorc_reader.h"
#include "vec/exec/format/parquet/vparquet_reader.h"
#include "vec/exec/scan/file_scanner.h"
#include "vec/functions/function_helpers.h"
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

bool _has_char_type(const vectorized::DataTypePtr& type) {
    switch (type->get_primitive_type()) {
    case TYPE_CHAR: {
        return true;
    }
    case TYPE_ARRAY: {
        const auto* arr_type =
                assert_cast<const vectorized::DataTypeArray*>(remove_nullable(type).get());
        return _has_char_type(arr_type->get_nested_type());
    }
    case TYPE_MAP: {
        const auto* map_type =
                assert_cast<const vectorized::DataTypeMap*>(remove_nullable(type).get());
        return _has_char_type(map_type->get_key_type()) ||
               _has_char_type(map_type->get_value_type());
    }
    case TYPE_STRUCT: {
        const auto* struct_type =
                assert_cast<const vectorized::DataTypeStruct*>(remove_nullable(type).get());
        return std::any_of(
                struct_type->get_elements().begin(), struct_type->get_elements().end(),
                [&](const vectorized::DataTypePtr& dt) -> bool { return _has_char_type(dt); });
    }
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
        const auto type = column_desc->type();
        if (_has_char_type(type)) {
            char_type_idx.push_back(i);
        }
    }
    res_block->shrink_char_type_column_suffix_zero(char_type_idx);
    VLOG_DEBUG << "dump block:" << res_block->dump_data(0, 10);
    return Status::OK();
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
    if (request.request_block_descs_size()) {
        auto tquery_id = ((UniqueId)request.query_id()).to_thrift();
        std::vector<vectorized::Block> result_blocks(request.request_block_descs_size());

        OlapReaderStatistics stats;
        int64_t acquire_tablet_ms = 0;
        int64_t acquire_rowsets_ms = 0;
        int64_t acquire_segments_ms = 0;
        int64_t lookup_row_data_ms = 0;

        int64_t external_init_reader_ms = 0;
        int64_t external_get_block_ms = 0;

        // Add counters for different file mapping types
        std::unordered_map<FileMappingType, int64_t> file_type_counts;

        auto id_file_map =
                ExecEnv::GetInstance()->get_id_manager()->get_id_file_map(request.query_id());
        if (!id_file_map) {
            return Status::InternalError("Backend:{} id_file_map is null, query_id: {}",
                                         BackendOptions::get_localhost(), print_id(tquery_id));
        }

        for (int i = 0; i < request.request_block_descs_size(); ++i) {
            const auto& request_block_desc = request.request_block_descs(i);
            if (request_block_desc.row_id_size() >= 1) {
                // Since this block belongs to the same table, we only need to take the first type for judgment.
                auto first_file_id = request_block_desc.file_id(0);
                auto first_file_mapping = id_file_map->get_file_mapping(first_file_id);
                if (!first_file_mapping) {
                    return Status::InternalError(
                            "Backend:{} file_mapping not found, query_id: {}, file_id: {}",
                            BackendOptions::get_localhost(), print_id(request.query_id()),
                            first_file_id);
                }
                file_type_counts[first_file_mapping->type] += request_block_desc.row_id_size();

                // prepare block char vector shrink for char type
                std::vector<size_t> char_type_idx;
                for (size_t j = 0; j < request_block_desc.column_descs_size(); j++) {
                    auto column_type = request_block_desc.column_descs(j).type();
                    std::transform(column_type.begin(), column_type.end(), column_type.begin(),
                                   [](unsigned char c) { return std::toupper(c); });
                    if (column_type == "CHAR") {
                        char_type_idx.push_back(j);
                    }
                }

                if (first_file_mapping->type == FileMappingType::INTERNAL) {
                    RETURN_IF_ERROR(read_batch_doris_format_row(
                            request_block_desc, id_file_map, tquery_id, result_blocks[i], stats,
                            &acquire_tablet_ms, &acquire_rowsets_ms, &acquire_segments_ms,
                            &lookup_row_data_ms));
                } else {
                    RETURN_IF_ERROR(read_batch_external_row(
                            request_block_desc, id_file_map, first_file_mapping, tquery_id,
                            result_blocks[i], &external_init_reader_ms, &external_get_block_ms));
                }

                // after read the block, shrink char type block
                result_blocks[i].shrink_char_type_column_suffix_zero(char_type_idx);
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
                             "Internal table:"
                             "hit_cached_pages:{}, total_pages_read:{}, compressed_bytes_read:{}, "
                             "io_latency:{}ns, uncompressed_bytes_read:{}, bytes_read:{}, "
                             "acquire_tablet_ms:{}, acquire_rowsets_ms:{}, acquire_segments_ms:{}, "
                             "lookup_row_data_ms:{}, file_types:[{}]; "
                             "External table : init_reader_ms:{}, get_block_ms:{}",
                             stats.cached_pages_num, stats.total_pages_num,
                             stats.compressed_bytes_read, stats.io_ns,
                             stats.uncompressed_bytes_read, stats.bytes_read, acquire_tablet_ms,
                             acquire_rowsets_ms, acquire_segments_ms, lookup_row_data_ms,
                             file_type_stats, external_init_reader_ms, external_get_block_ms);
    }

    if (request.has_gc_id_map() && request.gc_id_map()) {
        ExecEnv::GetInstance()->get_id_manager()->remove_id_file_map(request.query_id());
    }

    return Status::OK();
}

Status RowIdStorageReader::read_batch_doris_format_row(
        const PRequestBlockDesc& request_block_desc, std::shared_ptr<IdFileMap> id_file_map,
        const TUniqueId& query_id, vectorized::Block& result_block, OlapReaderStatistics& stats,
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

        RETURN_IF_ERROR(read_doris_format_row(
                id_file_map, file_mapping, request_block_desc.row_id(j), slots, full_read_schema,
                row_store_read_struct, stats, acquire_tablet_ms, acquire_rowsets_ms,
                acquire_segments_ms, lookup_row_data_ms, iterator_map, result_block));
    }
    return Status::OK();
}

Status RowIdStorageReader::read_batch_external_row(const PRequestBlockDesc& request_block_desc,
                                                   std::shared_ptr<IdFileMap> id_file_map,
                                                   std::shared_ptr<FileMapping> first_file_mapping,
                                                   const TUniqueId& query_id,
                                                   vectorized::Block& result_block,
                                                   int64_t* init_reader_ms, int64_t* get_block_ms) {
    std::vector<SlotDescriptor> slots;
    TFileScanRangeParams rpc_scan_params;
    TupleDescriptor tuple_desc(request_block_desc.desc(), false);
    std::unordered_map<std::string, int> colname_to_slot_id;
    std::unique_ptr<RuntimeState> runtime_state = nullptr;
    std::unique_ptr<RuntimeProfile> runtime_profile;
    runtime_profile = std::make_unique<RuntimeProfile>("ExternalRowIDFetcher");

    std::unique_ptr<vectorized::FileScanner> vfile_scanner_ptr = nullptr;

    {
        auto& external_info = first_file_mapping->get_external_file_info();
        int plan_node_id = external_info.plan_node_id;
        const auto& first_scan_range_desc = external_info.scan_range_desc;

        auto query_ctx = ExecEnv::GetInstance()->fragment_mgr()->get_query_ctx(query_id);
        const auto* old_scan_params = &(query_ctx->file_scan_range_params_map[plan_node_id]);
        rpc_scan_params = *old_scan_params;

        rpc_scan_params.required_slots.clear();
        rpc_scan_params.column_idxs.clear();
        rpc_scan_params.slot_name_to_schema_pos.clear();

        std::set partition_name_set(first_scan_range_desc.columns_from_path_keys.begin(),
                                    first_scan_range_desc.columns_from_path_keys.end());
        slots.reserve(request_block_desc.slots().size());
        for (auto slot_idx = 0; slot_idx < request_block_desc.slots().size(); ++slot_idx) {
            const auto& pslot = request_block_desc.slots().at(slot_idx);

            slots.emplace_back(SlotDescriptor {pslot});
            auto& slot = slots.back();

            tuple_desc.add_slot(&slot);
            colname_to_slot_id.emplace(slot.col_name(), slot.id());
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
            rpc_scan_params.default_value_of_src_slot.emplace(slot.id(), TExpr {});
            rpc_scan_params.required_slots.emplace_back(slot_info);
            rpc_scan_params.slot_name_to_schema_pos.emplace(slot.col_name(), column_idx);
        }

        if (result_block.is_empty_column()) [[likely]] {
            result_block = vectorized::Block(slots, request_block_desc.row_id_size());
        }

        const auto& query_options = query_ctx->get_query_options();
        const auto& query_globals = query_ctx->get_query_globals();

        /*
         * The scan stage needs the information in query_options to generate different behaviors according to the specific variables:
         *  query_options.hive_parquet_use_column_names, query_options.truncate_char_or_varchar_columns,query_globals.time_zone ...
         *
         * To ensure the same behavior as the scan stage, I get query_options query_globals from query_ctx, then create runtime_state
         * and pass it to vfile_scanner so that the runtime_state information is the same as the scan stage and the behavior is also consistent.
         */
        runtime_state = RuntimeState::create_unique(query_id, -1, query_options, query_globals,
                                                    ExecEnv::GetInstance(), query_ctx.get());

        vfile_scanner_ptr = vectorized::FileScanner::create_unique(
                runtime_state.get(), runtime_profile.get(), &rpc_scan_params, &colname_to_slot_id,
                &tuple_desc);

        RETURN_IF_ERROR(vfile_scanner_ptr->prepare_for_read_one_line(first_scan_range_desc));
    }

    for (size_t j = 0; j < request_block_desc.row_id_size(); ++j) {
        auto file_id = request_block_desc.file_id(j);
        auto file_mapping = id_file_map->get_file_mapping(file_id);
        if (!file_mapping) {
            return Status::InternalError(
                    "Backend:{} file_mapping not found, query_id: {}, file_id: {}",
                    BackendOptions::get_localhost(), print_id(query_id), file_id);
        }

        auto& external_info = file_mapping->get_external_file_info();
        auto& scan_range_desc = external_info.scan_range_desc;

        // Clear to avoid reading iceberg position delete file...
        scan_range_desc.table_format_params.iceberg_params = TIcebergFileDesc {};

        // Clear to avoid reading hive transactional delete delta file...
        scan_range_desc.table_format_params.transactional_hive_params = TTransactionalHiveDesc {};

        RETURN_IF_ERROR(vfile_scanner_ptr->read_one_line_from_range(
                scan_range_desc, request_block_desc.row_id(j), &result_block, external_info,
                init_reader_ms, get_block_ms));
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
