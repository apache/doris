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

#include "service/point_query_executor.h"

#include <fmt/format.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <google/protobuf/extension_set.h>
#include <stdlib.h>

#include <climits>
#include <memory>
#include <unordered_map>
#include <vector>

#include "cloud/cloud_tablet.h"
#include "cloud/config.h"
#include "common/consts.h"
#include "common/status.h"
#include "gutil/integral_types.h"
#include "olap/lru_cache.h"
#include "olap/olap_tuple.h"
#include "olap/row_cursor.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/key_util.h"
#include "util/runtime_profile.h"
#include "util/thrift_util.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/jsonb/serialize.h"
#include "vec/sink/vmysql_result_writer.h"

namespace doris {

Reusable::~Reusable() = default;

// get missing and include column ids
// input include_cids : the output expr slots columns unique ids
// missing_cids : the output expr columns that not in row columns cids
static void get_missing_and_include_cids(const TabletSchema& schema,
                                         const std::vector<SlotDescriptor*>& slots,
                                         int target_rs_column_id,
                                         std::unordered_set<int>& missing_cids,
                                         std::unordered_set<int>& include_cids) {
    missing_cids.clear();
    include_cids.clear();
    for (auto* slot : slots) {
        missing_cids.insert(slot->col_unique_id());
    }
    if (target_rs_column_id == -1) {
        // no row store columns
        return;
    }
    const TabletColumn& target_rs_column = schema.column_by_uid(target_rs_column_id);
    DCHECK(target_rs_column.is_row_store_column());
    // The full column group is considered a full match, thus no missing cids
    if (schema.row_columns_uids().empty()) {
        missing_cids.clear();
        return;
    }
    for (int cid : schema.row_columns_uids()) {
        missing_cids.erase(cid);
        include_cids.insert(cid);
    }
}

constexpr static int s_preallocted_blocks_num = 32;

static void extract_slot_ref(const vectorized::VExprSPtr& expr, TupleDescriptor* tuple_desc,
                             std::vector<SlotDescriptor*>& slots) {
    const auto& children = expr->children();
    for (const auto& i : children) {
        extract_slot_ref(i, tuple_desc, slots);
    }

    auto node_type = expr->node_type();
    if (node_type == TExprNodeType::SLOT_REF) {
        int column_id = static_cast<const vectorized::VSlotRef*>(expr.get())->column_id();
        auto* slot_desc = tuple_desc->slots()[column_id];
        slots.push_back(slot_desc);
    }
}

Status Reusable::init(const TDescriptorTable& t_desc_tbl, const std::vector<TExpr>& output_exprs,
                      const TQueryOptions& query_options, const TabletSchema& schema,
                      size_t block_size) {
    _runtime_state = RuntimeState::create_unique();
    _runtime_state->set_query_options(query_options);
    RETURN_IF_ERROR(DescriptorTbl::create(_runtime_state->obj_pool(), t_desc_tbl, &_desc_tbl));
    _runtime_state->set_desc_tbl(_desc_tbl);
    _block_pool.resize(block_size);
    for (auto& i : _block_pool) {
        i = vectorized::Block::create_unique(tuple_desc()->slots(), 2);
        // Name is useless but cost space
        i->clear_names();
    }

    RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(output_exprs, _output_exprs_ctxs));
    RowDescriptor row_desc(tuple_desc(), false);
    // Prepare the exprs to run.
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_exprs_ctxs, _runtime_state.get(), row_desc));
    RETURN_IF_ERROR(vectorized::VExpr::open(_output_exprs_ctxs, _runtime_state.get()));
    _create_timestamp = butil::gettimeofday_ms();
    _data_type_serdes = vectorized::create_data_type_serdes(tuple_desc()->slots());
    _col_default_values.resize(tuple_desc()->slots().size());
    for (int i = 0; i < tuple_desc()->slots().size(); ++i) {
        auto* slot = tuple_desc()->slots()[i];
        _col_uid_to_idx[slot->col_unique_id()] = i;
        _col_default_values[i] = slot->col_default_value();
    }

    // Get the output slot descriptors
    std::vector<SlotDescriptor*> output_slot_descs;
    for (const auto& expr : _output_exprs_ctxs) {
        extract_slot_ref(expr->root(), tuple_desc(), output_slot_descs);
    }

    if (schema.have_column(BeConsts::ROW_STORE_COL)) {
        const auto& column = *DORIS_TRY(schema.column(BeConsts::ROW_STORE_COL));
        _row_store_column_ids = column.unique_id();
    }
    get_missing_and_include_cids(schema, output_slot_descs, _row_store_column_ids,
                                 _missing_col_uids, _include_col_uids);

    return Status::OK();
}

std::unique_ptr<vectorized::Block> Reusable::get_block() {
    std::lock_guard lock(_block_mutex);
    if (_block_pool.empty()) {
        auto block = vectorized::Block::create_unique(tuple_desc()->slots(), 2);
        // Name is useless but cost space
        block->clear_names();
        return block;
    }
    auto block = std::move(_block_pool.back());
    CHECK(block != nullptr);
    _block_pool.pop_back();
    return block;
}

void Reusable::return_block(std::unique_ptr<vectorized::Block>& block) {
    std::lock_guard lock(_block_mutex);
    if (block == nullptr) {
        return;
    }
    block->clear_column_data();
    _block_pool.push_back(std::move(block));
    if (_block_pool.size() > s_preallocted_blocks_num) {
        _block_pool.resize(s_preallocted_blocks_num);
    }
}

LookupConnectionCache* LookupConnectionCache::create_global_instance(size_t capacity) {
    DCHECK(ExecEnv::GetInstance()->get_lookup_connection_cache() == nullptr);
    auto* res = new LookupConnectionCache(capacity);
    return res;
}

RowCache::RowCache(int64_t capacity, int num_shards)
        : LRUCachePolicyTrackingManual(
                  CachePolicy::CacheType::POINT_QUERY_ROW_CACHE, capacity, LRUCacheType::SIZE,
                  config::point_query_row_cache_stale_sweep_time_sec, num_shards) {}

// Create global instance of this class
RowCache* RowCache::create_global_cache(int64_t capacity, uint32_t num_shards) {
    DCHECK(ExecEnv::GetInstance()->get_row_cache() == nullptr);
    auto* res = new RowCache(capacity, num_shards);
    return res;
}

RowCache* RowCache::instance() {
    return ExecEnv::GetInstance()->get_row_cache();
}

bool RowCache::lookup(const RowCacheKey& key, CacheHandle* handle) {
    const std::string& encoded_key = key.encode();
    auto* lru_handle = LRUCachePolicy::lookup(encoded_key);
    if (!lru_handle) {
        // cache miss
        return false;
    }
    *handle = CacheHandle(this, lru_handle);
    return true;
}

void RowCache::insert(const RowCacheKey& key, const Slice& value) {
    char* cache_value = static_cast<char*>(malloc(value.size));
    memcpy(cache_value, value.data, value.size);
    auto* row_cache_value = new RowCacheValue;
    row_cache_value->cache_value = cache_value;
    const std::string& encoded_key = key.encode();
    auto* handle = LRUCachePolicyTrackingManual::insert(encoded_key, row_cache_value, value.size,
                                                        value.size, CachePriority::NORMAL);
    // handle will released
    auto tmp = CacheHandle {this, handle};
}

void RowCache::erase(const RowCacheKey& key) {
    const std::string& encoded_key = key.encode();
    LRUCachePolicy::erase(encoded_key);
}

PointQueryExecutor::~PointQueryExecutor() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            ExecEnv::GetInstance()->point_query_executor_mem_tracker());
    _tablet.reset();
    _reusable.reset();
    _result_block.reset();
    _row_read_ctxs.clear();
}

Status PointQueryExecutor::init(const PTabletKeyLookupRequest* request,
                                PTabletKeyLookupResponse* response) {
    SCOPED_TIMER(&_profile_metrics.init_ns);
    _response = response;
    // using cache
    __int128_t uuid =
            static_cast<__int128_t>(request->uuid().uuid_high()) << 64 | request->uuid().uuid_low();
    SCOPED_ATTACH_TASK(ExecEnv::GetInstance()->point_query_executor_mem_tracker());
    auto cache_handle = LookupConnectionCache::instance()->get(uuid);
    _binary_row_format = request->is_binary_row();
    _tablet = DORIS_TRY(ExecEnv::get_tablet(request->tablet_id()));
    if (cache_handle != nullptr) {
        _reusable = cache_handle;
        _profile_metrics.hit_lookup_cache = true;
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
        TQueryOptions t_query_options;
        len = request->query_options().size();
        if (request->has_query_options()) {
            RETURN_IF_ERROR(deserialize_thrift_msg(
                    reinterpret_cast<const uint8_t*>(request->query_options().data()), &len, false,
                    &t_query_options));
        }
        if (uuid != 0) {
            // could be reused by requests after, pre allocte more blocks
            RETURN_IF_ERROR(reusable_ptr->init(t_desc_tbl, t_output_exprs.exprs, t_query_options,
                                               *_tablet->tablet_schema(),
                                               s_preallocted_blocks_num));
            LookupConnectionCache::instance()->add(uuid, reusable_ptr);
        } else {
            RETURN_IF_ERROR(reusable_ptr->init(t_desc_tbl, t_output_exprs.exprs, t_query_options,
                                               *_tablet->tablet_schema(), 1));
        }
    }
    if (request->has_version() && request->version() >= 0) {
        _version = request->version();
    }
    RETURN_IF_ERROR(_init_keys(request));
    _result_block = _reusable->get_block();
    CHECK(_result_block != nullptr);

    return Status::OK();
}

Status PointQueryExecutor::lookup_up() {
    SCOPED_ATTACH_TASK(ExecEnv::GetInstance()->point_query_executor_mem_tracker());
    RETURN_IF_ERROR(_lookup_row_key());
    RETURN_IF_ERROR(_lookup_row_data());
    RETURN_IF_ERROR(_output_data());
    return Status::OK();
}

std::string PointQueryExecutor::print_profile() {
    auto init_us = _profile_metrics.init_ns.value() / 1000;
    auto init_key_us = _profile_metrics.init_key_ns.value() / 1000;
    auto lookup_key_us = _profile_metrics.lookup_key_ns.value() / 1000;
    auto lookup_data_us = _profile_metrics.lookup_data_ns.value() / 1000;
    auto output_data_us = _profile_metrics.output_data_ns.value() / 1000;
    auto total_us = init_us + lookup_key_us + lookup_data_us + output_data_us;
    auto read_stats = _profile_metrics.read_stats;
    return fmt::format(
            ""
            "[lookup profile:{}us] init:{}us, init_key:{}us,"
            ""
            ""
            "lookup_key:{}us, lookup_data:{}us, output_data:{}us, hit_lookup_cache:{}"
            ""
            ""
            ", is_binary_row:{}, output_columns:{}, total_keys:{}, row_cache_hits:{}"
            ", hit_cached_pages:{}, total_pages_read:{}, compressed_bytes_read:{}, "
            "io_latency:{}ns, "
            "uncompressed_bytes_read:{}, result_data_bytes:{}, row_hits:{}"
            ", rs_column_uid:{}"
            "",
            total_us, init_us, init_key_us, lookup_key_us, lookup_data_us, output_data_us,
            _profile_metrics.hit_lookup_cache, _binary_row_format, _reusable->output_exprs().size(),
            _row_read_ctxs.size(), _profile_metrics.row_cache_hits, read_stats.cached_pages_num,
            read_stats.total_pages_num, read_stats.compressed_bytes_read, read_stats.io_ns,
            read_stats.uncompressed_bytes_read, _profile_metrics.result_data_bytes, _row_hits,
            _reusable->rs_column_uid());
}

Status PointQueryExecutor::_init_keys(const PTabletKeyLookupRequest* request) {
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
    _row_read_ctxs.resize(olap_tuples.size());
    // get row cursor and encode keys
    for (size_t i = 0; i < olap_tuples.size(); ++i) {
        RowCursor cursor;
        RETURN_IF_ERROR(cursor.init_scan_key(_tablet->tablet_schema(), olap_tuples[i].values()));
        RETURN_IF_ERROR(cursor.from_tuple(olap_tuples[i]));
        encode_key_with_padding<RowCursor, true>(&_row_read_ctxs[i]._primary_key, cursor,
                                                 _tablet->tablet_schema()->num_key_columns(), true);
    }
    return Status::OK();
}

Status PointQueryExecutor::_lookup_row_key() {
    SCOPED_TIMER(&_profile_metrics.lookup_key_ns);
    // 2. lookup row location
    Status st;
    if (_version >= 0) {
        CHECK(config::is_cloud_mode()) << "Only cloud mode support snapshot read at present";
        RETURN_IF_ERROR(std::dynamic_pointer_cast<CloudTablet>(_tablet)->sync_rowsets(_version));
    }
    std::vector<RowsetSharedPtr> specified_rowsets;
    {
        std::shared_lock rlock(_tablet->get_header_lock());
        specified_rowsets = _tablet->get_rowset_by_ids(nullptr);
    }
    std::vector<std::unique_ptr<SegmentCacheHandle>> segment_caches(specified_rowsets.size());
    for (size_t i = 0; i < _row_read_ctxs.size(); ++i) {
        RowLocation location;
        if (!config::disable_storage_row_cache) {
            RowCache::CacheHandle cache_handle;
            auto hit_cache = RowCache::instance()->lookup(
                    {_tablet->tablet_id(), _row_read_ctxs[i]._primary_key}, &cache_handle);
            if (hit_cache) {
                _row_read_ctxs[i]._cached_row_data = std::move(cache_handle);
                ++_profile_metrics.row_cache_hits;
                continue;
            }
        }
        // Get rowlocation and rowset, ctx._rowset_ptr will acquire wrap this ptr
        auto rowset_ptr = std::make_unique<RowsetSharedPtr>();
        st = (_tablet->lookup_row_key(_row_read_ctxs[i]._primary_key, false, specified_rowsets,
                                      &location, INT32_MAX /*rethink?*/, segment_caches,
                                      rowset_ptr.get(), false));
        if (st.is<ErrorCode::KEY_NOT_FOUND>()) {
            continue;
        }
        RETURN_IF_ERROR(st);
        _row_read_ctxs[i]._row_location = location;
        // acquire and wrap this rowset
        (*rowset_ptr)->acquire();
        VLOG_DEBUG << "aquire rowset " << (*rowset_ptr)->rowset_id();
        _row_read_ctxs[i]._rowset_ptr = std::unique_ptr<RowsetSharedPtr, decltype(&release_rowset)>(
                rowset_ptr.release(), &release_rowset);
        _row_hits++;
    }
    return Status::OK();
}

Status PointQueryExecutor::_lookup_row_data() {
    // 3. get values
    SCOPED_TIMER(&_profile_metrics.lookup_data_ns);
    for (size_t i = 0; i < _row_read_ctxs.size(); ++i) {
        if (_row_read_ctxs[i]._cached_row_data.valid()) {
            vectorized::JsonbSerializeUtil::jsonb_to_block(
                    _reusable->get_data_type_serdes(),
                    _row_read_ctxs[i]._cached_row_data.data().data,
                    _row_read_ctxs[i]._cached_row_data.data().size, _reusable->get_col_uid_to_idx(),
                    *_result_block, _reusable->get_col_default_values(),
                    _reusable->include_col_uids());
            continue;
        }
        if (!_row_read_ctxs[i]._row_location.has_value()) {
            continue;
        }
        std::string value;
        // fill block by row store
        if (_reusable->rs_column_uid() != -1) {
            bool use_row_cache = !config::disable_storage_row_cache;
            RETURN_IF_ERROR(_tablet->lookup_row_data(
                    _row_read_ctxs[i]._primary_key, _row_read_ctxs[i]._row_location.value(),
                    *(_row_read_ctxs[i]._rowset_ptr), _reusable->tuple_desc(),
                    _profile_metrics.read_stats, value, use_row_cache));
            // serilize value to block, currently only jsonb row formt
            vectorized::JsonbSerializeUtil::jsonb_to_block(
                    _reusable->get_data_type_serdes(), value.data(), value.size(),
                    _reusable->get_col_uid_to_idx(), *_result_block,
                    _reusable->get_col_default_values(), _reusable->include_col_uids());
        }
        if (!_reusable->missing_col_uids().empty()) {
            if (!_reusable->runtime_state()->enable_short_circuit_query_access_column_store()) {
                std::string missing_columns;
                for (int cid : _reusable->missing_col_uids()) {
                    missing_columns += _tablet->tablet_schema()->column_by_uid(cid).name() + ",";
                }
                return Status::InternalError(
                        "Not support column store, set store_row_column=true or row_store_columns "
                        "in table "
                        "properties, missing columns: " +
                        missing_columns + " should be added to row store");
            }
            // fill missing columns by column store
            RowLocation row_loc = _row_read_ctxs[i]._row_location.value();
            BetaRowsetSharedPtr rowset =
                    std::static_pointer_cast<BetaRowset>(_tablet->get_rowset(row_loc.rowset_id));
            SegmentCacheHandle segment_cache;
            RETURN_IF_ERROR(SegmentLoader::instance()->load_segments(rowset, &segment_cache, true));
            // find segment
            auto it = std::find_if(segment_cache.get_segments().cbegin(),
                                   segment_cache.get_segments().cend(),
                                   [&](const segment_v2::SegmentSharedPtr& seg) {
                                       return seg->id() == row_loc.segment_id;
                                   });
            const auto& segment = *it;
            for (int cid : _reusable->missing_col_uids()) {
                int pos = _reusable->get_col_uid_to_idx().at(cid);
                auto row_id = static_cast<segment_v2::rowid_t>(row_loc.row_id);
                vectorized::MutableColumnPtr column =
                        _result_block->get_by_position(pos).column->assume_mutable();
                std::unique_ptr<ColumnIterator> iter;
                RETURN_IF_ERROR(segment->seek_and_read_by_rowid(
                        *_tablet->tablet_schema(), _reusable->tuple_desc()->slots()[pos], row_id,
                        column, _read_stats, iter));
            }
        }
    }
    if (_result_block->columns() > _reusable->include_col_uids().size()) {
        // Padding rows for some columns that no need to output to mysql client
        // eg. SELECT k1,v1,v2 FROM TABLE WHERE k1 = 1, k1 is not in output slots, tuple as bellow
        // TupleDescriptor{id=1, tbl=table_with_column_group}
        // SlotDescriptor{id=8, col=v1, colUniqueId=1 ...}
        // SlotDescriptor{id=9, col=v2, colUniqueId=2 ...}
        // thus missing in include_col_uids and missing_col_uids
        for (size_t i = 0; i < _result_block->columns(); ++i) {
            auto column = _result_block->get_by_position(i).column;
            int padding_rows = _row_hits - column->size();
            if (padding_rows > 0) {
                column->assume_mutable()->insert_many_defaults(padding_rows);
            }
        }
    }
    return Status::OK();
}

template <typename MysqlWriter>
Status serialize_block(RuntimeState* state, MysqlWriter& mysql_writer, vectorized::Block& block,
                       PTabletKeyLookupResponse* response) {
    block.clear_names();
    RETURN_IF_ERROR(mysql_writer.write(state, block));
    assert(mysql_writer.results().size() == 1);
    uint8_t* buf = nullptr;
    uint32_t len = 0;
    ThriftSerializer ser(false, 4096);
    RETURN_IF_ERROR(ser.serialize(&(mysql_writer.results()[0])->result_batch, &len, &buf));
    response->set_row_batch(std::string((const char*)buf, len));
    return Status::OK();
}

Status PointQueryExecutor::_output_data() {
    // 4. exprs exec and serialize to mysql row batches
    SCOPED_TIMER(&_profile_metrics.output_data_ns);
    if (_result_block->rows()) {
        // TODO reuse mysql_writer
        if (_binary_row_format) {
            vectorized::VMysqlResultWriter<true> mysql_writer(nullptr, _reusable->output_exprs(),
                                                              nullptr);
            RETURN_IF_ERROR(mysql_writer.init(_reusable->runtime_state()));
            RETURN_IF_ERROR(serialize_block(_reusable->runtime_state(), mysql_writer,
                                            *_result_block, _response));
        } else {
            vectorized::VMysqlResultWriter<false> mysql_writer(nullptr, _reusable->output_exprs(),
                                                               nullptr);
            RETURN_IF_ERROR(mysql_writer.init(_reusable->runtime_state()));
            RETURN_IF_ERROR(serialize_block(_reusable->runtime_state(), mysql_writer,
                                            *_result_block, _response));
        }
        VLOG_DEBUG << "dump block " << _result_block->dump_data();
    } else {
        _response->set_empty_batch(true);
    }
    _profile_metrics.result_data_bytes = _result_block->bytes();
    _reusable->return_block(_result_block);
    return Status::OK();
}

} // namespace doris
