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

#include "load/delta_writer/push_handler.h"

#include <fmt/core.h>
#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/MasterService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>

#include <algorithm>
#include <iostream>
#include <mutex>
#include <new>
#include <queue>
#include <shared_mutex>
#include <type_traits>
#include <utility>

#include "common/cast_set.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "core/block/block.h"
#include "format_v2/table/load_reader.h"
#include "io/hdfs_builder.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "storage/compaction/cumulative_compaction_time_series_policy.h"
#include "storage/delete/delete_handler.h"
#include "storage/olap_define.h"
#include "storage/rowset/pending_rowset_helper.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/txn/txn_manager.h"
#include "util/time.h"

namespace doris {
using namespace ErrorCode;

// Process push command, the main logical is as follows:
//    a. related tablets not exist:
//        current table isn't in schemachange state, only push for current
//        tablet
//    b. related tablets exist
//       I.  current tablet is old table (cur.creation_time <
//       related.creation_time):
//           push for current table and than convert data for related tables
//       II. current table is new table:
//           this usually means schema change is over,
//           clear schema change info in both current tablet and related
//           tablets, finally we will only push for current tablets. this is
//           very useful in rollup action.
Status PushHandler::process_streaming_ingestion(TabletSharedPtr tablet, const TPushReq& request,
                                                PushType push_type,
                                                std::vector<TTabletInfo>* tablet_info_vec) {
    LOG(INFO) << "begin to realtime push. tablet=" << tablet->tablet_id()
              << ", transaction_id=" << request.transaction_id;

    Status res = Status::OK();
    _request = request;

    RETURN_IF_ERROR(DescriptorTbl::create(&_pool, _request.desc_tbl, &_desc_tbl));

    res = _do_streaming_ingestion(tablet, request, push_type, tablet_info_vec);

    if (res.ok()) {
        if (tablet_info_vec != nullptr) {
            TTabletInfo tablet_info;
            tablet_info.tablet_id = tablet->tablet_id();
            tablet_info.schema_hash = tablet->schema_hash();
            RETURN_IF_ERROR(_engine.tablet_manager()->report_tablet_info(&tablet_info));
            tablet_info_vec->push_back(tablet_info);
        }
        LOG(INFO) << "process realtime push successfully. "
                  << "tablet=" << tablet->tablet_id() << ", partition_id=" << request.partition_id
                  << ", transaction_id=" << request.transaction_id;
    }

    return res;
}

Status PushHandler::_do_streaming_ingestion(TabletSharedPtr tablet, const TPushReq& request,
                                            PushType push_type,
                                            std::vector<TTabletInfo>* tablet_info_vec) {
    // add transaction in engine, then check sc status
    // lock, prevent sc handler checking transaction concurrently
    if (tablet == nullptr) {
        return Status::Error<TABLE_NOT_FOUND>(
                "PushHandler::_do_streaming_ingestion input tablet is nullptr");
    }

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);

    RETURN_IF_ERROR(
            tablet->prepare_txn(request.partition_id, request.transaction_id, load_id, false));

    // not call validate request here, because realtime load does not
    // contain version info

    Status res;
    // check delete condition if push for delete
    std::queue<DeletePredicatePB> del_preds;
    if (push_type == PushType::PUSH_FOR_DELETE) {
        DeletePredicatePB del_pred;
        TabletSchema tablet_schema;
        tablet_schema.copy_from(*tablet->tablet_schema());
        if (!request.columns_desc.empty() && request.columns_desc[0].col_unique_id >= 0) {
            tablet_schema.clear_columns();
            for (const auto& column_desc : request.columns_desc) {
                tablet_schema.append_column(TabletColumn(column_desc));
            }
        }
        res = DeleteHandler::generate_delete_predicate(tablet_schema, request.delete_conditions,
                                                       &del_pred);
        del_preds.push(del_pred);
        if (!res.ok()) {
            LOG(WARNING) << "fail to generate delete condition. res=" << res
                         << ", tablet=" << tablet->tablet_id();
            return res;
        }
    }

    int32_t max_version_config = tablet->max_version_config();
    // check if version number exceed limit
    if (tablet->exceed_version_limit(max_version_config)) {
        return Status::Status::Error<TOO_MANY_VERSION>(
                "failed to push data. version count: {}, exceed limit: {}, tablet: {}. Please "
                "reduce the frequency of loading data or adjust the max_tablet_version_num or "
                "time_series_max_tablet_version_num in "
                "be.conf to a larger value.",
                tablet->version_count(), max_version_config, tablet->tablet_id());
    }

    auto version_count = tablet->version_count() + tablet->stale_version_count();
    if (tablet->avg_rs_meta_serialize_size() * version_count >
        config::tablet_meta_serialize_size_limit) {
        return Status::Error<TOO_MANY_VERSION>(
                "failed to init rowset builder. meta serialize size : {}, exceed limit: {}, "
                "tablet: {}. Please reduce the frequency of loading data or adjust the "
                "max_tablet_version_num in be.conf to a larger value.",
                tablet->avg_rs_meta_serialize_size() * version_count,
                config::tablet_meta_serialize_size_limit, tablet->tablet_id());
    }

    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->copy_from(*tablet->tablet_schema());
    if (!request.columns_desc.empty() && request.columns_desc[0].col_unique_id >= 0) {
        tablet_schema->clear_columns();
        // TODO(lhy) handle variant
        for (const auto& column_desc : request.columns_desc) {
            tablet_schema->append_column(TabletColumn(column_desc));
        }
    }
    RowsetSharedPtr rowset_to_add;
    // writes
    res = _convert_v2(tablet, &rowset_to_add, tablet_schema, push_type);
    if (!res.ok()) {
        LOG(WARNING) << "fail to convert tmp file when realtime push. res=" << res
                     << ", failed to process realtime push."
                     << ", tablet=" << tablet->tablet_id()
                     << ", transaction_id=" << request.transaction_id;

        Status rollback_status = _engine.txn_manager()->rollback_txn(request.partition_id, *tablet,
                                                                     request.transaction_id);
        // has to check rollback status to ensure not delete a committed rowset
        if (rollback_status.ok()) {
            _engine.add_unused_rowset(rowset_to_add);
        }
        return res;
    }

    // add pending data to tablet

    if (push_type == PushType::PUSH_FOR_DELETE) {
        rowset_to_add->rowset_meta()->set_delete_predicate(std::move(del_preds.front()));
        del_preds.pop();
    }
    // Transfer ownership of `PendingRowsetGuard` to `TxnManager`
    Status commit_status = _engine.txn_manager()->commit_txn(
            request.partition_id, *tablet, request.transaction_id, load_id, rowset_to_add,
            std::move(_pending_rs_guard), false);
    if (!commit_status.ok() && !commit_status.is<PUSH_TRANSACTION_ALREADY_EXIST>()) {
        res = std::move(commit_status);
    }
    return res;
}

Status PushHandler::_convert_v2(TabletSharedPtr cur_tablet, RowsetSharedPtr* cur_rowset,
                                TabletSchemaSPtr tablet_schema, PushType push_type) {
    Status st = Status::OK();
    uint32_t num_rows = 0;
    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);

    do {
        VLOG_NOTICE << "start to convert delta file.";

        // 1. init RowsetBuilder of cur_tablet for current push
        VLOG_NOTICE << "init rowset builder. tablet=" << cur_tablet->tablet_id()
                    << ", block_row_size=" << tablet_schema->num_rows_per_row_block();
        // although the spark load output files are fully sorted,
        // but it depends on thirparty implementation, so we conservatively
        // set this value to OVERLAP_UNKNOWN
        RowsetWriterContext context;
        context.txn_id = _request.transaction_id;
        context.load_id = load_id;
        context.rowset_state = PREPARED;
        context.segments_overlap = OVERLAP_UNKNOWN;
        context.tablet_schema = tablet_schema;
        context.newest_write_timestamp = UnixSeconds();
        auto rowset_writer = DORIS_TRY(cur_tablet->create_rowset_writer(context, false));
        _pending_rs_guard = _engine.pending_local_rowsets().add(context.rowset_id);

        // 2. Init PushBrokerReader to read broker file if exist,
        //    in case of empty push this will be skipped.
        std::string path;
        // If it is push delete, the broker_scan_range is not set.
        if (push_type == PushType::PUSH_NORMAL_V2) {
            path = _request.broker_scan_range.ranges[0].path;
            LOG(INFO) << "tablet=" << cur_tablet->tablet_id() << ", file path=" << path
                      << ", file size=" << _request.broker_scan_range.ranges[0].file_size;
        }
        // For push load, this tablet maybe not need push data, so that the path maybe empty
        if (!path.empty()) {
            // init Reader
            std::unique_ptr<PushBrokerReader> reader =
                    PushBrokerReader::create_unique(_request.broker_scan_range, _request.desc_tbl);
            st = reader->init();
            if (reader == nullptr || !st.ok()) {
                st = Status::Error<PUSH_INIT_ERROR>("fail to init reader. st={}, tablet={}", st,
                                                    cur_tablet->tablet_id());
                break;
            }

            // 3. Init Block
            Block block;

            // 4. Read data from broker and write into cur_tablet
            VLOG_NOTICE << "start to convert etl file to delta.";
            while (!reader->eof()) {
                st = reader->next(&block);
                if (!st.ok()) {
                    LOG(WARNING) << "read next row failed."
                                 << " st=" << st << " read_rows=" << num_rows;
                    break;
                } else {
                    if (reader->eof()) {
                        break;
                    }
                    if (!(st = rowset_writer->add_block(&block)).ok()) {
                        LOG(WARNING) << "fail to attach block to rowset_writer. "
                                     << "st=" << st << ", tablet=" << cur_tablet->tablet_id()
                                     << ", read_rows=" << num_rows;
                        break;
                    }
                    num_rows++;
                }
            }

            reader->print_profile();
            RETURN_IF_ERROR(reader->close());
        }

        if (!st.ok()) {
            break;
        }

        if (!(st = rowset_writer->flush()).ok()) {
            LOG(WARNING) << "failed to finalize writer";
            break;
        }

        if (!(st = rowset_writer->build(*cur_rowset)).ok()) {
            LOG(WARNING) << "failed to build rowset";
            break;
        }

        _write_bytes += (*cur_rowset)->data_disk_size();
        _write_rows += (*cur_rowset)->num_rows();
    } while (false);

    VLOG_TRACE << "convert delta file end. st=" << st << ", tablet=" << cur_tablet->tablet_id()
               << ", processed_rows" << num_rows;
    return st;
}

PushBrokerReader::PushBrokerReader(const TBrokerScanRange& t_scan_range,
                                   const TDescriptorTable& t_desc_tbl)
        : _ready(false),
          _eof(false),
          _next_range(0),
          _t_desc_tbl(t_desc_tbl),
          _cur_reader_eof(true),
          _params(t_scan_range.params),
          _ranges(t_scan_range.ranges) {
    // change broker params to file params
    if (_ranges.empty()) {
        return;
    }
    _file_params.format_type = _ranges[0].format_type;
    _file_params.__set_src_tuple_id(_params.src_tuple_id);
    _file_params.__set_dest_tuple_id(_params.dest_tuple_id);
    _file_params.num_of_columns_from_file = _ranges[0].num_of_columns_from_file;
    _file_params.properties = _params.properties;
    _file_params.expr_of_dest_slot = _params.expr_of_dest_slot;
    _file_params.dest_sid_to_src_sid_without_trans = _params.dest_sid_to_src_sid_without_trans;
    _file_params.strict_mode = _params.strict_mode;
    if (_ranges[0].file_type == TFileType::FILE_HDFS) {
        _file_params.hdfs_params = parse_properties(_params.properties);
    } else {
        _file_params.__isset.broker_addresses = true;
        _file_params.broker_addresses = t_scan_range.broker_addresses;
    }

    for (const auto& range : _ranges) {
        TFileRangeDesc file_range;
        // TODO(cmy): in previous implementation, the file_type is set in _file_params
        // and it use _ranges[0].file_type.
        // Later, this field is moved to TFileRangeDesc, but here we still only use _ranges[0]'s
        // file_type.
        // Because I don't know if other range has this field, so just keep it same as before.
        file_range.__set_file_type(_ranges[0].file_type);
        file_range.__set_load_id(range.load_id);
        file_range.__set_path(range.path);
        file_range.__set_start_offset(range.start_offset);
        file_range.__set_size(range.size);
        file_range.__set_file_size(range.file_size);
        file_range.__set_columns_from_path(range.columns_from_path);

        _file_ranges.push_back(file_range);
    }
}

Status PushBrokerReader::init() {
    // init runtime state, runtime profile, counter
    TUniqueId dummy_id;
    dummy_id.hi = 0;
    dummy_id.lo = 0;
    TPlanFragmentExecParams params;
    params.fragment_instance_id = dummy_id;
    params.query_id = dummy_id;
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    std::shared_ptr<MemTrackerLimiter> tracker = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::LOAD,
            fmt::format("PushBrokerReader:dummy_id={}", print_id(dummy_id)));
    _runtime_state = RuntimeState::create_unique(params, query_options, query_globals,
                                                 ExecEnv::GetInstance(), nullptr, tracker);
    DescriptorTbl* desc_tbl = nullptr;
    Status status = DescriptorTbl::create(_runtime_state->obj_pool(), _t_desc_tbl, &desc_tbl);
    if (UNLIKELY(!status.ok())) {
        return Status::Error<PUSH_INIT_ERROR>("Failed to create descriptor table, msg: {}", status);
    }
    _runtime_state->set_desc_tbl(desc_tbl);
    _runtime_profile = _runtime_state->runtime_profile();
    _runtime_profile->set_name("PushBrokerReader");

    _file_cache_statistics = std::make_unique<io::FileCacheStatistics>();
    _file_reader_stats = std::make_unique<io::FileReaderStats>();
    _io_ctx = std::make_shared<io::IOContext>();
    _io_ctx->file_cache_stats = _file_cache_statistics.get();
    _io_ctx->file_reader_stats = _file_reader_stats.get();
    _io_ctx->query_id = &_runtime_state->query_id();

    auto* source = desc_tbl->get_tuple_descriptor(_params.src_tuple_id);
    _dest_tuple_desc = desc_tbl->get_tuple_descriptor(_params.dest_tuple_id);
    if (source == nullptr || _dest_tuple_desc == nullptr) {
        return Status::InternalError("Unknown source or destination tuple in push load");
    }
    // Older push requests omit the file-column count and read every source slot from Parquet.
    if (!_ranges.empty() && !_ranges.front().__isset.num_of_columns_from_file) {
        _file_params.num_of_columns_from_file = cast_set<int32_t>(source->slots().size());
    }
    _file_params.__set_file_type(_file_ranges.empty() ? TFileType::FILE_LOCAL
                                                      : _file_ranges.front().file_type);
    for (size_t i = 0; i < source->slots().size(); ++i) {
        TFileScanSlotInfo slot;
        slot.__set_slot_id(source->slots()[i]->id());
        slot.__set_is_file_slot(std::cmp_less(i, _file_params.num_of_columns_from_file));
        _file_params.required_slots.push_back(std::move(slot));
    }
    _file_params.__isset.required_slots = true;
    _cur_reader = std::make_unique<format::LoadTableReader>(source, _dest_tuple_desc);
    // Push ingestion must use the same source mapping and strict conversion contract as other
    // file loads, rather than keeping a second Parquet reader and conversion implementation.
    RETURN_IF_ERROR(_cur_reader->init({.projected_columns = {},
                                       .conjuncts = {},
                                       .format = format::FileFormat::PARQUET,
                                       .scan_params = &_file_params,
                                       .io_ctx = _io_ctx,
                                       .runtime_state = _runtime_state.get(),
                                       .scanner_profile = _runtime_profile}));
    _ready = true;
    return Status::OK();
}

PushBrokerReader::~PushBrokerReader() {
    WARN_IF_ERROR(close(), "Failed to close push load reader");
}

Status PushBrokerReader::next(Block* block) {
    if (!_ready || block == nullptr) {
        return Status::Error<INVALID_ARGUMENT>("PushBrokerReader not ready or block is nullptr");
    }
    if (_cur_reader_eof) {
        RETURN_IF_ERROR(_get_next_reader());
        if (_eof) {
            return Status::OK();
        }
    }
    *block = Block(_dest_tuple_desc->slots(), 0);
    return _cur_reader->get_block(block, &_cur_reader_eof);
}

Status PushBrokerReader::close() {
    _ready = false;
    if (_cur_reader != nullptr) {
        RETURN_IF_ERROR(_cur_reader->close());
        _cur_reader.reset();
    }
    return Status::OK();
}

void PushBrokerReader::print_profile() {
    std::stringstream ss;
    _runtime_profile->pretty_print(&ss);
    LOG(INFO) << ss.str();
}

Status PushBrokerReader::_get_next_reader() {
    // Closing the load reader also clears destination expressions needed by subsequent ranges.
    RETURN_IF_ERROR(_cur_reader->abort_split());
    if (_next_range >= _file_ranges.size()) {
        _eof = true;
        return Status::OK();
    }
    if (_file_params.format_type != TFileFormatType::FORMAT_PARQUET) {
        return Status::Error<PUSH_INIT_ERROR>("Unsupported file format type: {}",
                                              _file_params.format_type);
    }
    _cur_reader_eof = false;
    format::SplitReadOptions split;
    split.current_range = _file_ranges[_next_range++];
    split.current_split_format = format::FileFormat::PARQUET;
    return _cur_reader->prepare_split(split);
}

} // namespace doris
