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

#include "olap/push_handler.h"

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

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "olap/delete_handler.h"
#include "olap/olap_define.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/schema.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_schema.h"
#include "olap/txn_manager.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "util/runtime_profile.h"
#include "util/time.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/format/parquet/vparquet_reader.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/functions/simple_function_factory.h"

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
    LOG(INFO) << "begin to realtime push. tablet=" << tablet->full_name()
              << ", transaction_id=" << request.transaction_id;

    Status res = Status::OK();
    _request = request;

    static_cast<void>(DescriptorTbl::create(&_pool, _request.desc_tbl, &_desc_tbl));

    res = _do_streaming_ingestion(tablet, request, push_type, tablet_info_vec);

    if (res.ok()) {
        if (tablet_info_vec != nullptr) {
            TTabletInfo tablet_info;
            tablet_info.tablet_id = tablet->tablet_id();
            tablet_info.schema_hash = tablet->schema_hash();
            static_cast<void>(
                    StorageEngine::instance()->tablet_manager()->report_tablet_info(&tablet_info));
            tablet_info_vec->push_back(tablet_info);
        }
        LOG(INFO) << "process realtime push successfully. "
                  << "tablet=" << tablet->full_name() << ", partition_id=" << request.partition_id
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

    std::shared_lock base_migration_rlock(tablet->get_migration_lock(), std::try_to_lock);
    if (!base_migration_rlock.owns_lock()) {
        return Status::Error<TRY_LOCK_FAILED>(
                "PushHandler::_do_streaming_ingestion get lock failed");
    }
    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    {
        std::lock_guard<std::mutex> push_lock(tablet->get_push_lock());
        RETURN_IF_ERROR(StorageEngine::instance()->txn_manager()->prepare_txn(
                request.partition_id, tablet, request.transaction_id, load_id));
    }

    // not call validate request here, because realtime load does not
    // contain version info

    Status res;
    // check delete condition if push for delete
    std::queue<DeletePredicatePB> del_preds;
    if (push_type == PushType::PUSH_FOR_DELETE) {
        DeletePredicatePB del_pred;
        TabletSchema tablet_schema;
        tablet_schema.copy_from(*tablet->tablet_schema());
        for (const auto& delete_cond : request.delete_conditions) {
            if (!delete_cond.__isset.column_unique_id) {
                LOG(WARNING) << "column=" << delete_cond.column_name
                             << " in predicate does not have uid, table id="
                             << tablet_schema.table_id();
                // TODO(tsy): make it fail here after FE forbidding hard-link-schema-change
                continue;
            }
            if (tablet_schema.field_index(delete_cond.column_unique_id) == -1) {
                const auto& err_msg =
                        fmt::format("column id={} does not exists, table id={}",
                                    delete_cond.column_unique_id, tablet_schema.table_id());
                LOG(WARNING) << err_msg;
                DCHECK(false);
                return Status::Aborted(err_msg);
            }
            if (tablet_schema.column_by_uid(delete_cond.column_unique_id).name() !=
                delete_cond.column_name) {
                const auto& err_msg = fmt::format(
                        "colum name={} does not belongs to column uid={}, which column name={}",
                        delete_cond.column_name, delete_cond.column_unique_id,
                        tablet_schema.column_by_uid(delete_cond.column_unique_id).name());
                LOG(WARNING) << err_msg;
                DCHECK(false);
                return Status::Aborted(err_msg);
            }
        }
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
                         << ", tablet=" << tablet->full_name();
            return res;
        }
    }

    // check if version number exceed limit
    if (tablet->exceed_version_limit(config::max_tablet_version_num)) {
        return Status::Status::Error<TOO_MANY_VERSION>(
                "failed to push data. version count: {}, exceed limit: {}, tablet: {}",
                tablet->version_count(), config::max_tablet_version_num, tablet->full_name());
    }
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->copy_from(*tablet->tablet_schema());
    if (!request.columns_desc.empty() && request.columns_desc[0].col_unique_id >= 0) {
        tablet_schema->clear_columns();
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
                     << ", tablet=" << tablet->full_name()
                     << ", transaction_id=" << request.transaction_id;

        Status rollback_status = StorageEngine::instance()->txn_manager()->rollback_txn(
                request.partition_id, tablet, request.transaction_id);
        // has to check rollback status to ensure not delete a committed rowset
        if (rollback_status.ok()) {
            StorageEngine::instance()->add_unused_rowset(rowset_to_add);
        }
        return res;
    }

    // add pending data to tablet

    if (push_type == PushType::PUSH_FOR_DELETE) {
        rowset_to_add->rowset_meta()->set_delete_predicate(std::move(del_preds.front()));
        del_preds.pop();
    }
    Status commit_status = StorageEngine::instance()->txn_manager()->commit_txn(
            request.partition_id, tablet, request.transaction_id, load_id, rowset_to_add, false,
            nullptr);
    if (!commit_status.ok() && !commit_status.is<PUSH_TRANSACTION_ALREADY_EXIST>()) {
        res = std::move(commit_status);
    }
    return res;
}

Status PushHandler::_convert_v2(TabletSharedPtr cur_tablet, RowsetSharedPtr* cur_rowset,
                                TabletSchemaSPtr tablet_schema, PushType push_type) {
    Status res = Status::OK();
    uint32_t num_rows = 0;
    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);

    do {
        VLOG_NOTICE << "start to convert delta file.";

        // 1. init RowsetBuilder of cur_tablet for current push
        VLOG_NOTICE << "init rowset builder. tablet=" << cur_tablet->full_name()
                    << ", block_row_size=" << tablet_schema->num_rows_per_row_block();
        // although the spark load output files are fully sorted,
        // but it depends on thirparty implementation, so we conservatively
        // set this value to OVERLAP_UNKNOWN
        std::unique_ptr<RowsetWriter> rowset_writer;
        RowsetWriterContext context;
        context.txn_id = _request.transaction_id;
        context.load_id = load_id;
        context.rowset_state = PREPARED;
        context.segments_overlap = OVERLAP_UNKNOWN;
        context.tablet_schema = tablet_schema;
        context.newest_write_timestamp = UnixSeconds();
        res = cur_tablet->create_rowset_writer(context, &rowset_writer);
        if (!res.ok()) {
            LOG(WARNING) << "failed to init rowset writer, tablet=" << cur_tablet->full_name()
                         << ", txn_id=" << _request.transaction_id << ", res=" << res;
            break;
        }

        // 2. Init PushBrokerReader to read broker file if exist,
        //    in case of empty push this will be skipped.
        std::string path;
        // If it is push delete, the broker_scan_range is not set.
        if (push_type == PushType::PUSH_NORMAL_V2) {
            path = _request.broker_scan_range.ranges[0].path;
            LOG(INFO) << "tablet=" << cur_tablet->full_name() << ", file path=" << path
                      << ", file size=" << _request.broker_scan_range.ranges[0].file_size;
        }
        // For push load, this tablet maybe not need push data, so that the path maybe empty
        if (!path.empty()) {
            // init schema
            std::unique_ptr<Schema> schema(new (std::nothrow) Schema(tablet_schema));
            if (schema == nullptr) {
                res = Status::Error<MEM_ALLOC_FAILED>("fail to create schema. tablet={}",
                                                      cur_tablet->full_name());
                break;
            }

            // init Reader
            std::unique_ptr<PushBrokerReader> reader = PushBrokerReader::create_unique(
                    schema.get(), _request.broker_scan_range, _request.desc_tbl);
            res = reader->init();
            if (reader == nullptr || !res.ok()) {
                res = Status::Error<PUSH_INIT_ERROR>("fail to init reader. res={}, tablet={}", res,
                                                     cur_tablet->full_name());
                break;
            }

            // 3. Init Block
            vectorized::Block block;

            // 4. Read data from broker and write into cur_tablet
            VLOG_NOTICE << "start to convert etl file to delta.";
            while (!reader->eof()) {
                res = reader->next(&block);
                if (!res.ok()) {
                    LOG(WARNING) << "read next row failed."
                                 << " res=" << res << " read_rows=" << num_rows;
                    break;
                } else {
                    if (reader->eof()) {
                        break;
                    }
                    if (!(res = rowset_writer->add_block(&block))) {
                        LOG(WARNING) << "fail to attach block to rowset_writer. "
                                     << "res=" << res << ", tablet=" << cur_tablet->full_name()
                                     << ", read_rows=" << num_rows;
                        break;
                    }
                    num_rows++;
                }
            }

            reader->print_profile();
            static_cast<void>(reader->close());
        }

        if (!rowset_writer->flush().ok()) {
            LOG(WARNING) << "failed to finalize writer";
            break;
        }
        *cur_rowset = rowset_writer->build();
        if (*cur_rowset == nullptr) {
            res = Status::Error<MEM_ALLOC_FAILED>("fail to build rowset");
            break;
        }

        _write_bytes += (*cur_rowset)->data_disk_size();
        _write_rows += (*cur_rowset)->num_rows();
    } while (false);

    VLOG_TRACE << "convert delta file end. res=" << res << ", tablet=" << cur_tablet->full_name()
               << ", processed_rows" << num_rows;
    return res;
}

PushBrokerReader::PushBrokerReader(const Schema* schema, const TBrokerScanRange& t_scan_range,
                                   const TDescriptorTable& t_desc_tbl)
        : _ready(false),
          _eof(false),
          _next_range(0),
          _t_desc_tbl(t_desc_tbl),
          _cur_reader_eof(false),
          _params(t_scan_range.params),
          _ranges(t_scan_range.ranges) {
    // change broker params to file params
    if (0 == _ranges.size()) {
        return;
    }
    _file_params.format_type = _ranges[0].format_type;
    _file_params.src_tuple_id = _params.src_tuple_id;
    _file_params.dest_tuple_id = _params.dest_tuple_id;
    _file_params.num_of_columns_from_file = _ranges[0].num_of_columns_from_file;
    _file_params.properties = _params.properties;
    _file_params.expr_of_dest_slot = _params.expr_of_dest_slot;
    _file_params.dest_sid_to_src_sid_without_trans = _params.dest_sid_to_src_sid_without_trans;
    _file_params.strict_mode = _params.strict_mode;
    _file_params.__isset.broker_addresses = true;
    _file_params.broker_addresses = t_scan_range.broker_addresses;

    for (int i = 0; i < _ranges.size(); ++i) {
        TFileRangeDesc file_range;
        // TODO(cmy): in previous implementation, the file_type is set in _file_params
        // and it use _ranges[0].file_type.
        // Later, this field is moved to TFileRangeDesc, but here we still only use _ranges[0]'s
        // file_type.
        // Because I don't know if other range has this field, so just keep it same as before.
        file_range.__set_file_type(_ranges[0].file_type);
        file_range.__set_load_id(_ranges[i].load_id);
        file_range.__set_path(_ranges[i].path);
        file_range.__set_start_offset(_ranges[i].start_offset);
        file_range.__set_size(_ranges[i].size);
        file_range.__set_file_size(_ranges[i].file_size);
        file_range.__set_columns_from_path(_ranges[i].columns_from_path);

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
    TExecPlanFragmentParams fragment_params;
    fragment_params.params = params;
    fragment_params.protocol_version = PaloInternalServiceVersion::V1;
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    _runtime_state = RuntimeState::create_unique(params, query_options, query_globals,
                                                 ExecEnv::GetInstance());
    DescriptorTbl* desc_tbl = nullptr;
    Status status = DescriptorTbl::create(_runtime_state->obj_pool(), _t_desc_tbl, &desc_tbl);
    if (UNLIKELY(!status.ok())) {
        return Status::Error<PUSH_INIT_ERROR>("Failed to create descriptor table, msg: {}", status);
    }
    _runtime_state->set_desc_tbl(desc_tbl);
    _runtime_state->init_mem_trackers(dummy_id, "PushBrokerReader");
    _runtime_profile = _runtime_state->runtime_profile();
    _runtime_profile->set_name("PushBrokerReader");

    _file_cache_statistics.reset(new io::FileCacheStatistics());
    _io_ctx.reset(new io::IOContext());
    _io_ctx->file_cache_stats = _file_cache_statistics.get();
    _io_ctx->query_id = &_runtime_state->query_id();

    auto slot_descs = desc_tbl->get_tuple_descriptor(0)->slots();
    for (int i = 0; i < slot_descs.size(); i++) {
        _all_col_names.push_back(to_lower((slot_descs[i]->col_name())));
    }

    RETURN_IF_ERROR(_init_expr_ctxes());

    _ready = true;
    return Status::OK();
}

Status PushBrokerReader::next(vectorized::Block* block) {
    if (!_ready || block == nullptr) {
        return Status::Error<INVALID_ARGUMENT>("PushBrokerReader not ready or block is nullptr");
    }
    if (_cur_reader == nullptr || _cur_reader_eof) {
        RETURN_IF_ERROR(_get_next_reader());
        if (_eof) {
            return Status::OK();
        }
    }
    RETURN_IF_ERROR(_init_src_block());
    size_t read_rows = 0;
    RETURN_IF_ERROR(_cur_reader->get_next_block(_src_block_ptr, &read_rows, &_cur_reader_eof));
    if (read_rows > 0) {
        RETURN_IF_ERROR(_cast_to_input_block());
        RETURN_IF_ERROR(_convert_to_output_block(block));
    }
    return Status::OK();
}

Status PushBrokerReader::close() {
    _ready = false;
    return Status::OK();
}

Status PushBrokerReader::_init_src_block() {
    _src_block.clear();
    int idx = 0;
    for (auto& slot : _src_slot_descs) {
        vectorized::DataTypePtr data_type;
        auto it = _name_to_col_type.find(slot->col_name());
        if (it == _name_to_col_type.end()) {
            // not exist in file, using type from _input_tuple_desc
            data_type = vectorized::DataTypeFactory::instance().create_data_type(
                    slot->type(), slot->is_nullable());
        } else {
            data_type = vectorized::DataTypeFactory::instance().create_data_type(it->second, true);
        }
        if (data_type == nullptr) {
            return Status::NotSupported("Not support data type {} for column {}",
                                        it == _name_to_col_type.end() ? slot->type().debug_string()
                                                                      : it->second.debug_string(),
                                        slot->col_name());
        }
        vectorized::MutableColumnPtr data_column = data_type->create_column();
        _src_block.insert(vectorized::ColumnWithTypeAndName(std::move(data_column), data_type,
                                                            slot->col_name()));
        _src_block_name_to_idx.emplace(slot->col_name(), idx++);
    }
    _src_block_ptr = &_src_block;
    return Status::OK();
}

Status PushBrokerReader::_cast_to_input_block() {
    size_t idx = 0;
    for (auto& slot_desc : _src_slot_descs) {
        if (_name_to_col_type.find(slot_desc->col_name()) == _name_to_col_type.end()) {
            continue;
        }
        if (slot_desc->type().is_variant_type()) {
            continue;
        }
        auto& arg = _src_block_ptr->get_by_name(slot_desc->col_name());
        // remove nullable here, let the get_function decide whether nullable
        auto return_type = slot_desc->get_data_type_ptr();
        vectorized::ColumnsWithTypeAndName arguments {
                arg,
                {vectorized::DataTypeString().create_column_const(
                         arg.column->size(), remove_nullable(return_type)->get_family_name()),
                 std::make_shared<vectorized::DataTypeString>(), ""}};
        auto func_cast = vectorized::SimpleFunctionFactory::instance().get_function(
                "CAST", arguments, return_type);
        idx = _src_block_name_to_idx[slot_desc->col_name()];
        RETURN_IF_ERROR(
                func_cast->execute(nullptr, *_src_block_ptr, {idx}, idx, arg.column->size()));
        _src_block_ptr->get_by_position(idx).type = std::move(return_type);
    }
    return Status::OK();
}

Status PushBrokerReader::_convert_to_output_block(vectorized::Block* block) {
    block->clear();

    int ctx_idx = 0;
    size_t rows = _src_block.rows();
    auto filter_column = vectorized::ColumnUInt8::create(rows, 1);

    for (auto slot_desc : _dest_tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            continue;
        }
        int dest_index = ctx_idx++;
        vectorized::ColumnPtr column_ptr;

        auto& ctx = _dest_expr_ctxs[dest_index];
        int result_column_id = -1;
        // PT1 => dest primitive type
        RETURN_IF_ERROR(ctx->execute(&_src_block, &result_column_id));
        column_ptr = _src_block.get_by_position(result_column_id).column;
        // column_ptr maybe a ColumnConst, convert it to a normal column
        column_ptr = column_ptr->convert_to_full_column_if_const();
        DCHECK(column_ptr != nullptr);

        // because of src_slot_desc is always be nullable, so the column_ptr after do dest_expr
        // is likely to be nullable
        if (LIKELY(column_ptr->is_nullable())) {
            if (!slot_desc->is_nullable()) {
                column_ptr = remove_nullable(column_ptr);
            }
        } else if (slot_desc->is_nullable()) {
            column_ptr = make_nullable(column_ptr);
        }
        block->insert(dest_index,
                      vectorized::ColumnWithTypeAndName(column_ptr, slot_desc->get_data_type_ptr(),
                                                        slot_desc->col_name()));
    }
    _src_block.clear();

    size_t dest_size = block->columns();
    block->insert(vectorized::ColumnWithTypeAndName(std::move(filter_column),
                                                    std::make_shared<vectorized::DataTypeUInt8>(),
                                                    "filter column"));
    RETURN_IF_ERROR(vectorized::Block::filter_block(block, dest_size, dest_size));
    return Status::OK();
}

void PushBrokerReader::print_profile() {
    std::stringstream ss;
    _runtime_profile->pretty_print(&ss);
    LOG(INFO) << ss.str();
}

Status PushBrokerReader::_init_expr_ctxes() {
    // Construct _src_slot_descs
    const TupleDescriptor* src_tuple_desc =
            _runtime_state->desc_tbl().get_tuple_descriptor(_params.src_tuple_id);
    if (src_tuple_desc == nullptr) {
        return Status::InternalError("Unknown source tuple descriptor, tuple_id={}",
                                     _params.src_tuple_id);
    }

    std::map<SlotId, SlotDescriptor*> src_slot_desc_map;
    std::unordered_map<SlotDescriptor*, int> src_slot_desc_to_index {};
    for (int i = 0, len = src_tuple_desc->slots().size(); i < len; ++i) {
        auto* slot_desc = src_tuple_desc->slots()[i];
        src_slot_desc_to_index.emplace(slot_desc, i);
        src_slot_desc_map.emplace(slot_desc->id(), slot_desc);
    }
    for (auto slot_id : _params.src_slot_ids) {
        auto it = src_slot_desc_map.find(slot_id);
        if (it == std::end(src_slot_desc_map)) {
            return Status::InternalError("Unknown source slot descriptor, slot_id={}", slot_id);
        }
        _src_slot_descs.emplace_back(it->second);
    }
    _row_desc.reset(new RowDescriptor(_runtime_state->desc_tbl(),
                                      std::vector<TupleId>({_params.src_tuple_id}),
                                      std::vector<bool>({false})));

    if (!_pre_filter_texprs.empty()) {
        DCHECK(_pre_filter_texprs.size() == 1);
        RETURN_IF_ERROR(
                vectorized::VExpr::create_expr_tree(_pre_filter_texprs[0], _pre_filter_ctx_ptr));
        RETURN_IF_ERROR(_pre_filter_ctx_ptr->prepare(_runtime_state.get(), *_row_desc));
        RETURN_IF_ERROR(_pre_filter_ctx_ptr->open(_runtime_state.get()));
    }

    _dest_tuple_desc = _runtime_state->desc_tbl().get_tuple_descriptor(_params.dest_tuple_id);
    if (_dest_tuple_desc == nullptr) {
        return Status::InternalError("Unknown dest tuple descriptor, tuple_id={}",
                                     _params.dest_tuple_id);
    }
    bool has_slot_id_map = _params.__isset.dest_sid_to_src_sid_without_trans;
    for (auto slot_desc : _dest_tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            continue;
        }
        auto it = _params.expr_of_dest_slot.find(slot_desc->id());
        if (it == std::end(_params.expr_of_dest_slot)) {
            return Status::InternalError("No expr for dest slot, id={}, name={}", slot_desc->id(),
                                         slot_desc->col_name());
        }

        vectorized::VExprContextSPtr ctx;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(it->second, ctx));
        RETURN_IF_ERROR(ctx->prepare(_runtime_state.get(), *_row_desc.get()));
        RETURN_IF_ERROR(ctx->open(_runtime_state.get()));
        _dest_expr_ctxs.emplace_back(ctx);
        if (has_slot_id_map) {
            auto it1 = _params.dest_sid_to_src_sid_without_trans.find(slot_desc->id());
            if (it1 == std::end(_params.dest_sid_to_src_sid_without_trans)) {
                _src_slot_descs_order_by_dest.emplace_back(nullptr);
            } else {
                auto _src_slot_it = src_slot_desc_map.find(it1->second);
                if (_src_slot_it == std::end(src_slot_desc_map)) {
                    return Status::InternalError("No src slot {} in src slot descs", it1->second);
                }
                _dest_slot_to_src_slot_index.emplace(_src_slot_descs_order_by_dest.size(),
                                                     src_slot_desc_to_index[_src_slot_it->second]);
                _src_slot_descs_order_by_dest.emplace_back(_src_slot_it->second);
            }
        }
    }
    return Status::OK();
}

Status PushBrokerReader::_get_next_reader() {
    _cur_reader.reset(nullptr);
    if (_next_range >= _file_ranges.size()) {
        _eof = true;
        return Status::OK();
    }
    const TFileRangeDesc& range = _file_ranges[_next_range++];
    Status init_status;
    switch (_file_params.format_type) {
    case TFileFormatType::FORMAT_PARQUET: {
        std::unique_ptr<vectorized::ParquetReader> parquet_reader =
                vectorized::ParquetReader::create_unique(
                        _runtime_profile, _file_params, range,
                        _runtime_state->query_options().batch_size,
                        const_cast<cctz::time_zone*>(&_runtime_state->timezone_obj()),
                        _io_ctx.get(), _runtime_state.get());

        RETURN_IF_ERROR(parquet_reader->open());
        std::vector<std::string> place_holder;
        init_status = parquet_reader->init_reader(
                _all_col_names, place_holder, _colname_to_value_range, _push_down_exprs,
                _real_tuple_desc, _default_val_row_desc.get(), _col_name_to_slot_id,
                &_not_single_slot_filter_conjuncts, &_slot_id_to_filter_conjuncts, false);
        _cur_reader = std::move(parquet_reader);
        if (!init_status.ok()) {
            return Status::InternalError("failed to init reader for file {}, err: {}", range.path,
                                         init_status.to_string());
        }
        std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
                partition_columns;
        std::unordered_map<std::string, vectorized::VExprContextSPtr> missing_columns;
        static_cast<void>(_cur_reader->get_columns(&_name_to_col_type, &_missing_cols));
        static_cast<void>(_cur_reader->set_fill_columns(partition_columns, missing_columns));
        break;
    }
    default:
        return Status::Error<PUSH_INIT_ERROR>("Unsupported file format type: {}",
                                              _file_params.format_type);
    }
    _cur_reader_eof = false;

    return Status::OK();
}

std::string PushHandler::_debug_version_list(const Versions& versions) const {
    std::ostringstream txt;
    txt << "Versions: ";

    for (Versions::const_iterator it = versions.begin(); it != versions.end(); ++it) {
        txt << "[" << it->first << "~" << it->second << "],";
    }

    return txt.str();
}

} // namespace doris
