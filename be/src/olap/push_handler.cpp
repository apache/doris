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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/MasterService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/types.pb.h>

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
#include "vec/exec/vparquet_scanner.h"

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

    DescriptorTbl::create(&_pool, _request.desc_tbl, &_desc_tbl);

    res = _do_streaming_ingestion(tablet, request, push_type, tablet_info_vec);

    if (res.ok()) {
        if (tablet_info_vec != nullptr) {
            TTabletInfo tablet_info;
            tablet_info.tablet_id = tablet->tablet_id();
            tablet_info.schema_hash = tablet->schema_hash();
            StorageEngine::instance()->tablet_manager()->report_tablet_info(&tablet_info);
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
        return Status::Error<TABLE_NOT_FOUND>();
    }

    std::shared_lock base_migration_rlock(tablet->get_migration_lock(), std::try_to_lock);
    if (!base_migration_rlock.owns_lock()) {
        return Status::Error<TRY_LOCK_FAILED>();
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
        LOG(WARNING) << "failed to push data. version count: " << tablet->version_count()
                     << ", exceed limit: " << config::max_tablet_version_num
                     << ". tablet: " << tablet->full_name();
        return Status::Status::Error<TOO_MANY_VERSION>();
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
        rowset_to_add->rowset_meta()->set_delete_predicate(del_preds.front());
        del_preds.pop();
    }
    Status commit_status = StorageEngine::instance()->txn_manager()->commit_txn(
            request.partition_id, tablet, request.transaction_id, load_id, rowset_to_add, false);
    if (commit_status != Status::OK() && !commit_status.is<PUSH_TRANSACTION_ALREADY_EXIST>()) {
        res = commit_status;
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
            std::unique_ptr<PushBrokerReader> reader(new (std::nothrow) PushBrokerReader());
            if (reader == nullptr) {
                LOG(WARNING) << "fail to create reader. tablet=" << cur_tablet->full_name();
                res = Status::Error<MEM_ALLOC_FAILED>();
                break;
            }

            // init schema
            std::unique_ptr<Schema> schema(new (std::nothrow) Schema(tablet_schema));
            if (schema == nullptr) {
                LOG(WARNING) << "fail to create schema. tablet=" << cur_tablet->full_name();
                res = Status::Error<MEM_ALLOC_FAILED>();
                break;
            }

            // init Reader
            if (!(res = reader->init(schema.get(), _request.broker_scan_range,
                                     _request.desc_tbl))) {
                LOG(WARNING) << "fail to init reader. res=" << res
                             << ", tablet=" << cur_tablet->full_name();
                res = Status::Error<PUSH_INIT_ERROR>();
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
            reader->close();
        }

        if (rowset_writer->flush() != Status::OK()) {
            LOG(WARNING) << "failed to finalize writer";
            break;
        }
        *cur_rowset = rowset_writer->build();
        if (*cur_rowset == nullptr) {
            LOG(WARNING) << "fail to build rowset";
            res = Status::Error<MEM_ALLOC_FAILED>();
            break;
        }

        _write_bytes += (*cur_rowset)->data_disk_size();
        _write_rows += (*cur_rowset)->num_rows();
    } while (false);

    VLOG_TRACE << "convert delta file end. res=" << res << ", tablet=" << cur_tablet->full_name()
               << ", processed_rows" << num_rows;
    return res;
}

Status PushBrokerReader::init(const Schema* schema, const TBrokerScanRange& t_scan_range,
                              const TDescriptorTable& t_desc_tbl) {
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
    Status status = DescriptorTbl::create(_runtime_state->obj_pool(), t_desc_tbl, &desc_tbl);
    if (UNLIKELY(!status.ok())) {
        LOG(WARNING) << "Failed to create descriptor table, msg: " << status;
        return Status::Error<PUSH_INIT_ERROR>();
    }
    _runtime_state->set_desc_tbl(desc_tbl);
    status = _runtime_state->init_mem_trackers(dummy_id);
    if (UNLIKELY(!status.ok())) {
        LOG(WARNING) << "Failed to init mem trackers, msg: " << status;
        return Status::Error<PUSH_INIT_ERROR>();
    }
    _runtime_profile = _runtime_state->runtime_profile();
    _runtime_profile->set_name("PushBrokerReader");

    _counter.reset(new ScannerCounter());

    // init scanner
    BaseScanner* scanner = nullptr;
    switch (t_scan_range.ranges[0].format_type) {
    case TFileFormatType::FORMAT_PARQUET:
        scanner = new vectorized::VParquetScanner(
                _runtime_state.get(), _runtime_profile, t_scan_range.params, t_scan_range.ranges,
                t_scan_range.broker_addresses, _pre_filter_texprs, _counter.get());
        break;
    default:
        LOG(WARNING) << "Unsupported file format type: " << t_scan_range.ranges[0].format_type;
        return Status::Error<PUSH_INIT_ERROR>();
    }
    _scanner.reset(scanner);
    status = _scanner->open();
    if (UNLIKELY(!status.ok())) {
        LOG(WARNING) << "Failed to open scanner, msg: " << status;
        return Status::Error<PUSH_INIT_ERROR>();
    }

    _ready = true;
    return Status::OK();
}

Status PushBrokerReader::next(vectorized::Block* block) {
    if (!_ready || block == nullptr) {
        return Status::Error<INVALID_ARGUMENT>();
    }
    _scanner->get_next(block, &_eof);
    return Status::OK();
}

void PushBrokerReader::print_profile() {
    std::stringstream ss;
    _runtime_profile->pretty_print(&ss);
    LOG(INFO) << ss.str();
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
