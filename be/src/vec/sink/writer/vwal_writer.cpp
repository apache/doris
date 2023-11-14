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

#include "vwal_writer.h"

#include <brpc/http_method.h>
#include <bthread/bthread.h>
#include <fmt/format.h>
#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/data.pb.h>
#include <gen_cpp/internal_service.pb.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/common.h>
#include <sys/param.h>

#include <algorithm>
#include <exception>
#include <initializer_list>
#include <memory>
#include <mutex>
#include <ranges>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "olap/wal_manager.h"
#include "util/runtime_profile.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/runtime/vdatetime_value.h"
#include "vec/sink/vtablet_sink.h"

#ifdef DEBUG
#include <unordered_set>
#endif

#include "bvar/bvar.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/logging.h"
#include "common/object_pool.h"
#include "common/signal_handler.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "runtime/client_cache.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "util/defer_op.h"
#include "util/doris_metrics.h"
#include "util/network_util.h"
#include "util/proto_util.h"
#include "util/ref_count_closure.h"
#include "util/threadpool.h"
#include "util/thrift_rpc_helper.h"
#include "util/thrift_util.h"
#include "util/time.h"
#include "util/uid_util.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/future_block.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vexpr.h"
#include "vec/sink/vtablet_block_convertor.h"
#include "vec/sink/vtablet_finder.h"

namespace doris {
namespace vectorized {

VWalWriter::VWalWriter(int64_t tb_id, int64_t db_id, int64_t wal_id, RuntimeState* state,
                       TupleDescriptor* output_tuple_desc)
        : _tb_id(tb_id),
          _db_id(db_id),
          _wal_id(wal_id),
          _state(state),
          _output_tuple_desc(output_tuple_desc) {}

VWalWriter::~VWalWriter() {}

Status VWalWriter::init() {
    RETURN_IF_ERROR(_state->exec_env()->wal_mgr()->add_wal_path(_db_id, _tb_id, _wal_id,
                                                                _state->import_label()));
    RETURN_IF_ERROR(_state->exec_env()->wal_mgr()->create_wal_writer(_wal_id, _wal_writer));
    _state->exec_env()->wal_mgr()->add_wal_status_queue(_tb_id, _wal_id,
                                                        WalManager::WAL_STATUS::CREATE);
    std::stringstream ss;
    for (auto slot_desc : _output_tuple_desc->slots()) {
        ss << std::to_string(slot_desc->col_unique_id()) << ",";
    }
    std::string col_ids = ss.str().substr(0, ss.str().size() - 1);
    RETURN_IF_ERROR(_wal_writer->append_header(_version, col_ids));
    return Status::OK();
}
Status VWalWriter::write_wal(OlapTableBlockConvertor* block_convertor,
                             OlapTabletFinder* tablet_finder, vectorized::Block* block,
                             RuntimeState* state, int64_t num_rows, int64_t filtered_rows) {
    PBlock pblock;
    size_t uncompressed_bytes = 0, compressed_bytes = 0;
    if (filtered_rows == 0) {
        RETURN_IF_ERROR(block->serialize(state->be_exec_version(), &pblock, &uncompressed_bytes,
                                         &compressed_bytes, segment_v2::CompressionTypePB::SNAPPY));
        RETURN_IF_ERROR(_wal_writer->append_blocks(std::vector<PBlock*> {&pblock}));
    } else {
        auto cloneBlock = block->clone_without_columns();
        auto res_block = vectorized::MutableBlock::build_mutable_block(&cloneBlock);
        for (int i = 0; i < num_rows; ++i) {
            if (block_convertor->num_filtered_rows() > 0 && block_convertor->filter_map()[i]) {
                continue;
            }
            if (tablet_finder->num_filtered_rows() > 0 && tablet_finder->filter_bitmap().Get(i)) {
                continue;
            }
            res_block.add_row(block, i);
        }
        RETURN_IF_ERROR(res_block.to_block().serialize(state->be_exec_version(), &pblock,
                                                       &uncompressed_bytes, &compressed_bytes,
                                                       segment_v2::CompressionTypePB::SNAPPY));
        RETURN_IF_ERROR(_wal_writer->append_blocks(std::vector<PBlock*> {&pblock}));
    }
    return Status::OK();
}
Status VWalWriter::group_commit_block(vectorized::Block* input_block, int64_t num_rows,
                                      int64_t filter_rows, RuntimeState* state,
                                      vectorized::Block* block,
                                      OlapTableBlockConvertor* block_convertor,
                                      OlapTabletFinder* tablet_finder) {
    RETURN_IF_ERROR(write_wal(block_convertor, tablet_finder, block, state, num_rows, filter_rows));
#ifndef BE_TEST
    auto* future_block = assert_cast<FutureBlock*>(input_block);
    std::unique_lock<doris::Mutex> l(*(future_block->lock));
    future_block->set_result(Status::OK(), num_rows, num_rows - filter_rows);
    future_block->cv->notify_all();
#endif
    return Status::OK();
}
Status VWalWriter::close() {
    if (_wal_writer != nullptr) {
        RETURN_IF_ERROR(_wal_writer->finalize());
    }
    return Status::OK();
}
} // namespace vectorized
} // namespace doris