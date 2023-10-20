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

#include "vec/sink/vtablet_sink.h"

#include <brpc/http_header.h>
#include <brpc/http_method.h>
#include <brpc/uri.h>
#include <bthread/bthread.h>
#include <butil/iobuf_inl.h>
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
#include <opentelemetry/nostd/shared_ptr.h>
#include <sys/param.h>
#include <sys/types.h>

#include <algorithm>
#include <boost/algorithm/string/case_conv.hpp>
#include <cctype>
#include <exception>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <mutex>
#include <numeric>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "util/runtime_profile.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_string.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/runtime/vdatetime_value.h"

#ifdef DEBUG
#include <unordered_set>
#endif

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/logging.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "runtime/client_cache.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "service/brpc.h"
#include "util/binary_cast.hpp"
#include "util/brpc_client_cache.h"
#include "util/debug/sanitizer_scopes.h"
#include "util/defer_op.h"
#include "util/doris_metrics.h"
#include "util/network_util.h"
#include "util/proto_util.h"
#include "util/ref_count_closure.h"
#include "util/telemetry/telemetry.h"
#include "util/thread.h"
#include "util/threadpool.h"
#include "util/thrift_rpc_helper.h"
#include "util/thrift_util.h"
#include "util/time.h"
#include "util/uid_util.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/future_block.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/sink/vtablet_block_convertor.h"
#include "vec/sink/vtablet_finder.h"

namespace doris {
class TExpr;

namespace vectorized {

VOlapTableSink::VOlapTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                               const std::vector<TExpr>& texprs, bool group_commit)
        : AsyncWriterSink<VTabletWriter, VOLAP_TABLE_SINK>(row_desc, texprs),
          _pool(pool),
          _group_commit(group_commit) {}

Status VOlapTableSink::init(const TDataSink& t_sink) {
    RETURN_IF_ERROR(AsyncWriterSink::init(t_sink));
    RETURN_IF_ERROR(_writer->init_properties(_pool, _group_commit));
    return Status::OK();
}

Status VOlapTableSink::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return _close_status;
    }
    _close_status = AsyncWriterSink::close(state, exec_status);
    return _close_status;
}

} // namespace vectorized
} // namespace doris
