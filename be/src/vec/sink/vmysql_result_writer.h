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

#pragma once
#include <gen_cpp/PaloInternalService_types.h>
#include <stddef.h>

#include <memory>
#include <vector>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/result_block_buffer.h"
#include "runtime/result_writer.h"
#include "util/mysql_row_buffer.h"
#include "util/runtime_profile.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

namespace vectorized {
class Block;

class GetResultBatchCtx {
public:
    using ResultType = TFetchDataResult;
    ENABLE_FACTORY_CREATOR(GetResultBatchCtx)
    GetResultBatchCtx(PFetchDataResult* result, google::protobuf::Closure* done)
            : _result(result), _done(done) {}
#ifdef BE_TEST
    GetResultBatchCtx() = default;
#endif
    MOCK_FUNCTION ~GetResultBatchCtx() = default;
    MOCK_FUNCTION void on_failure(const Status& status);
    MOCK_FUNCTION void on_close(int64_t packet_seq, int64_t returned_rows = 0);
    MOCK_FUNCTION Status on_data(const std::shared_ptr<TFetchDataResult>& t_result,
                                 int64_t packet_seq, ResultBlockBufferBase* buffer);

private:
#ifndef BE_TEST
    const int32_t _max_msg_size = std::numeric_limits<int32_t>::max();
#else
    int32_t _max_msg_size = std::numeric_limits<int32_t>::max();
#endif

    PFetchDataResult* _result = nullptr;
    google::protobuf::Closure* _done = nullptr;
};

using MySQLResultBlockBuffer = ResultBlockBuffer<GetResultBatchCtx>;

template <bool is_binary_format = false>
class VMysqlResultWriter final : public ResultWriter {
public:
    VMysqlResultWriter(std::shared_ptr<ResultBlockBufferBase> sinker,
                       const VExprContextSPtrs& output_vexpr_ctxs, RuntimeProfile* parent_profile);

    Status init(RuntimeState* state) override;

    Status write(RuntimeState* state, Block& block) override;

    Status close(Status status) override;

private:
    void _init_profile();
    Status _set_options(const TSerdeDialect::type& serde_dialect);
    Status _write_one_block(RuntimeState* state, Block& block);

    std::shared_ptr<MySQLResultBlockBuffer> _sinker = nullptr;

    const VExprContextSPtrs& _output_vexpr_ctxs;

    RuntimeProfile* _parent_profile; // parent profile from result sink. not owned
    // total time cost on append batch operation
    RuntimeProfile::Counter* _append_row_batch_timer = nullptr;
    // tuple convert timer, child timer of _append_row_batch_timer
    RuntimeProfile::Counter* _convert_tuple_timer = nullptr;
    // file write timer, child timer of _append_row_batch_timer
    RuntimeProfile::Counter* _result_send_timer = nullptr;
    // number of sent rows
    RuntimeProfile::Counter* _sent_rows_counter = nullptr;
    // size of sent data
    RuntimeProfile::Counter* _bytes_sent_counter = nullptr;
    // If true, no block will be sent
    bool _is_dry_run = false;

    uint64_t _bytes_sent = 0;

    DataTypeSerDe::FormatOptions _options;
};
} // namespace vectorized
} // namespace doris

#include "common/compile_check_end.h"
