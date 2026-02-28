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

#include <gen_cpp/DataSinks_types.h>

#include <map>
#include <string>

#include "common/status.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {

class ObjectPool;
class RuntimeState;
class RuntimeProfile;

namespace vectorized {

class Block;
class VJniFormatTransformer;

class VMCPartitionWriter {
public:
    VMCPartitionWriter(RuntimeState* state, const VExprContextSPtrs& output_vexpr_ctxs,
                       const std::string& partition_spec,
                       std::map<std::string, std::string> writer_params);

    Status open();
    Status write(vectorized::Block& block);
    Status close(const Status& status);

private:
    TMCCommitData _build_mc_commit_data();

    RuntimeState* _state;
    const VExprContextSPtrs& _output_vexpr_ctxs;
    std::string _partition_spec;
    std::map<std::string, std::string> _writer_params;

    std::unique_ptr<VJniFormatTransformer> _jni_format_transformer;

    size_t _row_count = 0;
};

} // namespace vectorized
} // namespace doris
