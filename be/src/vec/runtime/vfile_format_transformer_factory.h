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

#include <memory>

#include "common/status.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {
class RuntimeState;
namespace io {
class FileWriter;
} // namespace io
} // namespace doris

namespace doris::vectorized {

class VFileFormatTransformer;

Status create_tvf_format_transformer(const TTVFTableSink& tvf_sink, RuntimeState* state,
                                     io::FileWriter* file_writer,
                                     const VExprContextSPtrs& output_vexpr_ctxs,
                                     std::unique_ptr<VFileFormatTransformer>* result);

} // namespace doris::vectorized
